from __future__ import absolute_import

import collections
import copy
import logging

import six

import kafka.common as Errors
from kafka.common import TopicPartition
from kafka.future import Future
from kafka.protocol.fetch import FetchRequest
from kafka.protocol.message import PartialMessage
from kafka.protocol.offset import OffsetRequest, OffsetResetStrategy

log = logging.getLogger(__name__)


ConsumerRecord = collections.namedtuple("ConsumerRecord",
    ["topic", "partition", "offset", "key", "value"])


class NoOffsetForPartitionError(Errors.KafkaError):
    pass


class RecordTooLargeError(Errors.KafkaError):
    pass


class Fetcher(object):
    DEFAULT_CONFIG = {
        'key_deserializer': None,
        'value_deserializer': None,
        'fetch_min_bytes': 1024,
        'fetch_max_wait_ms': 500,
        'max_partition_fetch_bytes': 1048576,
        'check_crcs': True,
    }

    def __init__(self, client, subscriptions, **configs):
                 #metrics=None,
                 #metric_group_prefix='consumer',
        self.config = copy.copy(self.DEFAULT_CONFIG)
        for key in self.config:
            if key in configs:
                self.config[key] = configs[key]

        self._client = client
        self._subscriptions = subscriptions
        self._records = collections.deque() # (offset, topic_partition, messages)
        self._unauthorized_topics = set()
        self._offset_out_of_range_partitions = dict() # {topic_partition: offset}
        self._record_too_large_partitions = dict() # {topic_partition: offset}

        #self.sensors = FetchManagerMetrics(metrics, metric_group_prefix)

    def init_fetches(self):
        """Send FetchRequests asynchronously for all assigned partitions"""
        futures = []
        for node_id, request in six.iteritems(self._create_fetch_requests()):
            if self._client.ready(node_id):
                log.debug("Sending FetchRequest to node %s", node_id)
                future = self._client.send(node_id, request)
                future.add_callback(self._handle_fetch_response, request)
                future.add_errback(log.error, 'Fetch to node %s failed: %s', node_id)
                futures.append(future)
        return futures

    def update_fetch_positions(self, partitions):
        """Update the fetch positions for the provided partitions.

        @param partitions: iterable of TopicPartitions
        @raises NoOffsetForPartitionError If no offset is stored for a given
                partition and no reset policy is available
        """
        # reset the fetch position to the committed position
        for tp in partitions:
            if not self._subscriptions.is_assigned(tp):
                log.warning("partition %s is not assigned - skipping offset"
                            " update", tp)
                continue
            elif self._subscriptions.is_fetchable(tp):
                log.warning("partition %s is still fetchable -- skipping offset"
                            " update", tp)
                continue

            # TODO: If there are several offsets to reset,
            # we could submit offset requests in parallel
            # for now, each call to _reset_offset will block
            if self._subscriptions.is_offset_reset_needed(tp):
                self._reset_offset(tp)
            elif self._subscriptions.assignment[tp].committed is None:
                # there's no committed position, so we need to reset with the
                # default strategy
                self._subscriptions.need_offset_reset(tp)
                self._reset_offset(tp)
            else:
                committed = self._subscriptions.assignment[tp].committed
                log.debug("Resetting offset for partition %s to the committed"
                          " offset %s", tp, committed)
                self._subscriptions.seek(tp, committed)

    def _reset_offset(self, partition):
        """Reset offsets for the given partition using the offset reset strategy.

        @param partition The given partition that needs reset offset
        @raises NoOffsetForPartitionError If no offset reset strategy is defined
        """
        timestamp = self._subscriptions.assignment[partition].reset_strategy
        if timestamp is OffsetResetStrategy.EARLIEST:
            strategy = 'earliest'
        elif timestamp is OffsetResetStrategy.LATEST:
            strategy = 'latest'
        else:
            raise NoOffsetForPartitionError(partition)

        log.debug("Resetting offset for partition %s to %s offset.",
                  partition, strategy)
        offset = self._offset(partition, timestamp)

        # we might lose the assignment while fetching the offset,
        # so check it is still active
        if self._subscriptions.is_assigned(partition):
            self._subscriptions.seek(partition, offset)

    def _offset(self, partition, timestamp):
        """Fetch a single offset before the given timestamp for the partition.

        Blocks until offset is obtained, or a non-retriable exception is raised

        @param partition The partition that needs fetching offset.
        @param timestamp The timestamp for fetching offset.
        @raises exceptions
        @return The offset of the message that is published before the given
                timestamp
        """
        while True:
            future = self._send_offset_request(partition, timestamp)
            self._client.poll(future=future)

            if future.succeeded():
                return future.value

            if not future.retriable():
                raise future.exception # pylint: disable-msg=raising-bad-type

            if future.exception.invalid_metadata:
                refresh_future = self._client.cluster.request_update()
                self._client.poll(future=refresh_future)

    def _raise_if_offset_out_of_range(self):
        """
        If any partition from previous FetchResponse contains
        OffsetOutOfRangeError and the default_reset_policy is None,
        raise OffsetOutOfRangeError
        """
        current_out_of_range_partitions = {}

        # filter only the fetchable partitions
        for partition, offset in self._offset_out_of_range_partitions:
            if not self._subscriptions.is_fetchable(partition):
                log.debug("Ignoring fetched records for %s since it is no"
                          " longer fetchable", partition)
                continue
            consumed = self._subscriptions.assignment[partition].consumed
            # ignore partition if its consumed offset != offset in FetchResponse
            # e.g. after seek()
            if consumed is not None and offset == consumed:
                current_out_of_range_partitions[partition] = offset

        self._offset_out_of_range_partitions.clear()
        if current_out_of_range_partitions:
            raise Errors.OffsetOutOfRangeError(current_out_of_range_partitions)

    def _raise_if_unauthorized_topics(self):
        """
        If any topic from previous FetchResponse contains an Authorization
        error, raise an exception

        @raise TopicAuthorizationFailedError
        """
        if self._unauthorized_topics:
            topics = set(self._unauthorized_topics)
            self._unauthorized_topics.clear()
            raise Errors.TopicAuthorizationFailedError(topics)

    def _raise_if_record_too_large(self):
        """
        If any partition from previous FetchResponse gets a RecordTooLarge
        error, raise RecordTooLargeError

        @raise RecordTooLargeError If there is a message larger than fetch size
                                   and hence cannot be ever returned
        """
        copied_record_too_large_partitions = dict(self._record_too_large_partitions)
        self._record_too_large_partitions.clear()

        if copied_record_too_large_partitions:
            raise RecordTooLargeError(
                "There are some messages at [Partition=Offset]: %s "
                " whose size is larger than the fetch size %s"
                " and hence cannot be ever returned."
                " Increase the fetch size, or decrease the maximum message"
                " size the broker will allow.",
                copied_record_too_large_partitions,
                self.config['max_partition_fetch_bytes'])

    def fetched_records(self):
        """Returns previously fetched records and updates consumed offsets

        NOTE: returning empty records guarantees the consumed position are NOT updated.

        @return {TopicPartition: deque([messages])}
        @raises OffsetOutOfRangeError if no subscription offset_reset_strategy
        """
        if self._subscriptions.needs_partition_assignment:
            return {}

        drained = collections.defaultdict(collections.deque)
        self._raise_if_offset_out_of_range()
        self._raise_if_unauthorized_topics()
        self._raise_if_record_too_large()

        # Loop over the records deque
        while self._records:
            (fetch_offset, tp, messages) = self._records.popleft()

            if not self._subscriptions.is_assigned(tp):
                # this can happen when a rebalance happened before
                # fetched records are returned to the consumer's poll call
                log.debug("Not returning fetched records for partition %s"
                          " since it is no longer assigned", tp)
                continue

            # note that the consumed position should always be available
            # as long as the partition is still assigned
            consumed = self._subscriptions.assignment[tp].consumed
            if not self._subscriptions.is_fetchable(tp):
                # this can happen when a partition consumption paused before
                # fetched records are returned to the consumer's poll call
                log.debug("Not returning fetched records for assigned partition"
                          " %s since it is no longer fetchable", tp)

                # we also need to reset the fetch positions to pretend we did
                # not fetch this partition in the previous request at all
                self._subscriptions.assignment[tp].fetched = consumed
            elif fetch_offset == consumed:
                next_offset = messages[-1][0] + 1
                log.debug("Returning fetched records for assigned partition %s"
                          " and update consumed position to %s", tp, next_offset)
                self._subscriptions.assignment[tp].consumed = next_offset

                # TODO: handle compressed messages
                for offset, size, msg in messages:
                    if msg.attributes:
                        raise Errors.KafkaError('Compressed messages not supported yet')
                    elif self.config['check_crcs'] and not msg.validate_crc():
                        raise Errors.InvalidMessageError(msg)

                    key, value = self._deserialize(msg)
                    record = ConsumerRecord(tp.topic, tp.partition, offset, key, value)
                    drained[tp].append(record)
            else:
                # these records aren't next in line based on the last consumed
                # position, ignore them they must be from an obsolete request
                log.debug("Ignoring fetched records for %s at offset %s",
                          tp, fetch_offset)
        return dict(drained)

    def _deserialize(self, msg):
        if self.config['key_deserializer']:
            key = self.config['key_deserializer'](msg.key) # pylint: disable-msg=not-callable
        else:
            key = msg.key
        if self.config['value_deserializer']:
            value = self.config['value_deserializer'](msg.value) # pylint: disable-msg=not-callable
        else:
            value = msg.value
        return key, value

    def _send_offset_request(self, partition, timestamp):
        """
        Fetch a single offset before the given timestamp for the partition.

        @param partition The TopicPartition that needs fetching offset.
        @param timestamp The timestamp for fetching offset.
        @return A future which can be polled to obtain the corresponding offset.
        """
        node_id = self._client.cluster.leader_for_partition(partition)
        if node_id is None:
            log.debug("Partition %s is unknown for fetching offset,"
                      " wait for metadata refresh", partition)
            return Future().failure(Errors.StaleMetadata(partition))
        elif node_id == -1:
            log.debug("Leader for partition %s unavailable for fetching offset,"
                      " wait for metadata refresh", partition)
            return Future().failure(Errors.LeaderNotAvailableError(partition))

        request = OffsetRequest(
            -1, [(partition.topic, [(partition.partition, timestamp, 1)])]
        )
        # Client returns a future that only fails on network issues
        # so create a separate future and attach a callback to update it
        # based on response error codes
        future = Future()
        if not self._client.ready(node_id):
            return future.failure(Errors.NodeNotReadyError(node_id))

        _f = self._client.send(node_id, request)
        _f.add_callback(self._handle_offset_response, partition, future)
        _f.add_errback(lambda e: future.failure(e))
        return future

    def _handle_offset_response(self, partition, future, response):
        """Callback for the response of the list offset call above.

        @param partition The partition that was fetched
        @param future the future to update based on response
        @param response The OffsetResponse from the server

        @raises IllegalStateError if response does not match partition
        """
        topic, partition_info = response.topics[0]
        if len(response.topics) != 1 or len(partition_info) != 1:
            raise Errors.IllegalStateError("OffsetResponse should only be for"
                                           " a single topic-partition")

        part, error_code, offsets = partition_info[0]
        if topic != partition.topic or part != partition.partition:
            raise Errors.IllegalStateError("OffsetResponse partition does not"
                                           " match OffsetRequest partition")

        error_type = Errors.for_code(error_code)
        if error_type is Errors.NoError:
            if len(offsets) != 1:
                raise Errors.IllegalStateError("OffsetResponse should only"
                                               " return a single offset")
            offset = offsets[0]
            log.debug("Fetched offset %d for partition %s", offset, partition)
            future.success(offset)
        elif error_type in (Errors.NotLeaderForPartitionError,
                       Errors.UnknownTopicOrPartitionError):
            log.warning("Attempt to fetch offsets for partition %s failed due"
                        " to obsolete leadership information, retrying.",
                        partition)
            future.failure(error_type(partition))
        else:
            log.error("Attempt to fetch offsets for partition %s failed due to:"
                      " %s", partition, error_type)
            future.failure(error_type(partition))

    def _create_fetch_requests(self):
        """
        Create fetch requests for all assigned partitions, grouped by node
        Except where no leader, node has requests in flight, or we have
        not returned all previously fetched records to consumer
        """
        # create the fetch info as a dict of lists of partition info tuples
        # which can be passed to FetchRequest() via .items()
        fetchable = collections.defaultdict(lambda: collections.defaultdict(list))

        for partition in self._subscriptions.fetchable_partitions():
            node_id = self._client.cluster.leader_for_partition(partition)
            if node_id is None or node_id == -1:
                log.debug("No leader found for partition %s."
                          " Requesting metadata update", partition)
                self._client.cluster.request_update()
            elif self._client.in_flight_request_count(node_id) == 0:
                # if there is a leader and no in-flight requests,
                # issue a new fetch but only fetch data for partitions whose
                # previously fetched data has been consumed
                fetched = self._subscriptions.assignment[partition].fetched
                consumed = self._subscriptions.assignment[partition].consumed
                if consumed == fetched:
                    partition_info = (
                        partition.partition,
                        fetched,
                        self.config['max_partition_fetch_bytes']
                    )
                    fetchable[node_id][partition.topic].append(partition_info)
                else:
                    log.debug("Skipping FetchRequest to %s because previously"
                              " fetched offsets (%s) have not been fully"
                              " consumed yet (%s)", node_id, fetched, consumed)

        requests = {}
        for node_id, partition_data in six.iteritems(fetchable):
            requests[node_id] = FetchRequest(
                -1, # replica_id
                self.config['fetch_max_wait_ms'],
                self.config['fetch_min_bytes'],
                partition_data.items())
        return requests

    def _handle_fetch_response(self, request, response):
        """The callback for fetch completion"""
        #total_bytes = 0
        #total_count = 0

        fetch_offsets = {}
        for topic, partitions in request.topics:
            for partition, offset, _ in partitions:
                fetch_offsets[TopicPartition(topic, partition)] = offset

        for topic, partitions in response.topics:
            for partition, error_code, highwater, messages in partitions:
                tp = TopicPartition(topic, partition)
                error_type = Errors.for_code(error_code)
                if not self._subscriptions.is_fetchable(tp):
                    # this can happen when a rebalance happened or a partition
                    # consumption paused while fetch is still in-flight
                    log.debug("Ignoring fetched records for partition %s"
                              " since it is no longer fetchable", tp)
                elif error_type is Errors.NoError:
                    fetch_offset = fetch_offsets[tp]

                    # we are interested in this fetch only if the beginning
                    # offset matches the current consumed position
                    consumed = self._subscriptions.assignment[tp].consumed
                    if consumed is None:
                        continue
                    elif consumed != fetch_offset:
                        # the fetched position has gotten out of sync with the
                        # consumed position (which might happen when a
                        # rebalance occurs with a fetch in-flight), so we need
                        # to reset the fetch position so the next fetch is right
                        self._subscriptions.assignment[tp].fetched = consumed
                        continue

                    partial = None
                    if messages and isinstance(messages[-1][-1], PartialMessage):
                        partial = messages.pop()

                    if messages:
                        last_offset, _, _ = messages[-1]
                        self._subscriptions.assignment[tp].fetched = last_offset + 1
                        self._records.append((fetch_offset, tp, messages))
                        #self.sensors.records_fetch_lag.record(highwater - last_offset)
                    elif partial:
                        # we did not read a single message from a non-empty
                        # buffer because that message's size is larger than
                        # fetch size, in this case record this exception
                        self._record_too_large_partitions[tp] = fetch_offset

                    # TODO: bytes metrics
                    #self.sensors.record_topic_fetch_metrics(tp.topic, num_bytes, parsed.size());
                    #totalBytes += num_bytes;
                    #totalCount += parsed.size();
                elif error_type in (Errors.NotLeaderForPartitionError,
                                    Errors.UnknownTopicOrPartitionError):
                    self._client.cluster.request_update()
                elif error_type is Errors.OffsetOutOfRangeError:
                    fetch_offset = fetch_offsets[tp]
                    if self._subscriptions.has_default_offset_reset_policy():
                        self._subscriptions.need_offset_reset(tp)
                    else:
                        self._offset_out_of_range_partitions[tp] = fetch_offset
                    log.info("Fetch offset %s is out of range, resetting offset",
                             self._subscriptions.assignment[tp].fetched)
                elif error_type is Errors.TopicAuthorizationFailedError:
                    log.warn("Not authorized to read from topic %s.", tp.topic)
                    self._unauthorized_topics.add(tp.topic)
                elif error_type is Errors.UnknownError:
                    log.warn("Unknown error fetching data for topic-partition %s", tp)
                else:
                    raise Errors.IllegalStateError("Unexpected error code %s"
                                                   " while fetching data"
                                                   % error_code)

        """TOOD - metrics
        self.sensors.bytesFetched.record(totalBytes)
        self.sensors.recordsFetched.record(totalCount)
        self.sensors.fetchThrottleTimeSensor.record(response.getThrottleTime())
        self.sensors.fetchLatency.record(resp.requestLatencyMs())


class FetchManagerMetrics(object):
    def __init__(self, metrics, prefix):
        self.metrics = metrics
        self.group_name = prefix + "-fetch-manager-metrics"

        self.bytes_fetched = metrics.sensor("bytes-fetched")
        self.bytes_fetched.add(metrics.metricName("fetch-size-avg", self.group_name,
            "The average number of bytes fetched per request"), metrics.Avg())
        self.bytes_fetched.add(metrics.metricName("fetch-size-max", self.group_name,
            "The maximum number of bytes fetched per request"), metrics.Max())
        self.bytes_fetched.add(metrics.metricName("bytes-consumed-rate", self.group_name,
            "The average number of bytes consumed per second"), metrics.Rate())

        self.records_fetched = self.metrics.sensor("records-fetched")
        self.records_fetched.add(metrics.metricName("records-per-request-avg", self.group_name,
            "The average number of records in each request"), metrics.Avg())
        self.records_fetched.add(metrics.metricName("records-consumed-rate", self.group_name,
            "The average number of records consumed per second"), metrics.Rate())

        self.fetch_latency = metrics.sensor("fetch-latency")
        self.fetch_latency.add(metrics.metricName("fetch-latency-avg", self.group_name,
            "The average time taken for a fetch request."), metrics.Avg())
        self.fetch_latency.add(metrics.metricName("fetch-latency-max", self.group_name,
            "The max time taken for any fetch request."), metrics.Max())
        self.fetch_latency.add(metrics.metricName("fetch-rate", self.group_name,
            "The number of fetch requests per second."), metrics.Rate(metrics.Count()))

        self.records_fetch_lag = metrics.sensor("records-lag")
        self.records_fetch_lag.add(metrics.metricName("records-lag-max", self.group_name,
            "The maximum lag in terms of number of records for any partition in self window"), metrics.Max())

        self.fetch_throttle_time_sensor = metrics.sensor("fetch-throttle-time")
        self.fetch_throttle_time_sensor.add(metrics.metricName("fetch-throttle-time-avg", self.group_name,
            "The average throttle time in ms"), metrics.Avg())
        self.fetch_throttle_time_sensor.add(metrics.metricName("fetch-throttle-time-max", self.group_name,
            "The maximum throttle time in ms"), metrics.Max())

        def record_topic_fetch_metrics(topic, num_bytes, num_records):
            # record bytes fetched
            name = '.'.join(["topic", topic, "bytes-fetched"])
            self.metrics[name].record(num_bytes);

            # record records fetched
            name = '.'.join(["topic", topic, "records-fetched"])
            self.metrics[name].record(num_records)
        """
