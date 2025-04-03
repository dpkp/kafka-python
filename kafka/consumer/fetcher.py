from __future__ import absolute_import, division

import collections
import copy
import itertools
import logging
import sys
import time

from kafka.vendor import six

import kafka.errors as Errors
from kafka.future import Future
from kafka.metrics.stats import Avg, Count, Max, Rate
from kafka.protocol.fetch import FetchRequest
from kafka.protocol.list_offsets import (
    ListOffsetsRequest, OffsetResetStrategy, UNKNOWN_OFFSET
)
from kafka.record import MemoryRecords
from kafka.serializer import Deserializer
from kafka.structs import TopicPartition, OffsetAndMetadata, OffsetAndTimestamp
from kafka.util import timeout_ms_fn

log = logging.getLogger(__name__)


# Isolation levels
READ_UNCOMMITTED = 0
READ_COMMITTED = 1

ConsumerRecord = collections.namedtuple("ConsumerRecord",
    ["topic", "partition", "leader_epoch", "offset", "timestamp", "timestamp_type",
     "key", "value", "headers", "checksum", "serialized_key_size", "serialized_value_size", "serialized_header_size"])


CompletedFetch = collections.namedtuple("CompletedFetch",
    ["topic_partition", "fetched_offset", "response_version",
     "partition_data", "metric_aggregator"])


class NoOffsetForPartitionError(Errors.KafkaError):
    pass


class RecordTooLargeError(Errors.KafkaError):
    pass


class Fetcher(six.Iterator):
    DEFAULT_CONFIG = {
        'key_deserializer': None,
        'value_deserializer': None,
        'fetch_min_bytes': 1,
        'fetch_max_wait_ms': 500,
        'fetch_max_bytes': 52428800,
        'max_partition_fetch_bytes': 1048576,
        'max_poll_records': sys.maxsize,
        'check_crcs': True,
        'metrics': None,
        'metric_group_prefix': 'consumer',
        'retry_backoff_ms': 100,
        'enable_incremental_fetch_sessions': True,
    }

    def __init__(self, client, subscriptions, **configs):
        """Initialize a Kafka Message Fetcher.

        Keyword Arguments:
            key_deserializer (callable): Any callable that takes a
                raw message key and returns a deserialized key.
            value_deserializer (callable, optional): Any callable that takes a
                raw message value and returns a deserialized value.
            enable_incremental_fetch_sessions: (bool): Use incremental fetch sessions
                when available / supported by kafka broker. See KIP-227. Default: True.
            fetch_min_bytes (int): Minimum amount of data the server should
                return for a fetch request, otherwise wait up to
                fetch_max_wait_ms for more data to accumulate. Default: 1.
            fetch_max_wait_ms (int): The maximum amount of time in milliseconds
                the server will block before answering the fetch request if
                there isn't sufficient data to immediately satisfy the
                requirement given by fetch_min_bytes. Default: 500.
            fetch_max_bytes (int): The maximum amount of data the server should
                return for a fetch request. This is not an absolute maximum, if
                the first message in the first non-empty partition of the fetch
                is larger than this value, the message will still be returned
                to ensure that the consumer can make progress. NOTE: consumer
                performs fetches to multiple brokers in parallel so memory
                usage will depend on the number of brokers containing
                partitions for the topic.
                Supported Kafka version >= 0.10.1.0. Default: 52428800 (50 MB).
            max_partition_fetch_bytes (int): The maximum amount of data
                per-partition the server will return. The maximum total memory
                used for a request = #partitions * max_partition_fetch_bytes.
                This size must be at least as large as the maximum message size
                the server allows or else it is possible for the producer to
                send messages larger than the consumer can fetch. If that
                happens, the consumer can get stuck trying to fetch a large
                message on a certain partition. Default: 1048576.
            check_crcs (bool): Automatically check the CRC32 of the records
                consumed. This ensures no on-the-wire or on-disk corruption to
                the messages occurred. This check adds some overhead, so it may
                be disabled in cases seeking extreme performance. Default: True
        """
        self.config = copy.copy(self.DEFAULT_CONFIG)
        for key in self.config:
            if key in configs:
                self.config[key] = configs[key]

        self._client = client
        self._subscriptions = subscriptions
        self._completed_fetches = collections.deque()  # Unparsed responses
        self._next_partition_records = None  # Holds a single PartitionRecords until fully consumed
        self._iterator = None
        self._fetch_futures = collections.deque()
        if self.config['metrics']:
            self._sensors = FetchManagerMetrics(self.config['metrics'], self.config['metric_group_prefix'])
        else:
            self._sensors = None
        self._isolation_level = READ_UNCOMMITTED
        self._session_handlers = {}
        self._nodes_with_pending_fetch_requests = set()

    def send_fetches(self):
        """Send FetchRequests for all assigned partitions that do not already have
        an in-flight fetch or pending fetch data.

        Returns:
            List of Futures: each future resolves to a FetchResponse
        """
        futures = []
        for node_id, (request, fetch_offsets) in six.iteritems(self._create_fetch_requests()):
            log.debug("Sending FetchRequest to node %s", node_id)
            self._nodes_with_pending_fetch_requests.add(node_id)
            future = self._client.send(node_id, request, wakeup=False)
            future.add_callback(self._handle_fetch_response, node_id, fetch_offsets, time.time())
            future.add_errback(self._handle_fetch_error, node_id)
            futures.append(future)
        self._fetch_futures.extend(futures)
        self._clean_done_fetch_futures()
        return futures

    def reset_offsets_if_needed(self, partitions, timeout_ms=None):
        """Lookup and set offsets for any partitions which are awaiting an
        explicit reset.

        Arguments:
            partitions (set of TopicPartitions): the partitions to reset

        Raises:
            KafkaTimeoutError if timeout_ms provided
        """
        inner_timeout_ms = timeout_ms_fn(timeout_ms, 'Timeout resetting offsets')
        for tp in partitions:
            # TODO: If there are several offsets to reset, we could submit offset requests in parallel
            if self._subscriptions.is_assigned(tp) and self._subscriptions.is_offset_reset_needed(tp):
                self._reset_offset(tp, timeout_ms=inner_timeout_ms())

    def _clean_done_fetch_futures(self):
        while True:
            if not self._fetch_futures:
                break
            if not self._fetch_futures[0].is_done:
                break
            self._fetch_futures.popleft()

    def in_flight_fetches(self):
        """Return True if there are any unprocessed FetchRequests in flight."""
        self._clean_done_fetch_futures()
        return bool(self._fetch_futures)

    def update_fetch_positions(self, partitions, timeout_ms=None):
        """Update the fetch positions for the provided partitions.

        Arguments:
            partitions (list of TopicPartitions): partitions to update

        Raises:
            NoOffsetForPartitionError: if no offset is stored for a given
                partition and no reset policy is available
            KafkaTimeoutError if timeout_ms provided.
        """
        inner_timeout_ms = timeout_ms_fn(timeout_ms, 'Timeout updating fetch positions')
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

            if self._subscriptions.is_offset_reset_needed(tp):
                self._reset_offset(tp, timeout_ms=inner_timeout_ms())
            elif self._subscriptions.assignment[tp].committed is None:
                # there's no committed position, so we need to reset with the
                # default strategy
                self._subscriptions.need_offset_reset(tp)
                self._reset_offset(tp, timeout_ms=inner_timeout_ms())
            else:
                committed = self._subscriptions.assignment[tp].committed.offset
                log.debug("Resetting offset for partition %s to the committed"
                          " offset %s", tp, committed)
                self._subscriptions.seek(tp, committed)

    def get_offsets_by_times(self, timestamps, timeout_ms):
        offsets = self._retrieve_offsets(timestamps, timeout_ms)
        for tp in timestamps:
            if tp not in offsets:
                offsets[tp] = None
        return offsets

    def beginning_offsets(self, partitions, timeout_ms):
        return self.beginning_or_end_offset(
            partitions, OffsetResetStrategy.EARLIEST, timeout_ms)

    def end_offsets(self, partitions, timeout_ms):
        return self.beginning_or_end_offset(
            partitions, OffsetResetStrategy.LATEST, timeout_ms)

    def beginning_or_end_offset(self, partitions, timestamp, timeout_ms):
        timestamps = dict([(tp, timestamp) for tp in partitions])
        offsets = self._retrieve_offsets(timestamps, timeout_ms)
        for tp in timestamps:
            offsets[tp] = offsets[tp].offset
        return offsets

    def _reset_offset(self, partition, timeout_ms=None):
        """Reset offsets for the given partition using the offset reset strategy.

        Arguments:
            partition (TopicPartition): the partition that needs reset offset

        Raises:
            NoOffsetForPartitionError: if no offset reset strategy is defined
            KafkaTimeoutError if timeout_ms provided
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
        offsets = self._retrieve_offsets({partition: timestamp}, timeout_ms=timeout_ms)

        if partition in offsets:
            offset = offsets[partition].offset

            # we might lose the assignment while fetching the offset,
            # so check it is still active
            if self._subscriptions.is_assigned(partition):
                self._subscriptions.seek(partition, offset)
        else:
            log.debug("Could not find offset for partition %s since it is probably deleted" % (partition,))

    def _retrieve_offsets(self, timestamps, timeout_ms=None):
        """Fetch offset for each partition passed in ``timestamps`` map.

        Blocks until offsets are obtained, a non-retriable exception is raised
        or ``timeout_ms`` passed.

        Arguments:
            timestamps: {TopicPartition: int} dict with timestamps to fetch
                offsets by. -1 for the latest available, -2 for the earliest
                available. Otherwise timestamp is treated as epoch milliseconds.

        Returns:
            {TopicPartition: OffsetAndTimestamp}: Mapping of partition to
                retrieved offset, timestamp, and leader_epoch. If offset does not exist for
                the provided timestamp, that partition will be missing from
                this mapping.

        Raises:
            KafkaTimeoutError if timeout_ms provided
        """
        if not timestamps:
            return {}

        inner_timeout_ms = timeout_ms_fn(timeout_ms, 'Timeout fetching offsets')
        timestamps = copy.copy(timestamps)
        while True:
            if not timestamps:
                return {}

            future = self._send_list_offsets_requests(timestamps)
            self._client.poll(future=future, timeout_ms=inner_timeout_ms())

            # Timeout w/o future completion
            if not future.is_done:
                break

            if future.succeeded():
                return future.value
            if not future.retriable():
                raise future.exception  # pylint: disable-msg=raising-bad-type

            if future.exception.invalid_metadata:
                refresh_future = self._client.cluster.request_update()
                self._client.poll(future=refresh_future, timeout_ms=inner_timeout_ms())

                if not future.is_done:
                    break

                # Issue #1780
                # Recheck partition existence after after a successful metadata refresh
                if refresh_future.succeeded() and isinstance(future.exception, Errors.StaleMetadata):
                    log.debug("Stale metadata was raised, and we now have an updated metadata. Rechecking partition existence")
                    unknown_partition = future.exception.args[0]  # TopicPartition from StaleMetadata
                    if self._client.cluster.leader_for_partition(unknown_partition) is None:
                        log.debug("Removed partition %s from offsets retrieval" % (unknown_partition, ))
                        timestamps.pop(unknown_partition)
            else:
                time.sleep(inner_timeout_ms(self.config['retry_backoff_ms']) / 1000)

        raise Errors.KafkaTimeoutError(
            "Failed to get offsets by timestamps in %s ms" % (timeout_ms,))

    def fetched_records(self, max_records=None, update_offsets=True):
        """Returns previously fetched records and updates consumed offsets.

        Arguments:
            max_records (int): Maximum number of records returned. Defaults
                to max_poll_records configuration.

        Raises:
            OffsetOutOfRangeError: if no subscription offset_reset_strategy
            CorruptRecordException: if message crc validation fails (check_crcs
                must be set to True)
            RecordTooLargeError: if a message is larger than the currently
                configured max_partition_fetch_bytes
            TopicAuthorizationError: if consumer is not authorized to fetch
                messages from the topic

        Returns: (records (dict), partial (bool))
            records: {TopicPartition: [messages]}
            partial: True if records returned did not fully drain any pending
                partition requests. This may be useful for choosing when to
                pipeline additional fetch requests.
        """
        if max_records is None:
            max_records = self.config['max_poll_records']
        assert max_records > 0

        drained = collections.defaultdict(list)
        records_remaining = max_records

        while records_remaining > 0:
            if not self._next_partition_records:
                if not self._completed_fetches:
                    break
                completion = self._completed_fetches.popleft()
                self._next_partition_records = self._parse_fetched_data(completion)
            else:
                records_remaining -= self._append(drained,
                                                  self._next_partition_records,
                                                  records_remaining,
                                                  update_offsets)
        return dict(drained), bool(self._completed_fetches)

    def _append(self, drained, part, max_records, update_offsets):
        if not part:
            return 0

        tp = part.topic_partition
        if not self._subscriptions.is_assigned(tp):
            # this can happen when a rebalance happened before
            # fetched records are returned to the consumer's poll call
            log.debug("Not returning fetched records for partition %s"
                      " since it is no longer assigned", tp)
        elif not self._subscriptions.is_fetchable(tp):
            # this can happen when a partition is paused before
            # fetched records are returned to the consumer's poll call
            log.debug("Not returning fetched records for assigned partition"
                      " %s since it is no longer fetchable", tp)

        else:
            # note that the position should always be available
            # as long as the partition is still assigned
            position = self._subscriptions.assignment[tp].position
            if part.next_fetch_offset == position.offset:
                log.debug("Returning fetched records at offset %d for assigned"
                          " partition %s", position.offset, tp)
                part_records = part.take(max_records)
                # list.extend([]) is a noop, but because drained is a defaultdict
                # we should avoid initializing the default list unless there are records
                if part_records:
                    drained[tp].extend(part_records)
                # We want to increment subscription position if (1) we're using consumer.poll(),
                # or (2) we didn't return any records (consumer iterator will update position
                # when each message is yielded). There may be edge cases where we re-fetch records
                # that we'll end up skipping, but for now we'll live with that.
                highwater = self._subscriptions.assignment[tp].highwater
                if highwater is not None and self._sensors:
                    self._sensors.records_fetch_lag.record(highwater - part.next_fetch_offset)
                if update_offsets or not part_records:
                    # TODO: save leader_epoch
                    log.debug("Updating fetch position for assigned partition %s to %s (leader epoch %s)",
                              tp, part.next_fetch_offset, part.leader_epoch)
                    self._subscriptions.assignment[tp].position = OffsetAndMetadata(part.next_fetch_offset, '', -1)
                return len(part_records)

            else:
                # these records aren't next in line based on the last consumed
                # position, ignore them they must be from an obsolete request
                log.debug("Ignoring fetched records for %s at offset %s since"
                          " the current position is %d", tp, part.next_fetch_offset,
                          position.offset)

        part.drain()
        return 0

    def _send_list_offsets_requests(self, timestamps):
        """Fetch offsets for each partition in timestamps dict. This may send
        request to multiple nodes, based on who is Leader for partition.

        Arguments:
            timestamps (dict): {TopicPartition: int} mapping of fetching
                timestamps.

        Returns:
            Future: resolves to a mapping of retrieved offsets
        """
        timestamps_by_node = collections.defaultdict(dict)
        for partition, timestamp in six.iteritems(timestamps):
            node_id = self._client.cluster.leader_for_partition(partition)
            if node_id is None:
                if partition.topic not in self._client.cluster.topics():
                    log.warning("Could not lookup offsets for partition %s since no metadata is available for topic. "
                                "Wait for metadata refresh and try again", partition)
                else:
                    log.warning("Could not lookup offsets for partition %s since no metadata is available for it. "
                                "Wait for metadata refresh and try again", partition)
                self._client.add_topic(partition.topic)
                return Future().failure(Errors.StaleMetadata(partition))
            elif node_id == -1:
                log.debug("Leader for partition %s unavailable for fetching "
                          "offset, wait for metadata refresh", partition)
                return Future().failure(
                    Errors.LeaderNotAvailableError(partition))
            else:
                leader_epoch = -1
                timestamps_by_node[node_id][partition] = (timestamp, leader_epoch)

        # Aggregate results until we have all
        list_offsets_future = Future()
        responses = []
        node_count = len(timestamps_by_node)

        def on_success(value):
            responses.append(value)
            if len(responses) == node_count:
                offsets = {}
                for r in responses:
                    offsets.update(r)
                list_offsets_future.success(offsets)

        def on_fail(err):
            if not list_offsets_future.is_done:
                list_offsets_future.failure(err)

        for node_id, timestamps in six.iteritems(timestamps_by_node):
            _f = self._send_list_offsets_request(node_id, timestamps)
            _f.add_callback(on_success)
            _f.add_errback(on_fail)
        return list_offsets_future

    def _send_list_offsets_request(self, node_id, timestamps_and_epochs):
        version = self._client.api_version(ListOffsetsRequest, max_version=4)
        by_topic = collections.defaultdict(list)
        for tp, (timestamp, leader_epoch) in six.iteritems(timestamps_and_epochs):
            if version >= 4:
                data = (tp.partition, leader_epoch, timestamp)
            elif version >= 1:
                data = (tp.partition, timestamp)
            else:
                data = (tp.partition, timestamp, 1)
            by_topic[tp.topic].append(data)

        if version <= 1:
            request = ListOffsetsRequest[version](
                    -1,
                    list(six.iteritems(by_topic)))
        else:
            request = ListOffsetsRequest[version](
                    -1,
                    self._isolation_level,
                    list(six.iteritems(by_topic)))


        # Client returns a future that only fails on network issues
        # so create a separate future and attach a callback to update it
        # based on response error codes
        future = Future()

        _f = self._client.send(node_id, request)
        _f.add_callback(self._handle_list_offsets_response, future)
        _f.add_errback(lambda e: future.failure(e))
        return future

    def _handle_list_offsets_response(self, future, response):
        """Callback for the response of the ListOffsets api call

        Arguments:
            future (Future): the future to update based on response
            response (ListOffsetsResponse): response from the server

        Raises:
            AssertionError: if response does not match partition
        """
        timestamp_offset_map = {}
        for topic, part_data in response.topics:
            for partition_info in part_data:
                partition, error_code = partition_info[:2]
                partition = TopicPartition(topic, partition)
                error_type = Errors.for_code(error_code)
                if error_type is Errors.NoError:
                    if response.API_VERSION == 0:
                        offsets = partition_info[2]
                        assert len(offsets) <= 1, 'Expected ListOffsetsResponse with one offset'
                        if not offsets:
                            offset = UNKNOWN_OFFSET
                        else:
                            offset = offsets[0]
                        timestamp = None
                        leader_epoch = -1
                    elif response.API_VERSION <= 3:
                        timestamp, offset = partition_info[2:]
                        leader_epoch = -1
                    else:
                        timestamp, offset, leader_epoch = partition_info[2:]
                    log.debug("Handling ListOffsetsResponse response for %s. "
                              "Fetched offset %s, timestamp %s, leader_epoch %s",
                              partition, offset, timestamp, leader_epoch)
                    if offset != UNKNOWN_OFFSET:
                        timestamp_offset_map[partition] = OffsetAndTimestamp(offset, timestamp, leader_epoch)
                elif error_type is Errors.UnsupportedForMessageFormatError:
                    # The message format on the broker side is before 0.10.0,
                    # we simply put None in the response.
                    log.debug("Cannot search by timestamp for partition %s because the"
                              " message format version is before 0.10.0", partition)
                elif error_type in (Errors.NotLeaderForPartitionError,
                                    Errors.ReplicaNotAvailableError,
                                    Errors.KafkaStorageError):
                    log.debug("Attempt to fetch offsets for partition %s failed due"
                              " to %s, retrying.", error_type.__name__, partition)
                    future.failure(error_type(partition))
                    return
                elif error_type is Errors.UnknownTopicOrPartitionError:
                    log.warning("Received unknown topic or partition error in ListOffsets "
                                "request for partition %s. The topic/partition " +
                                "may not exist or the user may not have Describe access "
                                "to it.", partition)
                    future.failure(error_type(partition))
                    return
                else:
                    log.warning("Attempt to fetch offsets for partition %s failed due to:"
                                " %s", partition, error_type.__name__)
                    future.failure(error_type(partition))
                    return
        if not future.is_done:
            future.success(timestamp_offset_map)

    def _fetchable_partitions(self):
        fetchable = self._subscriptions.fetchable_partitions()
        # do not fetch a partition if we have a pending fetch response to process
        discard = {fetch.topic_partition for fetch in self._completed_fetches}
        current = self._next_partition_records
        if current:
            discard.add(current.topic_partition)
        return [tp for tp in fetchable if tp not in discard]

    def _create_fetch_requests(self):
        """Create fetch requests for all assigned partitions, grouped by node.

        FetchRequests skipped if no leader, or node has requests in flight

        Returns:
            dict: {node_id: (FetchRequest, {TopicPartition: fetch_offset}), ...} (version depends on client api_versions)
        """
        # create the fetch info as a dict of lists of partition info tuples
        # which can be passed to FetchRequest() via .items()
        version = self._client.api_version(FetchRequest, max_version=10)
        fetchable = collections.defaultdict(collections.OrderedDict)

        for partition in self._fetchable_partitions():
            node_id = self._client.cluster.leader_for_partition(partition)

            position = self._subscriptions.assignment[partition].position

            # fetch if there is a leader and no in-flight requests
            if node_id is None or node_id == -1:
                log.debug("No leader found for partition %s."
                          " Requesting metadata update", partition)
                self._client.cluster.request_update()

            elif not self._client.connected(node_id) and self._client.connection_delay(node_id) > 0:
                # If we try to send during the reconnect backoff window, then the request is just
                # going to be failed anyway before being sent, so skip the send for now
                log.log(0, "Skipping fetch for partition %s because node %s is awaiting reconnect backoff",
                        partition, node_id)

            elif self._client.throttle_delay(node_id) > 0:
                # If we try to send while throttled, then the request is just
                # going to be failed anyway before being sent, so skip the send for now
                log.log(0, "Skipping fetch for partition %s because node %s is throttled",
                        partition, node_id)

            elif node_id in self._nodes_with_pending_fetch_requests:
                log.log(0, "Skipping fetch for partition %s because there is a pending fetch request to node %s",
                        partition, node_id)
                continue

            if version < 5:
                partition_info = (
                    partition.partition,
                    position.offset,
                    self.config['max_partition_fetch_bytes']
                )
            elif version <= 8:
                partition_info = (
                    partition.partition,
                    position.offset,
                    -1, # log_start_offset is used internally by brokers / replicas only
                    self.config['max_partition_fetch_bytes'],
                )
            else:
                partition_info = (
                    partition.partition,
                    position.leader_epoch,
                    position.offset,
                    -1, # log_start_offset is used internally by brokers / replicas only
                    self.config['max_partition_fetch_bytes'],
                )

            fetchable[node_id][partition] = partition_info
            log.debug("Adding fetch request for partition %s at offset %d",
                      partition, position.offset)

        requests = {}
        for node_id, next_partitions in six.iteritems(fetchable):
            if version >= 7 and self.config['enable_incremental_fetch_sessions']:
                if node_id not in self._session_handlers:
                    self._session_handlers[node_id] = FetchSessionHandler(node_id)
                session = self._session_handlers[node_id].build_next(next_partitions)
            else:
                # No incremental fetch support
                session = FetchRequestData(next_partitions, None, FetchMetadata.LEGACY)

            if version <= 2:
                request = FetchRequest[version](
                    -1,  # replica_id
                    self.config['fetch_max_wait_ms'],
                    self.config['fetch_min_bytes'],
                    session.to_send)
            elif version == 3:
                request = FetchRequest[version](
                    -1,  # replica_id
                    self.config['fetch_max_wait_ms'],
                    self.config['fetch_min_bytes'],
                    self.config['fetch_max_bytes'],
                    session.to_send)
            elif version <= 6:
                request = FetchRequest[version](
                    -1,  # replica_id
                    self.config['fetch_max_wait_ms'],
                    self.config['fetch_min_bytes'],
                    self.config['fetch_max_bytes'],
                    self._isolation_level,
                    session.to_send)
            else:
                # Through v8
                request = FetchRequest[version](
                    -1,  # replica_id
                    self.config['fetch_max_wait_ms'],
                    self.config['fetch_min_bytes'],
                    self.config['fetch_max_bytes'],
                    self._isolation_level,
                    session.id,
                    session.epoch,
                    session.to_send,
                    session.to_forget)

            fetch_offsets = {}
            for tp, partition_data in six.iteritems(next_partitions):
                if version <= 8:
                    offset = partition_data[1]
                else:
                    offset = partition_data[2]
                fetch_offsets[tp] = offset

            requests[node_id] = (request, fetch_offsets)

        return requests

    def _handle_fetch_response(self, node_id, fetch_offsets, send_time, response):
        """The callback for fetch completion"""
        if response.API_VERSION >= 7 and self.config['enable_incremental_fetch_sessions']:
            if node_id not in self._session_handlers:
                log.error("Unable to find fetch session handler for node %s. Ignoring fetch response", node_id)
                return
            if not self._session_handlers[node_id].handle_response(response):
                return

        partitions = set([TopicPartition(topic, partition_data[0])
                          for topic, partitions in response.topics
                          for partition_data in partitions])
        if self._sensors:
            metric_aggregator = FetchResponseMetricAggregator(self._sensors, partitions)
        else:
            metric_aggregator = None

        for topic, partitions in response.topics:
            for partition_data in partitions:
                tp = TopicPartition(topic, partition_data[0])
                fetch_offset = fetch_offsets[tp]
                completed_fetch = CompletedFetch(
                    tp, fetch_offset,
                    response.API_VERSION,
                    partition_data[1:],
                    metric_aggregator
                )
                self._completed_fetches.append(completed_fetch)

        if self._sensors:
            self._sensors.fetch_latency.record((time.time() - send_time) * 1000)
        self._nodes_with_pending_fetch_requests.remove(node_id)

    def _handle_fetch_error(self, node_id, exception):
        level = logging.INFO if isinstance(exception, Errors.Cancelled) else logging.ERROR
        log.log(level, 'Fetch to node %s failed: %s', node_id, exception)
        if node_id in self._session_handlers:
            self._session_handlers[node_id].handle_error(exception)
        self._nodes_with_pending_fetch_requests.remove(node_id)

    def _parse_fetched_data(self, completed_fetch):
        tp = completed_fetch.topic_partition
        fetch_offset = completed_fetch.fetched_offset
        error_code, highwater = completed_fetch.partition_data[:2]
        error_type = Errors.for_code(error_code)
        parsed_records = None

        try:
            if not self._subscriptions.is_fetchable(tp):
                # this can happen when a rebalance happened or a partition
                # consumption paused while fetch is still in-flight
                log.debug("Ignoring fetched records for partition %s"
                          " since it is no longer fetchable", tp)

            elif error_type is Errors.NoError:
                # we are interested in this fetch only if the beginning
                # offset (of the *request*) matches the current consumed position
                # Note that the *response* may return a messageset that starts
                # earlier (e.g., compressed messages) or later (e.g., compacted topic)
                position = self._subscriptions.assignment[tp].position
                if position is None or position.offset != fetch_offset:
                    log.debug("Discarding fetch response for partition %s"
                              " since its offset %d does not match the"
                              " expected offset %d", tp, fetch_offset,
                              position.offset)
                    return None

                records = MemoryRecords(completed_fetch.partition_data[-1])
                log.debug("Preparing to read %s bytes of data for partition %s with offset %d",
                          records.size_in_bytes(), tp, fetch_offset)
                parsed_records = self.PartitionRecords(fetch_offset, tp, records,
                                                       self.config['key_deserializer'],
                                                       self.config['value_deserializer'],
                                                       self.config['check_crcs'],
                                                       completed_fetch.metric_aggregator,
                                                       self._on_partition_records_drain)
                if not records.has_next() and records.size_in_bytes() > 0:
                    if completed_fetch.response_version < 3:
                        # Implement the pre KIP-74 behavior of throwing a RecordTooLargeException.
                        record_too_large_partitions = {tp: fetch_offset}
                        raise RecordTooLargeError(
                            "There are some messages at [Partition=Offset]: %s "
                            " whose size is larger than the fetch size %s"
                            " and hence cannot be ever returned. Please condier upgrading your broker to 0.10.1.0 or"
                            " newer to avoid this issue. Alternatively, increase the fetch size on the client (using"
                            " max_partition_fetch_bytes)" % (
                                record_too_large_partitions,
                                self.config['max_partition_fetch_bytes']),
                            record_too_large_partitions)
                    else:
                        # This should not happen with brokers that support FetchRequest/Response V3 or higher (i.e. KIP-74)
                        raise Errors.KafkaError("Failed to make progress reading messages at %s=%s."
                                                " Received a non-empty fetch response from the server, but no"
                                                " complete records were found." % (tp, fetch_offset))

                if highwater >= 0:
                    self._subscriptions.assignment[tp].highwater = highwater

            elif error_type in (Errors.NotLeaderForPartitionError,
                                Errors.ReplicaNotAvailableError,
                                Errors.UnknownTopicOrPartitionError,
                                Errors.KafkaStorageError):
                log.debug("Error fetching partition %s: %s", tp, error_type.__name__)
                self._client.cluster.request_update()
            elif error_type is Errors.OffsetOutOfRangeError:
                position = self._subscriptions.assignment[tp].position
                if position is None or position.offset != fetch_offset:
                    log.debug("Discarding stale fetch response for partition %s"
                              " since the fetched offset %d does not match the"
                              " current offset %d", tp, fetch_offset, position.offset)
                elif self._subscriptions.has_default_offset_reset_policy():
                    log.info("Fetch offset %s is out of range for topic-partition %s", fetch_offset, tp)
                    self._subscriptions.need_offset_reset(tp)
                else:
                    raise Errors.OffsetOutOfRangeError({tp: fetch_offset})

            elif error_type is Errors.TopicAuthorizationFailedError:
                log.warning("Not authorized to read from topic %s.", tp.topic)
                raise Errors.TopicAuthorizationFailedError(set([tp.topic]))
            elif getattr(error_type, 'retriable', False):
                log.debug("Retriable error fetching partition %s: %s", tp, error_type())
                if getattr(error_type, 'invalid_metadata', False):
                    self._client.cluster.request_update()
            else:
                raise error_type('Unexpected error while fetching data')

        finally:
            if parsed_records is None and completed_fetch.metric_aggregator:
                completed_fetch.metric_aggregator.record(tp, 0, 0)

            if error_type is not Errors.NoError:
                # we move the partition to the end if there was an error. This way, it's more likely that partitions for
                # the same topic can remain together (allowing for more efficient serialization).
                self._subscriptions.move_partition_to_end(tp)

        return parsed_records

    def _on_partition_records_drain(self, partition_records):
        # we move the partition to the end if we received some bytes. This way, it's more likely that partitions
        # for the same topic can remain together (allowing for more efficient serialization).
        if partition_records.bytes_read > 0:
            self._subscriptions.move_partition_to_end(partition_records.topic_partition)

    def close(self):
        if self._next_partition_records is not None:
            self._next_partition_records.drain()

    class PartitionRecords(object):
        def __init__(self, fetch_offset, tp, records, key_deserializer, value_deserializer, check_crcs, metric_aggregator, on_drain):
            self.fetch_offset = fetch_offset
            self.topic_partition = tp
            self.leader_epoch = -1
            self.next_fetch_offset = fetch_offset
            self.bytes_read = 0
            self.records_read = 0
            self.metric_aggregator = metric_aggregator
            self.check_crcs = check_crcs
            self.record_iterator = itertools.dropwhile(
                self._maybe_skip_record,
                self._unpack_records(tp, records, key_deserializer, value_deserializer))
            self.on_drain = on_drain

        def _maybe_skip_record(self, record):
            # When fetching an offset that is in the middle of a
            # compressed batch, we will get all messages in the batch.
            # But we want to start 'take' at the fetch_offset
            # (or the next highest offset in case the message was compacted)
            if record.offset < self.fetch_offset:
                log.debug("Skipping message offset: %s (expecting %s)",
                          record.offset, self.fetch_offset)
                return True
            else:
                return False

        # For truthiness evaluation
        def __bool__(self):
            return self.record_iterator is not None

        # py2
        __nonzero__ = __bool__

        def drain(self):
            if self.record_iterator is not None:
                self.record_iterator = None
                if self.metric_aggregator:
                    self.metric_aggregator.record(self.topic_partition, self.bytes_read, self.records_read)
                self.on_drain(self)

        def take(self, n=None):
            return list(itertools.islice(self.record_iterator, 0, n))

        def _unpack_records(self, tp, records, key_deserializer, value_deserializer):
            try:
                batch = records.next_batch()
                last_batch = None
                while batch is not None:
                    last_batch = batch

                    if self.check_crcs and not batch.validate_crc():
                        raise Errors.CorruptRecordException(
                                "Record batch for partition %s at offset %s failed crc check" % (
                                    self.topic_partition, batch.base_offset))

                    # Try DefaultsRecordBatch / message log format v2
                    # base_offset, last_offset_delta, and control batches
                    if batch.magic == 2:
                        self.leader_epoch = batch.leader_epoch
                        # Control batches have a single record indicating whether a transaction
                        # was aborted or committed.
                        # When isolation_level is READ_COMMITTED (currently unsupported)
                        # we should also skip all messages from aborted transactions
                        # For now we only support READ_UNCOMMITTED and so we ignore the
                        # abort/commit signal.
                        if batch.is_control_batch:
                            self.next_fetch_offset = next(batch).offset + 1
                            batch = records.next_batch()
                            continue

                    for record in batch:
                        if self.check_crcs and not record.validate_crc():
                            raise Errors.CorruptRecordException(
                                    "Record for partition %s at offset %s failed crc check" % (
                                        self.topic_partition, record.offset))
                        key_size = len(record.key) if record.key is not None else -1
                        value_size = len(record.value) if record.value is not None else -1
                        key = self._deserialize(key_deserializer, tp.topic, record.key)
                        value = self._deserialize(value_deserializer, tp.topic, record.value)
                        headers = record.headers
                        header_size = sum(
                            len(h_key.encode("utf-8")) + (len(h_val) if h_val is not None else 0) for h_key, h_val in
                            headers) if headers else -1
                        self.records_read += 1
                        self.bytes_read += record.size_in_bytes
                        self.next_fetch_offset = record.offset + 1
                        yield ConsumerRecord(
                            tp.topic, tp.partition, self.leader_epoch, record.offset, record.timestamp,
                            record.timestamp_type, key, value, headers, record.checksum,
                            key_size, value_size, header_size)

                    batch = records.next_batch()
                else:
                    # Message format v2 preserves the last offset in a batch even if the last record is removed
                    # through compaction. By using the next offset computed from the last offset in the batch,
                    # we ensure that the offset of the next fetch will point to the next batch, which avoids
                    # unnecessary re-fetching of the same batch (in the worst case, the consumer could get stuck
                    # fetching the same batch repeatedly).
                    if last_batch and last_batch.magic == 2:
                        self.next_fetch_offset = last_batch.base_offset + last_batch.last_offset_delta + 1
                    self.drain()

            # If unpacking raises StopIteration, it is erroneously
            # caught by the generator. We want all exceptions to be raised
            # back to the user. See Issue 545
            except StopIteration:
                log.exception('StopIteration raised unpacking messageset')
                raise RuntimeError('StopIteration raised unpacking messageset')

        def _deserialize(self, f, topic, bytes_):
            if not f:
                return bytes_
            if isinstance(f, Deserializer):
                return f.deserialize(topic, bytes_)
            return f(bytes_)


class FetchSessionHandler(object):
    """
    FetchSessionHandler maintains the fetch session state for connecting to a broker.

    Using the protocol outlined by KIP-227, clients can create incremental fetch sessions.
    These sessions allow the client to fetch information about a set of partition over
    and over, without explicitly enumerating all the partitions in the request and the
    response.

    FetchSessionHandler tracks the partitions which are in the session.  It also
    determines which partitions need to be included in each fetch request, and what
    the attached fetch session metadata should be for each request.
    """

    def __init__(self, node_id):
        self.node_id = node_id
        self.next_metadata = FetchMetadata.INITIAL
        self.session_partitions = {}

    def build_next(self, next_partitions):
        """
        Arguments:
            next_partitions (dict): TopicPartition -> TopicPartitionState

        Returns:
            FetchRequestData
        """
        if self.next_metadata.is_full:
            log.debug("Built full fetch %s for node %s with %s partition(s).",
                self.next_metadata, self.node_id, len(next_partitions))
            self.session_partitions = next_partitions
            return FetchRequestData(next_partitions, None, self.next_metadata)

        prev_tps = set(self.session_partitions.keys())
        next_tps = set(next_partitions.keys())
        log.debug("Building incremental partitions from next: %s, previous: %s", next_tps, prev_tps)
        added = next_tps - prev_tps
        for tp in added:
            self.session_partitions[tp] = next_partitions[tp]
        removed = prev_tps - next_tps
        for tp in removed:
            self.session_partitions.pop(tp)
        altered = set()
        for tp in next_tps & prev_tps:
            if next_partitions[tp] != self.session_partitions[tp]:
                self.session_partitions[tp] = next_partitions[tp]
                altered.add(tp)

        log.debug("Built incremental fetch %s for node %s. Added %s, altered %s, removed %s out of %s",
                  self.next_metadata, self.node_id, added, altered, removed, self.session_partitions.keys())
        to_send = collections.OrderedDict({tp: next_partitions[tp] for tp in next_partitions if tp in (added | altered)})
        return FetchRequestData(to_send, removed, self.next_metadata)

    def handle_response(self, response):
        if response.error_code != Errors.NoError.errno:
            error_type = Errors.for_code(response.error_code)
            log.info("Node %s was unable to process the fetch request with %s: %s.",
                self.node_id, self.next_metadata, error_type())
            if error_type is Errors.FetchSessionIdNotFoundError:
                self.next_metadata = FetchMetadata.INITIAL
            else:
                self.next_metadata = self.next_metadata.next_close_existing()
            return False

        response_tps = self._response_partitions(response)
        session_tps = set(self.session_partitions.keys())
        if self.next_metadata.is_full:
            if response_tps != session_tps:
                log.info("Node %s sent an invalid full fetch response with extra %s / omitted %s",
                         self.node_id, response_tps - session_tps, session_tps - response_tps)
                self.next_metadata = FetchMetadata.INITIAL
                return False
            elif response.session_id == FetchMetadata.INVALID_SESSION_ID:
                log.debug("Node %s sent a full fetch response with %s partitions",
                          self.node_id, len(response_tps))
                self.next_metadata = FetchMetadata.INITIAL
                return True
            elif response.session_id == FetchMetadata.THROTTLED_SESSION_ID:
                log.debug("Node %s sent a empty full fetch response due to a quota violation (%s partitions)",
                          self.node_id, len(response_tps))
                # Keep current metadata
                return True
            else:
                # The server created a new incremental fetch session.
                log.debug("Node %s sent a full fetch response that created a new incremental fetch session %s"
                          " with %s response partitions",
                          self.node_id, response.session_id,
                          len(response_tps))
                self.next_metadata = FetchMetadata.new_incremental(response.session_id)
                return True
        else:
            if response_tps - session_tps:
                log.info("Node %s sent an invalid incremental fetch response with extra partitions %s",
                         self.node_id, response_tps - session_tps)
                self.next_metadata = self.next_metadata.next_close_existing()
                return False
            elif response.session_id == FetchMetadata.INVALID_SESSION_ID:
                # The incremental fetch session was closed by the server.
                log.debug("Node %s sent an incremental fetch response closing session %s"
                          " with %s response partitions (%s implied)",
                          self.node_id, self.next_metadata.session_id,
                          len(response_tps), len(self.session_partitions) - len(response_tps))
                self.next_metadata = FetchMetadata.INITIAL
                return True
            elif response.session_id == FetchMetadata.THROTTLED_SESSION_ID:
                log.debug("Node %s sent a empty incremental fetch response due to a quota violation (%s partitions)",
                          self.node_id, len(response_tps))
                # Keep current metadata
                return True
            else:
                # The incremental fetch session was continued by the server.
                log.debug("Node %s sent an incremental fetch response for session %s"
                          " with %s response partitions (%s implied)",
                          self.node_id, response.session_id,
                          len(response_tps), len(self.session_partitions) - len(response_tps))
                self.next_metadata = self.next_metadata.next_incremental()
                return True

    def handle_error(self, _exception):
        self.next_metadata = self.next_metadata.next_close_existing()

    def _response_partitions(self, response):
        return {TopicPartition(topic, partition_data[0])
                for topic, partitions in response.topics
                for partition_data in partitions}


class FetchMetadata(object):
    __slots__ = ('session_id', 'epoch')

    MAX_EPOCH = 2147483647
    INVALID_SESSION_ID = 0 # used by clients with no session.
    THROTTLED_SESSION_ID = -1 # returned with empty response on quota violation
    INITIAL_EPOCH = 0 # client wants to create or recreate a session.
    FINAL_EPOCH = -1 # client wants to close any existing session, and not create a new one.

    def __init__(self, session_id, epoch):
        self.session_id = session_id
        self.epoch = epoch

    @property
    def is_full(self):
        return self.epoch == self.INITIAL_EPOCH or self.epoch == self.FINAL_EPOCH

    @classmethod
    def next_epoch(cls, prev_epoch):
        if prev_epoch < 0:
            return cls.FINAL_EPOCH
        elif prev_epoch == cls.MAX_EPOCH:
            return 1
        else:
            return prev_epoch + 1

    def next_close_existing(self):
        return self.__class__(self.session_id, self.INITIAL_EPOCH)

    @classmethod
    def new_incremental(cls, session_id):
        return cls(session_id, cls.next_epoch(cls.INITIAL_EPOCH))

    def next_incremental(self):
        return self.__class__(self.session_id, self.next_epoch(self.epoch))

FetchMetadata.INITIAL = FetchMetadata(FetchMetadata.INVALID_SESSION_ID, FetchMetadata.INITIAL_EPOCH)
FetchMetadata.LEGACY = FetchMetadata(FetchMetadata.INVALID_SESSION_ID, FetchMetadata.FINAL_EPOCH)


class FetchRequestData(object):
    __slots__ = ('_to_send', '_to_forget', '_metadata')

    def __init__(self, to_send, to_forget, metadata):
        self._to_send = to_send or dict() # {TopicPartition: (partition, ...)}
        self._to_forget = to_forget or set() # {TopicPartition}
        self._metadata = metadata

    @property
    def metadata(self):
        return self._metadata

    @property
    def id(self):
        return self._metadata.session_id

    @property
    def epoch(self):
        return self._metadata.epoch

    @property
    def to_send(self):
        # Return as list of [(topic, [(partition, ...), ...]), ...]
        # so it can be passed directly to encoder
        partition_data = collections.defaultdict(list)
        for tp, partition_info in six.iteritems(self._to_send):
            partition_data[tp.topic].append(partition_info)
        return list(partition_data.items())

    @property
    def to_forget(self):
        # Return as list of [(topic, (partiiton, ...)), ...]
        # so it an be passed directly to encoder
        partition_data = collections.defaultdict(list)
        for tp in self._to_forget:
            partition_data[tp.topic].append(tp.partition)
        return list(partition_data.items())


class FetchMetrics(object):
    __slots__ = ('total_bytes', 'total_records')

    def __init__(self):
        self.total_bytes = 0
        self.total_records = 0


class FetchResponseMetricAggregator(object):
    """
    Since we parse the message data for each partition from each fetch
    response lazily, fetch-level metrics need to be aggregated as the messages
    from each partition are parsed. This class is used to facilitate this
    incremental aggregation.
    """
    def __init__(self, sensors, partitions):
        self.sensors = sensors
        self.unrecorded_partitions = partitions
        self.fetch_metrics = FetchMetrics()
        self.topic_fetch_metrics = collections.defaultdict(FetchMetrics)

    def record(self, partition, num_bytes, num_records):
        """
        After each partition is parsed, we update the current metric totals
        with the total bytes and number of records parsed. After all partitions
        have reported, we write the metric.
        """
        self.unrecorded_partitions.remove(partition)
        self.fetch_metrics.total_bytes += num_bytes
        self.fetch_metrics.total_records += num_records
        self.topic_fetch_metrics[partition.topic].total_bytes += num_bytes
        self.topic_fetch_metrics[partition.topic].total_records += num_records

        # once all expected partitions from the fetch have reported in, record the metrics
        if not self.unrecorded_partitions:
            self.sensors.bytes_fetched.record(self.fetch_metrics.total_bytes)
            self.sensors.records_fetched.record(self.fetch_metrics.total_records)
            for topic, metrics in six.iteritems(self.topic_fetch_metrics):
                self.sensors.record_topic_fetch_metrics(topic, metrics.total_bytes, metrics.total_records)


class FetchManagerMetrics(object):
    def __init__(self, metrics, prefix):
        self.metrics = metrics
        self.group_name = '%s-fetch-manager-metrics' % (prefix,)

        self.bytes_fetched = metrics.sensor('bytes-fetched')
        self.bytes_fetched.add(metrics.metric_name('fetch-size-avg', self.group_name,
            'The average number of bytes fetched per request'), Avg())
        self.bytes_fetched.add(metrics.metric_name('fetch-size-max', self.group_name,
            'The maximum number of bytes fetched per request'), Max())
        self.bytes_fetched.add(metrics.metric_name('bytes-consumed-rate', self.group_name,
            'The average number of bytes consumed per second'), Rate())

        self.records_fetched = self.metrics.sensor('records-fetched')
        self.records_fetched.add(metrics.metric_name('records-per-request-avg', self.group_name,
            'The average number of records in each request'), Avg())
        self.records_fetched.add(metrics.metric_name('records-consumed-rate', self.group_name,
            'The average number of records consumed per second'), Rate())

        self.fetch_latency = metrics.sensor('fetch-latency')
        self.fetch_latency.add(metrics.metric_name('fetch-latency-avg', self.group_name,
            'The average time taken for a fetch request.'), Avg())
        self.fetch_latency.add(metrics.metric_name('fetch-latency-max', self.group_name,
            'The max time taken for any fetch request.'), Max())
        self.fetch_latency.add(metrics.metric_name('fetch-rate', self.group_name,
            'The number of fetch requests per second.'), Rate(sampled_stat=Count()))

        self.records_fetch_lag = metrics.sensor('records-lag')
        self.records_fetch_lag.add(metrics.metric_name('records-lag-max', self.group_name,
            'The maximum lag in terms of number of records for any partition in self window'), Max())

    def record_topic_fetch_metrics(self, topic, num_bytes, num_records):
        # record bytes fetched
        name = '.'.join(['topic', topic, 'bytes-fetched'])
        bytes_fetched = self.metrics.get_sensor(name)
        if not bytes_fetched:
            metric_tags = {'topic': topic.replace('.', '_')}

            bytes_fetched = self.metrics.sensor(name)
            bytes_fetched.add(self.metrics.metric_name('fetch-size-avg',
                    self.group_name,
                    'The average number of bytes fetched per request for topic %s' % (topic,),
                    metric_tags), Avg())
            bytes_fetched.add(self.metrics.metric_name('fetch-size-max',
                    self.group_name,
                    'The maximum number of bytes fetched per request for topic %s' % (topic,),
                    metric_tags), Max())
            bytes_fetched.add(self.metrics.metric_name('bytes-consumed-rate',
                    self.group_name,
                    'The average number of bytes consumed per second for topic %s' % (topic,),
                    metric_tags), Rate())
        bytes_fetched.record(num_bytes)

        # record records fetched
        name = '.'.join(['topic', topic, 'records-fetched'])
        records_fetched = self.metrics.get_sensor(name)
        if not records_fetched:
            metric_tags = {'topic': topic.replace('.', '_')}

            records_fetched = self.metrics.sensor(name)
            records_fetched.add(self.metrics.metric_name('records-per-request-avg',
                    self.group_name,
                    'The average number of records in each request for topic %s' % (topic,),
                    metric_tags), Avg())
            records_fetched.add(self.metrics.metric_name('records-consumed-rate',
                    self.group_name,
                    'The average number of records consumed per second for topic %s' % (topic,),
                    metric_tags), Rate())
        records_fetched.record(num_records)
