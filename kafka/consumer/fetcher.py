from __future__ import absolute_import

import collections
import copy
import logging
import random
import sys
import time

from kafka.vendor import six

import kafka.errors as Errors
from kafka.future import Future
from kafka.metrics.stats import Avg, Count, Max, Rate
from kafka.protocol.fetch import FetchRequest
from kafka.protocol.message import PartialMessage
from kafka.protocol.offset import OffsetRequest, OffsetResetStrategy
from kafka.serializer import Deserializer
from kafka.structs import TopicPartition

log = logging.getLogger(__name__)


ConsumerRecord = collections.namedtuple("ConsumerRecord",
    ["topic", "partition", "offset", "timestamp", "timestamp_type",
     "key", "value", "checksum", "serialized_key_size", "serialized_value_size"])


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
        'max_partition_fetch_bytes': 1048576,
        'max_poll_records': sys.maxsize,
        'check_crcs': True,
        'skip_double_compressed_messages': False,
        'iterator_refetch_records': 1, # undocumented -- interface may change
        'metric_group_prefix': 'consumer',
        'api_version': (0, 8, 0),
    }

    def __init__(self, client, subscriptions, metrics, **configs):
        """Initialize a Kafka Message Fetcher.

        Keyword Arguments:
            key_deserializer (callable): Any callable that takes a
                raw message key and returns a deserialized key.
            value_deserializer (callable, optional): Any callable that takes a
                raw message value and returns a deserialized value.
            fetch_min_bytes (int): Minimum amount of data the server should
                return for a fetch request, otherwise wait up to
                fetch_max_wait_ms for more data to accumulate. Default: 1.
            fetch_max_wait_ms (int): The maximum amount of time in milliseconds
                the server will block before answering the fetch request if
                there isn't sufficient data to immediately satisfy the
                requirement given by fetch_min_bytes. Default: 500.
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
            skip_double_compressed_messages (bool): A bug in KafkaProducer
                caused some messages to be corrupted via double-compression.
                By default, the fetcher will return the messages as a compressed
                blob of bytes with a single offset, i.e. how the message was
                actually published to the cluster. If you prefer to have the
                fetcher automatically detect corrupt messages and skip them,
                set this option to True. Default: False.
        """
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
        self._iterator = None
        self._fetch_futures = collections.deque()
        self._sensors = FetchManagerMetrics(metrics, self.config['metric_group_prefix'])

    def send_fetches(self):
        """Send FetchRequests asynchronously for all assigned partitions.

        Note: noop if there are unconsumed records internal to the fetcher

        Returns:
            List of Futures: each future resolves to a FetchResponse
        """
        futures = []
        for node_id, request in six.iteritems(self._create_fetch_requests()):
            if self._client.ready(node_id):
                log.debug("Sending FetchRequest to node %s", node_id)
                future = self._client.send(node_id, request)
                future.add_callback(self._handle_fetch_response, request, time.time())
                future.add_errback(log.error, 'Fetch to node %s failed: %s', node_id)
                futures.append(future)
        self._fetch_futures.extend(futures)
        self._clean_done_fetch_futures()
        return futures

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

    def update_fetch_positions(self, partitions):
        """Update the fetch positions for the provided partitions.

        Arguments:
            partitions (list of TopicPartitions): partitions to update

        Raises:
            NoOffsetForPartitionError: if no offset is stored for a given
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

        Arguments:
            partition (TopicPartition): the partition that needs reset offset

        Raises:
            NoOffsetForPartitionError: if no offset reset strategy is defined
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

        Arguments:
            partition The partition that needs fetching offset.
            timestamp (int): timestamp for fetching offset. -1 for the latest
                available, -2 for the earliest available. Otherwise timestamp
                is treated as epoch seconds.

        Returns:
            int: message offset
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
                self._client.poll(future=refresh_future, sleep=True)

    def _raise_if_offset_out_of_range(self):
        """Check FetchResponses for offset out of range.

        Raises:
            OffsetOutOfRangeError: if any partition from previous FetchResponse
                contains OffsetOutOfRangeError and the default_reset_policy is
                None
        """
        if not self._offset_out_of_range_partitions:
            return

        current_out_of_range_partitions = {}

        # filter only the fetchable partitions
        for partition, offset in six.iteritems(self._offset_out_of_range_partitions):
            if not self._subscriptions.is_fetchable(partition):
                log.debug("Ignoring fetched records for %s since it is no"
                          " longer fetchable", partition)
                continue
            position = self._subscriptions.assignment[partition].position
            # ignore partition if the current position != offset in FetchResponse
            # e.g. after seek()
            if position is not None and offset == position:
                current_out_of_range_partitions[partition] = position

        self._offset_out_of_range_partitions.clear()
        if current_out_of_range_partitions:
            raise Errors.OffsetOutOfRangeError(current_out_of_range_partitions)

    def _raise_if_unauthorized_topics(self):
        """Check FetchResponses for topic authorization failures.

        Raises:
            TopicAuthorizationFailedError
        """
        if self._unauthorized_topics:
            topics = set(self._unauthorized_topics)
            self._unauthorized_topics.clear()
            raise Errors.TopicAuthorizationFailedError(topics)

    def _raise_if_record_too_large(self):
        """Check FetchResponses for messages larger than the max per partition.

        Raises:
            RecordTooLargeError: if there is a message larger than fetch size
        """
        if not self._record_too_large_partitions:
            return

        copied_record_too_large_partitions = dict(self._record_too_large_partitions)
        self._record_too_large_partitions.clear()

        raise RecordTooLargeError(
            "There are some messages at [Partition=Offset]: %s "
            " whose size is larger than the fetch size %s"
            " and hence cannot be ever returned."
            " Increase the fetch size, or decrease the maximum message"
            " size the broker will allow.",
            copied_record_too_large_partitions,
            self.config['max_partition_fetch_bytes'])

    def fetched_records(self, max_records=None):
        """Returns previously fetched records and updates consumed offsets.

        Arguments:
            max_records (int): Maximum number of records returned. Defaults
                to max_poll_records configuration.

        Raises:
            OffsetOutOfRangeError: if no subscription offset_reset_strategy
            InvalidMessageError: if message crc validation fails (check_crcs
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

        if self._subscriptions.needs_partition_assignment:
            return {}, False

        self._raise_if_offset_out_of_range()
        self._raise_if_unauthorized_topics()
        self._raise_if_record_too_large()

        drained = collections.defaultdict(list)
        partial = bool(self._records and max_records)
        while self._records and max_records > 0:
            part = self._records.popleft()
            max_records -= self._append(drained, part, max_records)
            if part.has_more():
                self._records.appendleft(part)
            else:
                partial &= False
        return dict(drained), partial

    def _append(self, drained, part, max_records):
        tp = part.topic_partition
        fetch_offset = part.fetch_offset
        if not self._subscriptions.is_assigned(tp):
            # this can happen when a rebalance happened before
            # fetched records are returned to the consumer's poll call
            log.debug("Not returning fetched records for partition %s"
                      " since it is no longer assigned", tp)
        else:
            # note that the position should always be available
            # as long as the partition is still assigned
            position = self._subscriptions.assignment[tp].position
            if not self._subscriptions.is_fetchable(tp):
                # this can happen when a partition is paused before
                # fetched records are returned to the consumer's poll call
                log.debug("Not returning fetched records for assigned partition"
                          " %s since it is no longer fetchable", tp)

            elif fetch_offset == position:
                part_records = part.take(max_records)
                if not part_records:
                    return 0
                next_offset = part_records[-1].offset + 1

                log.log(0, "Returning fetched records at offset %d for assigned"
                           " partition %s and update position to %s", position,
                           tp, next_offset)

                for record in part_records:
                    # Fetched compressed messages may include additional records
                    if record.offset < fetch_offset:
                        log.debug("Skipping message offset: %s (expecting %s)",
                                  record.offset, fetch_offset)
                        continue
                    drained[tp].append(record)

                self._subscriptions.assignment[tp].position = next_offset
                return len(part_records)

            else:
                # these records aren't next in line based on the last consumed
                # position, ignore them they must be from an obsolete request
                log.debug("Ignoring fetched records for %s at offset %s since"
                          " the current position is %d", tp, part.fetch_offset,
                          position)

        part.discard()
        return 0

    def _message_generator(self):
        """Iterate over fetched_records"""
        if self._subscriptions.needs_partition_assignment:
            raise StopIteration('Subscription needs partition assignment')

        while self._records:

            # Check on each iteration since this is a generator
            self._raise_if_offset_out_of_range()
            self._raise_if_unauthorized_topics()
            self._raise_if_record_too_large()

            # Send additional FetchRequests when the internal queue is low
            # this should enable moderate pipelining
            if len(self._records) <= self.config['iterator_refetch_records']:
                self.send_fetches()

            part = self._records.popleft()

            tp = part.topic_partition
            fetch_offset = part.fetch_offset
            if not self._subscriptions.is_assigned(tp):
                # this can happen when a rebalance happened before
                # fetched records are returned
                log.debug("Not returning fetched records for partition %s"
                          " since it is no longer assigned", tp)
                continue

            # note that the position should always be available
            # as long as the partition is still assigned
            position = self._subscriptions.assignment[tp].position
            if not self._subscriptions.is_fetchable(tp):
                # this can happen when a partition is paused before
                # fetched records are returned
                log.debug("Not returning fetched records for assigned partition"
                          " %s since it is no longer fetchable", tp)

            elif fetch_offset == position:
                log.log(0, "Returning fetched records at offset %d for assigned"
                           " partition %s", position, tp)

                # We can ignore any prior signal to drop pending message sets
                # because we are starting from a fresh one where fetch_offset == position
                # i.e., the user seek()'d to this position
                self._subscriptions.assignment[tp].drop_pending_message_set = False

                for msg in part.messages:

                    # Because we are in a generator, it is possible for
                    # subscription state to change between yield calls
                    # so we need to re-check on each loop
                    # this should catch assignment changes, pauses
                    # and resets via seek_to_beginning / seek_to_end
                    if not self._subscriptions.is_fetchable(tp):
                        log.debug("Not returning fetched records for partition %s"
                                  " since it is no longer fetchable", tp)
                        break

                    # If there is a seek during message iteration,
                    # we should stop unpacking this message set and
                    # wait for a new fetch response that aligns with the
                    # new seek position
                    elif self._subscriptions.assignment[tp].drop_pending_message_set:
                        log.debug("Skipping remainder of message set for partition %s", tp)
                        self._subscriptions.assignment[tp].drop_pending_message_set = False
                        break

                    # Compressed messagesets may include earlier messages
                    elif msg.offset < self._subscriptions.assignment[tp].position:
                        log.debug("Skipping message offset: %s (expecting %s)",
                                  msg.offset,
                                  self._subscriptions.assignment[tp].position)
                        continue

                    self._subscriptions.assignment[tp].position = msg.offset + 1
                    yield msg

            else:
                # these records aren't next in line based on the last consumed
                # position, ignore them they must be from an obsolete request
                log.debug("Ignoring fetched records for %s at offset %s since"
                          " the current position is %d", tp, part.fetch_offset,
                          position)

    def _unpack_message_set(self, tp, messages):
        try:
            for offset, size, msg in messages:
                if self.config['check_crcs'] and not msg.validate_crc():
                    raise Errors.InvalidMessageError(msg)
                elif msg.is_compressed():
                    # If relative offset is used, we need to decompress the entire message first to compute
                    # the absolute offset.
                    inner_mset = msg.decompress()

                    # There should only ever be a single layer of compression
                    if inner_mset[0][-1].is_compressed():
                        log.warning('MessageSet at %s offset %d appears '
                                    ' double-compressed. This should not'
                                    ' happen -- check your producers!',
                                    tp, offset)
                        if self.config['skip_double_compressed_messages']:
                            log.warning('Skipping double-compressed message at'
                                        ' %s %d', tp, offset)
                            continue

                    if msg.magic > 0:
                        last_offset, _, _ = inner_mset[-1]
                        absolute_base_offset = offset - last_offset
                    else:
                        absolute_base_offset = -1

                    for inner_offset, inner_size, inner_msg in inner_mset:
                        if msg.magic > 0:
                            # When magic value is greater than 0, the timestamp
                            # of a compressed message depends on the
                            # typestamp type of the wrapper message:

                            if msg.timestamp_type == 0: # CREATE_TIME (0)
                                inner_timestamp = inner_msg.timestamp

                            elif msg.timestamp_type == 1: # LOG_APPEND_TIME (1)
                                inner_timestamp = msg.timestamp

                            else:
                                raise ValueError('Unknown timestamp type: {0}'.format(msg.timestamp_type))
                        else:
                            inner_timestamp = msg.timestamp

                        if absolute_base_offset >= 0:
                            inner_offset += absolute_base_offset

                        key = self._deserialize(
                            self.config['key_deserializer'],
                            tp.topic, inner_msg.key)
                        value = self._deserialize(
                            self.config['value_deserializer'],
                            tp.topic, inner_msg.value)
                        yield ConsumerRecord(tp.topic, tp.partition, inner_offset,
                                             inner_timestamp, msg.timestamp_type,
                                             key, value, inner_msg.crc,
                                             len(inner_msg.key) if inner_msg.key is not None else -1,
                                             len(inner_msg.value) if inner_msg.value is not None else -1)

                else:
                    key = self._deserialize(
                        self.config['key_deserializer'],
                        tp.topic, msg.key)
                    value = self._deserialize(
                        self.config['value_deserializer'],
                        tp.topic, msg.value)
                    yield ConsumerRecord(tp.topic, tp.partition, offset,
                                         msg.timestamp, msg.timestamp_type,
                                         key, value, msg.crc,
                                         len(msg.key) if msg.key is not None else -1,
                                         len(msg.value) if msg.value is not None else -1)

        # If unpacking raises StopIteration, it is erroneously
        # caught by the generator. We want all exceptions to be raised
        # back to the user. See Issue 545
        except StopIteration as e:
            log.exception('StopIteration raised unpacking messageset: %s', e)
            raise Exception('StopIteration raised unpacking messageset')

    def __iter__(self):  # pylint: disable=non-iterator-returned
        return self

    def __next__(self):
        if not self._iterator:
            self._iterator = self._message_generator()
        try:
            return next(self._iterator)
        except StopIteration:
            self._iterator = None
            raise

    def _deserialize(self, f, topic, bytes_):
        if not f:
            return bytes_
        if isinstance(f, Deserializer):
            return f.deserialize(topic, bytes_)
        return f(bytes_)

    def _send_offset_request(self, partition, timestamp):
        """Fetch a single offset before the given timestamp for the partition.

        Arguments:
            partition (TopicPartition): partition that needs fetching offset
            timestamp (int): timestamp for fetching offset

        Returns:
            Future: resolves to the corresponding offset
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

        request = OffsetRequest[0](
            -1, [(partition.topic, [(partition.partition, timestamp, 1)])]
        )
        # Client returns a future that only fails on network issues
        # so create a separate future and attach a callback to update it
        # based on response error codes
        future = Future()
        _f = self._client.send(node_id, request)
        _f.add_callback(self._handle_offset_response, partition, future)
        _f.add_errback(lambda e: future.failure(e))
        return future

    def _handle_offset_response(self, partition, future, response):
        """Callback for the response of the list offset call above.

        Arguments:
            partition (TopicPartition): The partition that was fetched
            future (Future): the future to update based on response
            response (OffsetResponse): response from the server

        Raises:
            AssertionError: if response does not match partition
        """
        topic, partition_info = response.topics[0]
        assert len(response.topics) == 1 and len(partition_info) == 1, (
            'OffsetResponse should only be for a single topic-partition')

        part, error_code, offsets = partition_info[0]
        assert topic == partition.topic and part == partition.partition, (
            'OffsetResponse partition does not match OffsetRequest partition')

        error_type = Errors.for_code(error_code)
        if error_type is Errors.NoError:
            assert len(offsets) == 1, 'Expected OffsetResponse with one offset'
            offset = offsets[0]
            log.debug("Fetched offset %d for partition %s", offset, partition)
            future.success(offset)
        elif error_type in (Errors.NotLeaderForPartitionError,
                       Errors.UnknownTopicOrPartitionError):
            log.debug("Attempt to fetch offsets for partition %s failed due"
                      " to obsolete leadership information, retrying.",
                      partition)
            future.failure(error_type(partition))
        else:
            log.warning("Attempt to fetch offsets for partition %s failed due to:"
                        " %s", partition, error_type)
            future.failure(error_type(partition))

    def _fetchable_partitions(self):
        fetchable = self._subscriptions.fetchable_partitions()
        pending = set([part.topic_partition for part in self._records])
        return fetchable.difference(pending)

    def _create_fetch_requests(self):
        """Create fetch requests for all assigned partitions, grouped by node.

        FetchRequests skipped if no leader, or node has requests in flight

        Returns:
            dict: {node_id: FetchRequest, ...} (version depends on api_version)
        """
        # create the fetch info as a dict of lists of partition info tuples
        # which can be passed to FetchRequest() via .items()
        fetchable = collections.defaultdict(lambda: collections.defaultdict(list))

        for partition in self._fetchable_partitions():
            node_id = self._client.cluster.leader_for_partition(partition)
            position = self._subscriptions.assignment[partition].position

            # fetch if there is a leader and no in-flight requests
            if node_id is None or node_id == -1:
                log.debug("No leader found for partition %s."
                          " Requesting metadata update", partition)
                self._client.cluster.request_update()

            elif self._client.in_flight_request_count(node_id) == 0:
                partition_info = (
                    partition.partition,
                    position,
                    self.config['max_partition_fetch_bytes']
                )
                fetchable[node_id][partition.topic].append(partition_info)
                log.debug("Adding fetch request for partition %s at offset %d",
                          partition, position)

        if self.config['api_version'] >= (0, 10):
            version = 2
        elif self.config['api_version'] == (0, 9):
            version = 1
        else:
            version = 0
        requests = {}
        for node_id, partition_data in six.iteritems(fetchable):
            requests[node_id] = FetchRequest[version](
                -1, # replica_id
                self.config['fetch_max_wait_ms'],
                self.config['fetch_min_bytes'],
                partition_data.items())
        return requests

    def _handle_fetch_response(self, request, send_time, response):
        """The callback for fetch completion"""
        total_bytes = 0
        total_count = 0
        recv_time = time.time()

        fetch_offsets = {}
        for topic, partitions in request.topics:
            for partition, offset, _ in partitions:
                fetch_offsets[TopicPartition(topic, partition)] = offset

        # randomized ordering should improve balance for short-lived consumers
        random.shuffle(response.topics)
        for topic, partitions in response.topics:
            random.shuffle(partitions)
            for partition, error_code, highwater, messages in partitions:
                tp = TopicPartition(topic, partition)
                error_type = Errors.for_code(error_code)
                if not self._subscriptions.is_fetchable(tp):
                    # this can happen when a rebalance happened or a partition
                    # consumption paused while fetch is still in-flight
                    log.debug("Ignoring fetched records for partition %s"
                              " since it is no longer fetchable", tp)

                elif error_type is Errors.NoError:
                    self._subscriptions.assignment[tp].highwater = highwater

                    # we are interested in this fetch only if the beginning
                    # offset (of the *request*) matches the current consumed position
                    # Note that the *response* may return a messageset that starts
                    # earlier (e.g., compressed messages) or later (e.g., compacted topic)
                    fetch_offset = fetch_offsets[tp]
                    position = self._subscriptions.assignment[tp].position
                    if position is None or position != fetch_offset:
                        log.debug("Discarding fetch response for partition %s"
                                  " since its offset %d does not match the"
                                  " expected offset %d", tp, fetch_offset,
                                  position)
                        continue

                    num_bytes = 0
                    partial = None
                    if messages and isinstance(messages[-1][-1], PartialMessage):
                        partial = messages.pop()

                    if messages:
                        log.debug("Adding fetched record for partition %s with"
                                  " offset %d to buffered record list", tp,
                                  position)
                        unpacked = list(self._unpack_message_set(tp, messages))
                        self._records.append(self.PartitionRecords(fetch_offset, tp, unpacked))
                        last_offset, _, _ = messages[-1]
                        self._sensors.records_fetch_lag.record(highwater - last_offset)
                        num_bytes = sum(msg[1] for msg in messages)
                    elif partial:
                        # we did not read a single message from a non-empty
                        # buffer because that message's size is larger than
                        # fetch size, in this case record this exception
                        self._record_too_large_partitions[tp] = fetch_offset

                    self._sensors.record_topic_fetch_metrics(topic, num_bytes, len(messages))
                    total_bytes += num_bytes
                    total_count += len(messages)
                elif error_type in (Errors.NotLeaderForPartitionError,
                                    Errors.UnknownTopicOrPartitionError):
                    self._client.cluster.request_update()
                elif error_type is Errors.OffsetOutOfRangeError:
                    fetch_offset = fetch_offsets[tp]
                    log.info("Fetch offset %s is out of range for topic-partition %s", fetch_offset, tp)
                    if self._subscriptions.has_default_offset_reset_policy():
                        self._subscriptions.need_offset_reset(tp)
                        log.info("Resetting offset for topic-partition %s", tp)
                    else:
                        self._offset_out_of_range_partitions[tp] = fetch_offset
                elif error_type is Errors.TopicAuthorizationFailedError:
                    log.warn("Not authorized to read from topic %s.", tp.topic)
                    self._unauthorized_topics.add(tp.topic)
                elif error_type is Errors.UnknownError:
                    log.warn("Unknown error fetching data for topic-partition %s", tp)
                else:
                    raise error_type('Unexpected error while fetching data')

        # Because we are currently decompressing messages lazily, the sensors here
        # will get compressed bytes / message set stats when compression is enabled
        self._sensors.bytes_fetched.record(total_bytes)
        self._sensors.records_fetched.record(total_count)
        if response.API_VERSION >= 1:
            self._sensors.fetch_throttle_time_sensor.record(response.throttle_time_ms)
        self._sensors.fetch_latency.record((recv_time - send_time) * 1000)

    class PartitionRecords(six.Iterator):
        def __init__(self, fetch_offset, tp, messages):
            self.fetch_offset = fetch_offset
            self.topic_partition = tp
            self.messages = messages
            self.message_idx = 0

        def discard(self):
            self.messages = None

        def take(self, n):
            if not self.has_more():
                return []
            next_idx = self.message_idx + n
            res = self.messages[self.message_idx:next_idx]
            self.message_idx = next_idx
            if self.has_more():
                self.fetch_offset = self.messages[self.message_idx].offset
            return res

        def has_more(self):
            return self.messages and self.message_idx < len(self.messages)


class FetchManagerMetrics(object):
    def __init__(self, metrics, prefix):
        self.metrics = metrics
        self.group_name = '%s-fetch-manager-metrics' % prefix

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

        self.fetch_throttle_time_sensor = metrics.sensor('fetch-throttle-time')
        self.fetch_throttle_time_sensor.add(metrics.metric_name('fetch-throttle-time-avg', self.group_name,
            'The average throttle time in ms'), Avg())
        self.fetch_throttle_time_sensor.add(metrics.metric_name('fetch-throttle-time-max', self.group_name,
            'The maximum throttle time in ms'), Max())

    def record_topic_fetch_metrics(self, topic, num_bytes, num_records):
        # record bytes fetched
        name = '.'.join(['topic', topic, 'bytes-fetched'])
        bytes_fetched = self.metrics.get_sensor(name)
        if not bytes_fetched:
            metric_tags = {'topic': topic.replace('.', '_')}

            bytes_fetched = self.metrics.sensor(name)
            bytes_fetched.add(self.metrics.metric_name('fetch-size-avg',
                    self.group_name,
                    'The average number of bytes fetched per request for topic %s' % topic,
                    metric_tags), Avg())
            bytes_fetched.add(self.metrics.metric_name('fetch-size-max',
                    self.group_name,
                    'The maximum number of bytes fetched per request for topic %s' % topic,
                    metric_tags), Max())
            bytes_fetched.add(self.metrics.metric_name('bytes-consumed-rate',
                    self.group_name,
                    'The average number of bytes consumed per second for topic %s' % topic,
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
                    'The average number of records in each request for topic %s' % topic,
                    metric_tags), Avg())
            records_fetched.add(self.metrics.metric_name('records-consumed-rate',
                    self.group_name,
                    'The average number of records consumed per second for topic %s' % topic,
                    metric_tags), Rate())
        records_fetched.record(num_records)
