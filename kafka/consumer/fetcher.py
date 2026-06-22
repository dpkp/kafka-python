import collections
import copy
import itertools
import logging
import sys
import time
import warnings

import kafka.errors as Errors
from kafka.future import Future
from kafka.metrics.stats import Avg, Count, Max, Rate
from kafka.protocol.consumer import FetchRequest
from kafka.protocol.consumer import (
    ListOffsetsRequest, OffsetForLeaderEpochRequest,
    OffsetSpec, UNKNOWN_OFFSET, IsolationLevel,
)
from kafka.record import MemoryRecords
from kafka.serializer import Deserializer, DeserializeWrapper
from kafka.structs import TopicPartition, OffsetAndMetadata, OffsetAndTimestamp
from kafka.util import Timer

log = logging.getLogger(__name__)

_LOGGED_DESERIALIZE_WARNING = False

ConsumerRecord = collections.namedtuple("ConsumerRecord",
    ["topic", "partition", "leader_epoch", "offset", "timestamp", "timestamp_type",
     "key", "value", "headers", "checksum", "serialized_key_size", "serialized_value_size", "serialized_header_size"])
ConsumerRecord.__doc__ = """A single record (message) consumed from a topic partition.

Yielded by :meth:`~kafka.KafkaConsumer.poll` (inside the returned
``{TopicPartition: [ConsumerRecord, ...]}`` mapping) and by iterating
over a :class:`~kafka.KafkaConsumer`. ``key`` and ``value`` are decoded
by the consumer's configured deserializers.

Keyword Arguments:
    topic (str): The topic this record was received from.
    partition (int): The partition this record was received from.
    leader_epoch (int): The partition leader epoch for this record, or -1
        if unknown.
    offset (int): The position of this record in the topic partition.
    timestamp (int): The timestamp of this record, in milliseconds since
        the epoch (UTC), or -1 if unknown.
    timestamp_type (int): The type of the timestamp: 0 for CreateTime (set
        by the producer) or 1 for LogAppendTime (set by the broker).
    key: The (deserialized) key of the record, or None.
    value: The (deserialized) value of the record, or None.
    headers (list): A list of ``(key, value)`` header tuples, where key is
        a str and value is bytes.
    checksum (int): Deprecated. The CRC32 checksum of the record, or None.
        Removed in message format v2 (Kafka 0.11+).
    serialized_key_size (int): The size of the serialized, uncompressed key
        in bytes, or -1 if the key is None.
    serialized_value_size (int): The size of the serialized, uncompressed
        value in bytes, or -1 if the value is None.
    serialized_header_size (int): The size of the serialized, uncompressed
        headers in bytes, or -1 if there are no headers.
"""


CompletedFetch = collections.namedtuple("CompletedFetch",
    ["topic_partition", "fetched_offset", "response_version",
     "partition_data", "metric_aggregator"])


ExceptionMetadata = collections.namedtuple("ExceptionMetadata",
    ["partition", "fetched_offset", "exception"])


_FetchTopic = FetchRequest.FetchTopic
_FetchPartition = _FetchTopic.FetchPartition
_ForgottenTopic = FetchRequest.ForgottenTopic
_ListOffsetsTopic = ListOffsetsRequest.ListOffsetsTopic
_ListOffsetsPartition = _ListOffsetsTopic.ListOffsetsPartition
_OffsetForLeaderTopic = OffsetForLeaderEpochRequest.OffsetForLeaderTopic
_OffsetForLeaderPartition = _OffsetForLeaderTopic.OffsetForLeaderPartition


class RecordTooLargeError(Errors.KafkaError):
    pass


class Fetcher:
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
        'request_timeout_ms': 30000,
        'retry_backoff_ms': 100,
        'enable_incremental_fetch_sessions': True,
        'isolation_level': 'read_uncommitted',
        'client_rack': '',
        'metadata_max_age_ms': 5 * 60 * 1000,
    }

    def __init__(self, client, subscriptions, **configs):
        """Initialize a Kafka Message Fetcher.

        Keyword Arguments:
            key_deserializer (kafka.serializer.Deserializer): Takes a
                raw message key and returns a deserialized key.
                Default: None.
            value_deserializer (kafka.serializer.Deserializer): Takes a
                raw message value and returns a deserialized value.
                Default: None.
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
            isolation_level (str): Configure KIP-98 transactional consumer by
                setting to 'read_committed'. This will cause the consumer to
                skip records from aborted tranactions. Default: 'read_uncommitted'
        """
        self.config = copy.copy(self.DEFAULT_CONFIG)
        for key in self.config:
            if key in configs:
                self.config[key] = configs[key]

        for key in ('key_deserializer', 'value_deserializer'):
            if self.config[key] is not None and not isinstance(self.config[key], Deserializer):
                warnings.warn('%s does not implement kafka.serializer.Deserializer' % (key,), category=DeprecationWarning, stacklevel=3)
                self.config[key] = DeserializeWrapper(self.config[key])

        try:
            self._isolation_level = IsolationLevel.build_from(self.config['isolation_level'])
        except ValueError:
            raise Errors.KafkaConfigurationError('Unrecognized isolation_level') from None

        self._client = client
        self._manager = client._manager
        self._net = self._manager._net
        self._subscriptions = subscriptions
        self._completed_fetches = collections.deque()  # Unparsed responses
        self._next_partition_records = None  # Holds a single PartitionRecords until fully consumed
        self._paused_completed_fetches = {}  # tp -> CompletedFetch (raw)
        self._paused_partition_records = {}  # tp -> PartitionRecords (parsed)
        self._iterator = None
        self._fetch_futures = collections.deque()
        if self.config['metrics']:
            self._sensors = FetchManagerMetrics(self.config['metrics'], self.config['metric_group_prefix'])
        else:
            self._sensors = None
        self._session_handlers = {}
        self._nodes_with_pending_fetch_requests = set()
        self._cached_list_offsets_exception = None
        self._next_in_line_exception_metadata = None
        # In-flight offset-reset Task, cached across reset_offsets_if_needed
        # calls so concurrent callers (consumer.poll fire-and-forget,
        # consumer.position blocking-await) share one fan-out instead of
        # racing duplicate ListOffsets requests.
        self._reset_task = None
        # KIP-320 offset validation: same caching pattern, separate from
        # reset (a partition can be awaiting-reset OR awaiting-validation,
        # never both - awaiting-validation requires a valid position).
        self._validation_task = None
        self._cached_log_truncation = None

    @property
    def _enable_incremental_fetch_sessions(self):
        if self._manager.broker_version is None or self._manager.broker_version < (1, 1):
            return False
        return self.config['enable_incremental_fetch_sessions']

    def fetch_records(self, max_records=None, update_offsets=True, timeout_ms=None):
        """Drain buffered records, pipeline next fetches, and wait briefly
        for in-flight responses if no records are immediately available.

        Single-call replacement for the legacy
        ``fetched_records -> send_fetches -> client.poll -> fetched_records``
        loop in :meth:`KafkaConsumer._poll_once`. The caller no longer
        drives the event loop; the wait happens inside this method via a
        wakeup Future fired by any in-flight fetch's completion callback.

        Arguments:
            max_records (int, optional): cap on returned records.
            update_offsets (bool): advance subscription positions for
                consumed records.
            timeout_ms (int, optional): wall-clock cap on the wait phase.
                Only applies when no records are immediately available.

        Returns:
            tuple[dict[TopicPartition, list[ConsumerRecord]], bool]:
                ``(records, idle)``. ``idle`` is True when there were no
                buffered records, no in-flight fetches, and no pending
                offset-reset task -- i.e. nothing this fetcher could wait
                on. Callers in that state should sleep before retrying
                instead of busy-looping.
        """
        # Drain whatever's already buffered from prior fetch responses.
        records, partial = self.fetched_records(
            max_records, update_offsets=update_offsets)
        if not partial:
            # No buffered records remaining; send next batch of fetch requests.
            self.send_fetches()

        if records:
            return records, False

        # No records yet. Block until either an in-flight fetch
        # completes (records may have arrived) or a pending offset-reset
        # task completes (positions become available, enabling a fetch
        # on the next caller iteration).
        #
        # add_both fires synchronously on an already-done future: if a fetch
        # response lands between the drain above and this wait setup, _wake
        # fires immediately so we re-drain instead of stalling for the full
        # timeout.
        #
        # This relies on _fetch_futures holding only *recent* completions.
        # otherwise a fetch that completed and was already drained iterations
        # ago lingers behind a slow broker's in-flight fetch and re-fires
        # _wake on every call, busy-looping the poll loop until that slow
        # fetch finally returns.
        waited_on = list(self._fetch_futures)
        if self._reset_task is not None and not self._reset_task.is_done:
            waited_on.append(self._reset_task)
        if not waited_on:
            return records, True  # nothing pending; caller should sleep

        wakeup = Future()
        def _wake(_):
            if not wakeup.is_done:
                wakeup.success(None)
        for fut in waited_on:
            fut.add_both(_wake)

        try:
            self._net.run(self._manager.wait_for, wakeup, timeout_ms)
        except Errors.KafkaTimeoutError:
            pass

        records, _ = self.fetched_records(
            max_records, update_offsets=update_offsets)
        return records, False

    def send_fetches(self):
        """Send FetchRequests for all assigned partitions that do not already have
        an in-flight fetch or pending fetch data.

        Returns:
            List of Futures: each future resolves to a FetchResponse
        """
        return self._manager.run(self._send_fetches_async)

    async def _send_fetches_async(self):
        futures = []
        for node_id, (request, fetch_offsets) in self._create_fetch_requests().items():
            log.debug("Sending FetchRequest to node %s", node_id)
            self._nodes_with_pending_fetch_requests.add(node_id)
            future = self._manager.send(request, node_id=node_id)
            future.add_callback(self._handle_fetch_response, node_id, fetch_offsets, time.monotonic())
            future.add_errback(self._handle_fetch_error, node_id)
            future.add_both(self._clear_pending_fetch_request, node_id)
            futures.append(future)
        self._fetch_futures.extend(futures)
        await self._clean_done_fetch_futures()
        return futures

    async def _clean_done_fetch_futures(self):
        # Drop every completed fetch future. With multiple brokers, fetches
        # may complete out of order. fetch_records() relies on _fetch_futures
        # holding only recent completions (it fires _wake synchronously on any
        # done future to avoid stalling -- see the wait setup there); a
        # lingering stale completion re-fires that wake on every call and busy-
        # loops the poll loop until the slow broker's in-flight fetch returns.
        #
        # Threading: this REBINDS self._fetch_futures, which must happen on the
        # IO thread so it never races the foreground's list(self._fetch_futures)
        # read in fetch_records(). Defined async to enforce that -- the body
        # can only run by being driven on the IO loop (awaited from another
        # coroutine, or scheduled via manager.run/call_soon), so the rebind
        # always executes on the IO thread regardless of who initiates it.
        # The rebind is a single atomic attribute store, so a foreground reader
        # always sees either the old or the new deque, never a half-cleaned one.
        #
        # Two alternate designs we considered (either would remove the need for
        # this "evict every done future + rebind" dance):
        #
        #   1. Wakeup flag (Apache Kafka Java client, FetchBuffer). Instead of
        #      waiting on the fetch-future objects, wait on a single consumable
        #      signal: the IO thread sets a flag (wokenup) when it buffers a
        #      completed fetch; the foreground's wait loops `while not woken:
        #      await` and consumes the flag (compareAndSet true->false) on each
        #      pass. Because the signal is cleared on consumption and is not
        #      re-derived from lingering future objects, a stale/drained
        #      completion cannot re-trigger it -- so no busy-loop and no
        #      per-call cleanup of a future list at all. This is the most
        #      faithful port of the threaded Java consumer's design.
        #
        #   2. Per-node fetch tracking. Key fetches by broker: dict[node_id,
        #      deque] (or just dict[node_id, Future], since _create_fetch_-
        #      requests keeps at most one in-flight fetch per node). Within a
        #      single connection responses return in request order, so each
        #      per-node deque completes in order and the simple head-only
        #      popleft cleanup is correct again -- no out-of-order stranding,
        #      and cleanup is an in-place popleft (atomic, no rebind, so the
        #      threading note above goes away). This structure could also
        #      subsume _nodes_with_pending_fetch_requests entirely ("pending"
        #      == the node's last future is not done), collapsing two
        #      structures into one source of truth.
        if not self._fetch_futures:
            return
        self._fetch_futures = collections.deque(
            fut for fut in self._fetch_futures if not fut.is_done)

    def in_flight_fetches(self):
        """Return True if there are any unprocessed (incomplete) FetchRequests
        in flight."""

        # Read-only on purpose: this may be called from the foreground thread,
        # which must not mutate _fetch_futures (see _clean_done_fetch_futures --
        # cleanup is IO-thread-only). Snapshot first so we never iterate the
        # deque while the IO thread extends it, and check is_done directly
        # rather than relying on a prior cleanup pass.
        return any(not fut.is_done for fut in list(self._fetch_futures))

    def reset_offsets_if_needed(self, timeout_ms=None):
        """Schedule pending offset resets and return the in-flight Task.

        Returns the cached Future for the in-flight reset task (shared
        across concurrent callers) or None if no reset is needed. Callers
        may discard the Future (fire-and-forget, e.g. consumer.poll) or
        await it via ``manager.wait_for(future, timeout_ms)`` to block
        until resets complete (e.g. consumer.position).

        Arguments:
            timeout_ms (int, optional): Maximum wall-clock the reset task
                should run, including time spent awaiting metadata refresh
                for unknown leaders. If None, uses ``request_timeout_ms``
                as a default upper bound so a permanently-unresolvable
                partition (deleted topic, etc.) doesn't spin forever. The
                first caller's timeout wins for the cached task; later
                callers' bounds are enforced via their own ``wait_for`` on
                the returned Future.

        Raises:
            NoOffsetForPartitionError: if a previous reset attempt left a
                cached non-retriable exception.
        """
        # Raise exception from previous offset fetch if there is one
        exc, self._cached_list_offsets_exception = self._cached_list_offsets_exception, None
        if exc:
            raise exc

        if self._reset_task is not None and not self._reset_task.is_done:
            return self._reset_task

        if not self._subscriptions.partitions_needing_reset():
            return None

        self._reset_task = self._manager.call_soon(
            self._reset_offsets_async, timeout_ms)
        return self._reset_task

    def offsets_by_times(self, timestamps, timeout_ms=None):
        """Fetch offset for each partition passed in ``timestamps`` map.

        Blocks until offsets are obtained, a non-retriable exception is raised
        or ``timeout_ms`` passed.

        Arguments:
            timestamps: {TopicPartition: int} dict with timestamps to fetch
                offsets by. -1 for the latest available, -2 for the earliest
                available. Otherwise timestamp is treated as epoch milliseconds.
            timeout_ms (int, optional): The maximum time in milliseconds to block.

        Returns:
            {TopicPartition: OffsetAndTimestamp or None}: Mapping of partition to
                retrieved offset, timestamp, and leader_epoch. If offset does not
                exist for the provided timestamp, the value for the TopicPartition
                will be None.

        Raises:
            KafkaTimeoutError if timeout_ms provided
        """
        offsets = self._net.run(self._fetch_offsets_by_times_async, timestamps, timeout_ms)
        for tp in timestamps:
            if tp not in offsets:
                offsets[tp] = None
        return offsets

    async def _fetch_offsets_by_times_async(self, timestamps, timeout_ms=None):
        """Fetch offsets for each partition in timestamps dict. This may send
        request to multiple nodes, based on who is Leader for partition.

        Per-node requests are dispatched concurrently; if any fails, the first
        exception encountered propagates and the remaining results are dropped.

        Arguments:
            timestamps (dict): {TopicPartition: int} mapping of partitions to
                timestamps or OffsetSpec sentinels.

        Returns:
            (fetched_offsets, partitions_to_retry):
                dict[TopicPartition, OffsetAndTimestamp],
                set[TopicPartition]

        Raises:
            KafkaTimeoutError: if offsets cannot be fully fetched before timeout_ms
        """
        if not timestamps:
            return {}

        timer = Timer(timeout_ms, "Failed to get offsets by timestamps in %s ms" % (timeout_ms,))
        timestamps = copy.copy(timestamps)
        fetched_offsets = dict()
        while True:
            if not timestamps:
                return {}

            future = self._manager.call_soon(self._send_list_offsets_requests, timestamps)
            try:
                refresh_future = None
                backoff = False
                offsets, retry = await self._manager.wait_for(future, timer.timeout_ms)
            except Errors.InvalidMetadataError:
                refresh_future = self._manager.cluster.request_update()
            except Errors.RetriableError:
                if self._manager.cluster.need_update:
                    refresh_future = self._manager.cluster.request_update()
                else:
                    backoff = True
            else:
                fetched_offsets.update(offsets)
                if not retry:
                    return fetched_offsets
                timestamps = {tp: timestamps[tp] for tp in retry}

            if refresh_future:
                try:
                    await self._manager.wait_for(refresh_future, timer.timeout_ms)
                except Errors.RetriableError:
                    backoff = True

            if backoff:
                delay = self.config['retry_backoff_ms'] / 1000
                if timer.timeout_ms is not None:
                    delay = min(delay, timer.timeout_ms / 1000)
                await self._manager._net.sleep(delay)

            timer.maybe_raise()

    def beginning_offsets(self, partitions, timeout_ms=None):
        """Fetch earliest (oldest) offset for each partition.

        Blocks until offsets are obtained, a non-retriable exception is raised
        or ``timeout_ms`` passed.

        Arguments:
            partitions ([TopicPartition]): List of partitions for list offsets.
            timeout_ms (int, optional): The maximum time in milliseconds to block.

        Returns:
            {TopicPartition: int}: Mapping of partition to retrieved offset.

        Raises:
            KafkaTimeoutError if timeout_ms provided.
        """
        return self.beginning_or_end_offset(
            partitions, OffsetSpec.EARLIEST, timeout_ms)

    def end_offsets(self, partitions, timeout_ms=None):
        """Fetch latest (most recent) offset for each partition.

        Blocks until offsets are obtained, a non-retriable exception is raised
        or ``timeout_ms`` passed.

        Arguments:
            partitions ([TopicPartition]): List of partitions for list offsets.
            timeout_ms (int, optional): The maximum time in milliseconds to block.

        Returns:
            {TopicPartition: int}: Mapping of partition to retrieved offset.

        Raises:
            KafkaTimeoutError if timeout_ms provided.
        """
        return self.beginning_or_end_offset(
            partitions, OffsetSpec.LATEST, timeout_ms)

    def beginning_or_end_offset(self, partitions, timestamp, timeout_ms=None):
        """Fetch offset for each partition using ``timestamp``.

        Blocks until offsets are obtained, a non-retriable exception is raised
        or ``timeout_ms`` passed.

        Arguments:
            partitions ([TopicPartition]): List of partitions for list offsets.
            timestamp (int or OffsetSpec): OffsetSpec.LATEST (-1) for the latest
                available, OffsetSpec.EARLIEST (-2) for the earliest available.
                Otherwise timestamp is treated as epoch milliseconds.
            timeout_ms (int, optional): The maximum time in milliseconds to block.

        Returns:
            {TopicPartition: int}: Mapping of partition to retrieved offset.

        Raises:
            UnsupportedVersionError if broker does not support any compatible
                ListOffsetsRequest api version.
            KafkaTimeoutError if timeout_ms provided.
        """
        timestamps = dict([(tp, timestamp) for tp in partitions])
        offsets = self._net.run(self._fetch_offsets_by_times_async, timestamps, timeout_ms)
        for tp in timestamps:
            offsets[tp] = offsets[tp].offset
        return offsets

    def fetched_records(self, max_records=None, update_offsets=True):
        """Returns previously fetched records and updates consumed offsets.

        Arguments:
            max_records (int): Maximum number of records returned. Defaults
                to max_poll_records configuration.

        Raises:
            OffsetOutOfRangeError: if no subscription offset_reset_strategy
            CorruptRecordError: if message crc validation fails (check_crcs
                must be set to True)
            RecordTooLargeError: if a message is larger than the currently
                configured max_partition_fetch_bytes
            TopicAuthorizationError: if consumer is not authorized to fetch
                messages from the topic
            ValueError: if max_records is <= 0

        Returns: (records (dict), partial (bool))
            records: {TopicPartition: [messages]}
            partial: True if records returned did not fully drain any pending
                partition requests. This may be useful for choosing when to
                pipeline additional fetch requests.
        """
        if max_records is None:
            max_records = self.config['max_poll_records']
        if max_records <= 0:
            raise ValueError('max_records must be > 0')

        if self._next_in_line_exception_metadata is not None:
            exc_meta = self._next_in_line_exception_metadata
            self._next_in_line_exception_metadata = None
            tp = exc_meta.partition
            if self._subscriptions.is_fetchable(tp) and self._subscriptions.position(tp).offset == exc_meta.fetched_offset:
                raise exc_meta.exception

        drained = collections.defaultdict(list)
        records_remaining = max_records
        # Needed to construct ExceptionMetadata if any exception is found when processing completed_fetch
        fetched_partition = None
        fetched_offset = -1

        # KAFKA-7548: restore parked data for any partition that the user
        # has since resumed. Raw completions go back into the fetch queue;
        # parsed records take the in-line slot when free, otherwise stay
        # parked and get picked up on a subsequent call.
        for tp in list(self._paused_completed_fetches):
            if not self._subscriptions.is_paused(tp):
                self._completed_fetches.append(self._paused_completed_fetches.pop(tp))
        if self._next_partition_records is None:
            for tp in list(self._paused_partition_records):
                if not self._subscriptions.is_paused(tp):
                    self._next_partition_records = self._paused_partition_records.pop(tp)
                    break

        try:
            while records_remaining > 0:
                if not self._next_partition_records:
                    if not self._completed_fetches:
                        break
                    completion = self._completed_fetches.popleft()
                    if self._subscriptions.is_paused(completion.topic_partition):
                        self._paused_completed_fetches[completion.topic_partition] = completion
                        continue
                    fetched_partition = completion.topic_partition
                    fetched_offset = completion.fetched_offset
                    self._next_partition_records = self._parse_fetched_data(completion)
                else:
                    tp = self._next_partition_records.topic_partition
                    if self._subscriptions.is_paused(tp):
                        self._paused_partition_records[tp] = self._next_partition_records
                        self._next_partition_records = None
                        continue
                    fetched_partition = tp
                    fetched_offset = self._next_partition_records.next_fetch_offset
                    records_remaining -= self._append(drained,
                                                      self._next_partition_records,
                                                      records_remaining,
                                                      update_offsets)
        except Exception as e:
            if not drained:
                raise e
            # To be thrown in the next call of this method
            self._next_in_line_exception_metadata = ExceptionMetadata(fetched_partition, fetched_offset, e)
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
                    log.debug("Updating fetch position for assigned partition %s to %s (leader epoch %s)",
                              tp, part.next_fetch_offset, part.leader_epoch)
                    self._subscriptions.assignment[tp].position = OffsetAndMetadata(
                        part.next_fetch_offset, '', part.leader_epoch)
                return len(part_records)

            else:
                # these records aren't next in line based on the last consumed
                # position, ignore them they must be from an obsolete request
                log.debug("Ignoring fetched records for %s at offset %s since"
                          " the current position is %d", tp, part.next_fetch_offset,
                          position.offset)

        part.drain()
        return 0

    def _reset_offset_if_needed(self, partition, timestamp, offset):
        # we might lose the assignment while fetching the offset, or the user might seek to a different offset,
        # so verify it is still assigned and still in need of the requested reset
        if not self._subscriptions.is_assigned(partition):
            log.debug("Skipping reset of partition %s since it is no longer assigned", partition)
        elif not self._subscriptions.is_offset_reset_needed(partition):
            log.debug("Skipping reset of partition %s since reset is no longer needed", partition)
        elif timestamp and not timestamp == self._subscriptions.assignment[partition].reset_strategy:
            log.debug("Skipping reset of partition %s since an alternative reset has been requested", partition)
        else:
            log.info("Resetting offset for partition %s to offset %s.", partition, offset)
            self._subscriptions.seek(partition, offset)

    async def _reset_offsets_async(self, timeout_ms=None):
        """Drive resets to completion or until the timer expires.

        Each iteration fans out per-node ListOffsets requests concurrently
        and awaits all of them. After a retriable failure (NotLeader, etc.)
        a partition's next_allowed_retry_time is set ``retry_backoff_ms`` in
        the future; the loop sleeps until that time and retries rather than
        relying on an external caller to redrive. If all partitions have
        unknown leaders, awaits a metadata refresh and retries within the
        remaining budget.

        Arguments:
            timeout_ms (int, optional): Hard upper bound on the loop's
                wall-clock. None falls back to ``request_timeout_ms`` so a
                deleted-topic / permanently-unknown-leader partition can't
                spin the loop forever. The metadata-refresh wait inside
                the loop is capped by ``min(remaining_timer, request_timeout_ms)``.

        Per-node failures are caught inside _reset_offsets_for_node and
        stuffed into self._cached_list_offsets_exception; the next call to
        reset_offsets_if_needed surfaces them.
        """
        if timeout_ms is None:
            timeout_ms = self.config['request_timeout_ms']
        timer = Timer(timeout_ms)
        while not timer.expired:
            if self._cached_list_offsets_exception is not None:
                return
            partitions = self._subscriptions.partitions_needing_reset()
            if not partitions:
                next_retry = self._subscriptions.next_offset_reset_retry_time()
                if next_retry is None:
                    return
                delay = max(0.0, next_retry - time.monotonic())
                if timer.timeout_ms is not None:
                    delay = min(delay, timer.timeout_ms / 1000)
                if delay > 0:
                    await self._manager._net.sleep(delay)
                continue

            offset_resets = {}
            for tp in partitions:
                ts = self._subscriptions.assignment[tp].reset_strategy
                if ts:
                    offset_resets[tp] = ts
            if not offset_resets:
                return

            timestamps_by_node = self._group_list_offset_requests(offset_resets)
            if not timestamps_by_node:
                # All requested partitions have unknown / unavailable leaders.
                # _group_list_offset_requests has already requested a metadata
                # refresh; await it within the remaining budget (capped at
                # request_timeout_ms for any single broker round-trip).
                metadata_update = self._manager.cluster.request_update()
                wait_ms = self.config['request_timeout_ms']
                if timer.timeout_ms is not None:
                    wait_ms = min(wait_ms, timer.timeout_ms)
                try:
                    await self._manager.wait_for(metadata_update, wait_ms)
                except Errors.KafkaTimeoutError:
                    pass
                continue

            log.debug('Resetting offsets for %s', set(offset_resets.keys()))
            # Gather: schedule all per-node tasks concurrently, then await.
            node_tasks = []
            for node_id, t_and_e in timestamps_by_node.items():
                node_partitions = set(t_and_e.keys())
                expire_at = time.monotonic() + self.config['request_timeout_ms'] / 1000
                self._subscriptions.set_reset_pending(node_partitions, expire_at)
                node_tasks.append(self._manager.call_soon(
                    self._reset_offsets_for_node, node_id, t_and_e, node_partitions))
            for task in node_tasks:
                await task

    async def _reset_offsets_for_node(self, node_id, timestamps_and_epochs, partitions):
        try:
            fetched_offsets, partitions_to_retry = await self._send_list_offsets_request(node_id, timestamps_and_epochs)
        except Exception as error:
            self._subscriptions.reset_failed(partitions, time.monotonic() + self.config['retry_backoff_ms'] / 1000)
            self._manager.cluster.request_update()
            if not isinstance(error, Errors.RetriableError):
                if not self._cached_list_offsets_exception:
                    self._cached_list_offsets_exception = error
                else:
                    log.error("Discarding error in ListOffsetResponse because another error is pending: %s", error)
            return

        if partitions_to_retry:
            self._subscriptions.reset_failed(partitions_to_retry, time.monotonic() + self.config['retry_backoff_ms'] / 1000)
            self._manager.cluster.request_update()
        for partition, offset in fetched_offsets.items():
            ts, _epoch = timestamps_and_epochs[partition]
            self._reset_offset_if_needed(partition, ts, offset.offset)

    async def _send_list_offsets_requests(self, timestamps):
        """Fetch offsets for each partition in timestamps dict. This may send
        request to multiple nodes, based on who is Leader for partition.

        Per-node requests are dispatched concurrently; if any fails, the first
        exception encountered propagates and the remaining results are dropped.

        Arguments:
            timestamps (dict): {TopicPartition: int} mapping of fetching
                timestamps.

        Returns:
            (fetched_offsets, partitions_to_retry):
                dict[TopicPartition, OffsetAndTimestamp],
                set[TopicPartition]

        Raises:
            StaleMetadata: if no node has known leader for any partition.
        """
        timestamps_by_node = self._group_list_offset_requests(timestamps)
        if not timestamps_by_node:
            raise Errors.StaleMetadata()

        futures = [
            self._manager.call_soon(self._send_list_offsets_request, node_id, ts)
            for node_id, ts in timestamps_by_node.items()
        ]

        fetched_offsets = dict()
        partitions_to_retry = set()
        for f in futures:
            offs, retry = await f
            fetched_offsets.update(offs)
            partitions_to_retry.update(retry)
        return fetched_offsets, partitions_to_retry

    def _group_list_offset_requests(self, timestamps):
        timestamps_by_node = collections.defaultdict(dict)
        for partition, timestamp in timestamps.items():
            node_id = self._manager.cluster.leader_for_partition(partition)
            if node_id is None:
                self._manager.cluster.add_topic(partition.topic)
                log.debug("Partition %s is unknown for fetching offset", partition)
                self._manager.cluster.request_update()
            elif node_id == -1:
                log.debug("Leader for partition %s unavailable for fetching "
                          "offset, wait for metadata refresh", partition)
                self._manager.cluster.request_update()
            else:
                leader_epoch = -1
                timestamps_by_node[node_id][partition] = (timestamp, leader_epoch)
        return dict(timestamps_by_node)

    async def _send_list_offsets_request(self, node_id, timestamps_and_epochs):
        """Send single ListOffsetsResponse to node_id

        Returns:
            (fetched_offsets, partitions_to_retry):
                dict[TopicPartition, OffsetAndTimestamp],
                set[TopicPartition]

        Raises:
            TopicAuthorizationFailedError: if any topic returned an auth error
        """
        min_version = 1 if any(res[0] >= 0 for res in timestamps_and_epochs.values()) else 0
        min_version = max(min_version, ListOffsetsRequest.min_version_for_isolation_level(self._isolation_level))
        by_topic = collections.defaultdict(list)
        for tp, (timestamp, leader_epoch) in timestamps_and_epochs.items():
            data = _ListOffsetsPartition(
                partition_index=tp.partition,
                current_leader_epoch=leader_epoch,
                timestamp=timestamp)
            by_topic[tp.topic].append(data)

        request = ListOffsetsRequest(
            isolation_level=self._isolation_level,
            topics=list(by_topic.items()),
            min_version=min_version,
        )

        log.debug("Sending ListOffsetRequest %s to broker %s", request, node_id)
        response = await self._manager.send(request, node_id=node_id)
        return self._handle_list_offsets_response(response)

    def _handle_list_offsets_response(self, response):
        """Parse a ListOffsets response.

        Returns:
            (fetched_offsets, partitions_to_retry):
                dict[TopicPartition, OffsetAndTimestamp],
                set[TopicPartition]

        Raises:
            TopicAuthorizationFailedError: if any topic returned an auth error
            ValueError: if ListOffsetsResponse v0 and > 1 offset returned
        """
        fetched_offsets = dict()
        partitions_to_retry = set()
        unauthorized_topics = set()
        for topic_data in response.topics:
            for partition_info in topic_data.partitions:
                tp = TopicPartition(topic_data.name, partition_info.partition_index)
                error_code = partition_info.error_code
                error_type = Errors.for_code(error_code)
                if error_type is Errors.NoError:
                    if response.API_VERSION == 0:
                        offsets = partition_info.old_style_offsets
                        if len(offsets) > 1:
                            raise ValueError('Expected ListOffsetsResponse with one offset')
                        offset = offsets[0] if offsets else UNKNOWN_OFFSET
                    else:
                        offset = partition_info.offset
                    timestamp = partition_info.timestamp
                    leader_epoch = partition_info.leader_epoch
                    # DataContainer currently does not set default for
                    # out-of-version fields; so we need to handle explicitly
                    if timestamp is None:
                        timestamp = -1
                    if leader_epoch is None:
                        leader_epoch = -1
                    log.debug("Handling ListOffsetsResponse response for %s. "
                              "Fetched offset %s, timestamp %s, leader_epoch %s",
                              tp, offset, timestamp, leader_epoch)
                    if offset != UNKNOWN_OFFSET:
                        fetched_offsets[tp] = OffsetAndTimestamp(offset, timestamp, leader_epoch)
                elif error_type is Errors.UnsupportedForMessageFormatError:
                    # The message format on the broker side is before 0.10.0, which means it does not
                    # support timestamps. We treat this case the same as if we weren't able to find an
                    # offset corresponding to the requested timestamp and leave it out of the result.
                    log.debug("Cannot search by timestamp for partition %s because the"
                              " message format version is before 0.10.0", tp)
                elif error_type in (Errors.NotLeaderForPartitionError,
                                    Errors.ReplicaNotAvailableError,
                                    Errors.KafkaStorageError,
                                    Errors.OffsetNotAvailableError,
                                    Errors.LeaderNotAvailableError):
                    log.debug("Attempt to fetch offsets for partition %s failed due"
                              " to %s, retrying.", error_type.__name__, tp)
                    partitions_to_retry.add(tp)
                elif error_type is Errors.UnknownTopicOrPartitionError:
                    log.warning("Received unknown topic or partition error in ListOffsets "
                                "request for partition %s. The topic/partition " +
                                "may not exist or the user may not have Describe access "
                                "to it.", tp)
                    partitions_to_retry.add(tp)
                elif error_type is Errors.TopicAuthorizationFailedError:
                    unauthorized_topics.add(tp.topic)
                else:
                    log.warning("Attempt to fetch offsets for partition %s failed due to:"
                                " %s", tp, error_type.__name__)
                    partitions_to_retry.add(tp)
        if unauthorized_topics:
            raise Errors.TopicAuthorizationFailedError(unauthorized_topics)
        return fetched_offsets, partitions_to_retry

    # ------------------------------------------------------------------
    # KIP-320: offset validation via OffsetForLeaderEpoch
    # ------------------------------------------------------------------

    def maybe_validate_positions(self):
        """Walk assigned partitions; mark any whose cluster leader epoch has
        advanced beyond the position's epoch as awaiting validation.

        Cheap fire-and-forget marker; the actual RPC fan-out runs in
        ``validate_offsets_if_needed`` -> ``_validate_offsets_async``.
        Idempotent: partitions already awaiting validation, awaiting
        reset, or with no recorded epoch are skipped inside
        ``maybe_validate_position``.
        """
        for tp in self._subscriptions.assigned_partitions():
            current_epoch = self._manager.cluster.leader_epoch_for_partition(tp)
            self._subscriptions.maybe_validate_position_for_current_leader(tp, current_epoch)

    def validate_offsets_if_needed(self, timeout_ms=None):
        """Schedule any pending position validations and return the in-flight Task.

        Mirrors :meth:`reset_offsets_if_needed`: returns a cached Future
        shared across callers so concurrent ``consumer.poll`` and
        ``consumer.position`` callers don't race the same partition into
        duplicate OffsetForLeaderEpoch requests.

        Raises:
            LogTruncationError: if a previous validation detected truncation
                on one or more partitions. The exception is cleared after
                being raised so subsequent calls will re-attempt validation.
        """
        exc, self._cached_log_truncation = self._cached_log_truncation, None
        if exc:
            raise exc

        if self._validation_task is not None and not self._validation_task.is_done:
            return self._validation_task

        if not self._subscriptions.partitions_needing_validation():
            return None

        self._validation_task = self._manager.call_soon(
            self._validate_offsets_async, timeout_ms)
        return self._validation_task

    async def _validate_offsets_async(self, timeout_ms=None):
        """Drive offset validations to completion or until the timer expires.

        Same overall shape as ``_reset_offsets_async``: per-node fan-out.
        After a retriable failure (FencedLeaderEpoch, etc.) a partition's
        next_allowed_retry_time is set ``retry_backoff_ms`` in the future;
        the loop sleeps until that time and retries rather than relying on
        an external caller to redrive. Stops on first ``LogTruncationError``
        accumulation; the next caller surfaces it.
        """
        if timeout_ms is None:
            timeout_ms = self.config['request_timeout_ms']
        timer = Timer(timeout_ms)
        while not timer.expired:
            if self._cached_log_truncation is not None:
                return
            partitions = self._subscriptions.partitions_needing_validation()
            if not partitions:
                next_retry = self._subscriptions.next_offset_validation_retry_time()
                if next_retry is None:
                    return
                delay = max(0.0, next_retry - time.monotonic())
                if timer.timeout_ms is not None:
                    delay = min(delay, timer.timeout_ms / 1000)
                if delay > 0:
                    await self._manager._net.sleep(delay)
                continue

            positions = {}
            for tp in partitions:
                state = self._subscriptions.assignment[tp]
                if state.position is not None and state.position.leader_epoch >= 0:
                    positions[tp] = state.position
            if not positions:
                return

            requests_by_node = self._group_offset_for_leader_epoch_requests(positions)
            if not requests_by_node:
                metadata_update = self._manager.cluster.request_update()
                wait_ms = self.config['request_timeout_ms']
                if timer.timeout_ms is not None:
                    wait_ms = min(wait_ms, timer.timeout_ms)
                try:
                    await self._manager.wait_for(metadata_update, wait_ms)
                except Errors.KafkaTimeoutError:
                    pass
                continue

            log.debug('Validating offsets for %s', set(positions.keys()))
            node_tasks = []
            for node_id, payload in requests_by_node.items():
                node_partitions = set(payload.keys())
                expire_at = time.monotonic() + self.config['request_timeout_ms'] / 1000
                self._subscriptions.set_validation_pending(node_partitions, expire_at)
                node_tasks.append(self._manager.call_soon(
                    self._validate_offsets_for_node, node_id, payload))
            for task in node_tasks:
                await task

    async def _validate_offsets_for_node(self, node_id, partitions_to_positions):
        try:
            truncations = await self._send_offset_for_leader_epoch_request(
                node_id, partitions_to_positions)
        except Exception as error:
            self._subscriptions.validation_failed(
                set(partitions_to_positions),
                time.monotonic() + self.config['retry_backoff_ms'] / 1000)
            self._manager.cluster.request_update()
            if not isinstance(error, Errors.RetriableError):
                log.error("Non-retriable error from OffsetForLeaderEpoch on node %s: %s",
                          node_id, error)
            return

        if truncations:
            if self._cached_log_truncation is None:
                self._cached_log_truncation = Errors.LogTruncationError(truncations)
            else:
                self._cached_log_truncation.divergent_offsets.update(truncations)

    def _group_offset_for_leader_epoch_requests(self, positions):
        """Group {TopicPartition: OffsetAndMetadata} by leader node.

        Partitions whose leader is unknown trigger a metadata refresh and
        are dropped from this round. Partitions whose position lacks an
        epoch are also dropped - they can't be validated.
        """
        by_node = collections.defaultdict(dict)
        for tp, position in positions.items():
            if position.leader_epoch < 0:
                continue
            node_id = self._manager.cluster.leader_for_partition(tp)
            if node_id is None:
                self._manager.cluster.add_topic(tp.topic)
                self._manager.cluster.request_update()
            elif node_id == -1:
                self._manager.cluster.request_update()
            else:
                by_node[node_id][tp] = position
        return dict(by_node)

    async def _send_offset_for_leader_epoch_request(self, node_id, partitions_to_positions):
        """Send one OffsetForLeaderEpoch request and return any truncations.

        Returns:
            dict[TopicPartition, OffsetAndMetadata]: partitions whose log
            was truncated past their position. Successful validations
            update :class:`SubscriptionState` directly via
            ``complete_validation``; retriable per-partition errors leave
            ``next_allowed_retry_time`` set so the outer loop will retry.

        Raises:
            TopicAuthorizationFailedError: if any topic returned an auth error.
        """
        by_topic = collections.defaultdict(list)
        for tp, position in partitions_to_positions.items():
            current_leader_epoch = self._manager.cluster.leader_epoch_for_partition(tp)
            if current_leader_epoch is None or current_leader_epoch < 0:
                current_leader_epoch = -1
            by_topic[tp.topic].append(_OffsetForLeaderPartition(
                partition=tp.partition,
                current_leader_epoch=current_leader_epoch,
                leader_epoch=position.leader_epoch,
            ))

        request = OffsetForLeaderEpochRequest(
            replica_id=-1,
            topics=list(by_topic.items()),
        )

        log.debug("Sending OffsetForLeaderEpochRequest %s to broker %s", request, node_id)
        response = await self._manager.send(request, node_id=node_id)
        return self._handle_offset_for_leader_epoch_response(response, partitions_to_positions)

    def _handle_offset_for_leader_epoch_response(self, response, requested_positions):
        """Parse an OffsetForLeaderEpoch response.

        Side effects: calls ``complete_validation`` / ``validation_failed``
        / ``request_position_validation`` on the subscription state as
        appropriate for each partition's response code.

        Returns:
            dict[TopicPartition, OffsetAndMetadata]: subset of requested
            partitions where end_offset < requested position (truncation).
        """
        truncations = {}
        unauthorized_topics = set()
        retry_at = time.monotonic() + self.config['retry_backoff_ms'] / 1000
        retry = set()

        for topic_data in response.topics:
            for partition_info in topic_data.partitions:
                tp = TopicPartition(topic_data.topic, partition_info.partition)
                requested = requested_positions.get(tp)
                if requested is None:
                    continue
                error_type = Errors.for_code(partition_info.error_code)

                if error_type is Errors.NoError:
                    end_offset = partition_info.end_offset
                    end_epoch = partition_info.leader_epoch
                    if end_epoch is None:
                        end_epoch = -1
                    current = self._subscriptions.assignment[tp].position if \
                        self._subscriptions.is_assigned(tp) else None
                    # Position may have changed (seek, rebalance) since request
                    # was sent; skip stale completions.
                    if current is None or current != requested:
                        log.debug("Skipping validation completion for %s: position "
                                  "changed since request was sent", tp)
                        continue

                    has_reset_policy = self._subscriptions.has_default_offset_reset_policy()

                    if end_offset < 0 or end_epoch < 0:
                        # UNDEFINED_EPOCH / UNDEFINED_EPOCH_OFFSET: broker has
                        # no record of our requested epoch on this partition.
                        # Mirror Java SubscriptionState.maybeCompleteValidation:
                        # this is truncation with no known diverging offset.
                        if has_reset_policy:
                            log.info("Truncation detected for %s at position %s "
                                     "(broker returned UNDEFINED end_offset/leader_epoch); "
                                     "resetting offset per auto_offset_reset policy",
                                     tp, current.offset)
                            self._subscriptions.request_offset_reset(tp)
                        else:
                            log.warning("Truncation detected for %s at position %s "
                                        "(broker returned UNDEFINED end_offset/leader_epoch), "
                                        "but no reset policy is set", tp, current.offset)
                            truncations[tp] = None
                            self._subscriptions.complete_validation(tp)
                    elif end_offset < current.offset:
                        # Broker confirms the diverging point. Seek there
                        # directly instead of resetting via policy, so the
                        # consumer only re-reads records past the divergence
                        # (Java: state.seekValidated(newPosition)).
                        divergent = OffsetAndMetadata(end_offset, '', end_epoch)
                        if has_reset_policy:
                            log.info("Truncation detected for %s at position %s; "
                                     "seeking to first diverging offset %s",
                                     tp, current.offset, divergent)
                            self._subscriptions.seek(tp, divergent)
                        else:
                            log.warning("Truncation detected for %s at position %s "
                                        "(first diverging offset is %s), but no reset "
                                        "policy is set", tp, current.offset, divergent)
                            truncations[tp] = divergent
                            self._subscriptions.complete_validation(tp)
                    else:
                        validated = OffsetAndMetadata(
                            current.offset, current.metadata, end_epoch)
                        self._subscriptions.complete_validation(tp, validated)

                elif error_type in (Errors.FencedLeaderEpochError,
                                    Errors.UnknownLeaderEpochError,
                                    Errors.NotLeaderForPartitionError,
                                    Errors.ReplicaNotAvailableError,
                                    Errors.KafkaStorageError,
                                    Errors.LeaderNotAvailableError):
                    log.debug("OffsetForLeaderEpoch for %s returned retriable %s; "
                              "will retry after backoff", tp, error_type.__name__)
                    self._manager.cluster.request_update()
                    retry.add(tp)
                elif error_type is Errors.UnknownTopicOrPartitionError:
                    log.warning("OffsetForLeaderEpoch for %s: unknown topic/partition", tp)
                    retry.add(tp)
                elif error_type is Errors.TopicAuthorizationFailedError:
                    unauthorized_topics.add(tp.topic)
                else:
                    log.warning("OffsetForLeaderEpoch for %s failed with %s",
                                tp, error_type.__name__)
                    retry.add(tp)

        if retry:
            self._subscriptions.validation_failed(retry, retry_at)
        if unauthorized_topics:
            raise Errors.TopicAuthorizationFailedError(unauthorized_topics)
        return truncations

    def _fetchable_partitions(self):
        fetchable = self._subscriptions.fetchable_partitions()
        # do not fetch a partition if we have a pending fetch response to process
        # use copy to avoid runtimeerror on mutation from different thread
        discard = {fetch.topic_partition for fetch in self._completed_fetches.copy()}
        current = self._next_partition_records
        if current:
            discard.add(current.topic_partition)
        discard.update(self._paused_completed_fetches)
        discard.update(self._paused_partition_records)
        return [tp for tp in fetchable if tp not in discard]

    def _select_read_replica(self, tp):
        """Pick the node to fetch from for ``tp``: a cached preferred read
        replica (KIP-392) when valid and *still listed as a replica of
        ``tp``*, otherwise the partition leader. A preferred replica that
        has been demoted out of the partition's replica set (or fell out
        of cluster metadata entirely) is cleared so the next fetch goes
        to the leader.
        """
        preferred = self._subscriptions.assignment[tp].preferred_read_replica()
        if preferred is None:
            return self._manager.cluster.leader_for_partition(tp)
        if not self._manager.cluster.is_replica_node(tp, preferred):
            self._subscriptions.assignment[tp].clear_preferred_read_replica()
            leader = self._manager.cluster.leader_for_partition(tp)
            log.debug("Preferred read replica %s for partition %s no longer"
                      " online or no longer a replica; falling back to leader %s",
                      preferred, tp, leader)
            return leader
        return preferred

    def _create_fetch_requests(self):
        """Create fetch requests for all assigned partitions, grouped by node.

        FetchRequests skipped if no leader, or node has requests in flight

        Returns:
            dict: {node_id: (FetchRequest, {TopicPartition: fetch_offset}), ...}
        """
        # TODO:
        # v13 topic ids (KIP-516)
        # v14 tiered storage (KIP-405)
        # v15 replica state (KIP-903)
        # v16 node endpoints (KIP-951)
        # v17 directory id (KIP-853)
        max_version = 12
        fetchable = collections.defaultdict(collections.OrderedDict)
        for tp in self._fetchable_partitions():
            node_id = self._select_read_replica(tp)

            position = self._subscriptions.assignment[tp].position

            # fetch if there is a leader and no in-flight requests
            if node_id is None or node_id == -1:
                log.debug("No leader found for partition %s."
                          " Requesting metadata update", tp)
                self._manager.cluster.request_update()

            elif self._manager.connection_delay(node_id) > 0:
                # If we try to send during the reconnect backoff window, then the request is just
                # going to be failed anyway before being sent, so skip the send for now
                log.debug("Skipping fetch for partition %s because node %s is awaiting reconnect backoff",
                          tp, node_id)

            # TODO: handle throttle_delay in kafka.net
            elif self._client.throttle_delay(node_id) > 0:
                # If we try to send while throttled, then the request is just
                # going to be failed anyway before being sent, so skip the send for now
                log.debug("Skipping fetch for partition %s because node %s is throttled",
                          tp, node_id)

            elif node_id in self._nodes_with_pending_fetch_requests:
                log.debug("Skipping fetch for partition %s because there is a pending fetch request to node %s",
                          tp, node_id)

            else:
                # Leader is connected and does not have a pending fetch request.
                # current_leader_epoch (v9+) = metadata view (broker fencing);
                # last_fetched_epoch (v12+) = record view (broker divergence
                # detection). They differ once leadership advances past the
                # record at the fetch offset.
                current_leader_epoch = self._manager.cluster.leader_epoch_for_partition(tp)
                if current_leader_epoch is None:
                    current_leader_epoch = -1
                partition_info = _FetchPartition(
                    partition=tp.partition,
                    current_leader_epoch=current_leader_epoch,
                    fetch_offset=position.offset,
                    last_fetched_epoch=position.leader_epoch,
                    partition_max_bytes=self.config['max_partition_fetch_bytes']
                )
                fetchable[node_id][tp] = partition_info
                log.debug("Adding fetch request for partition %s at offset %d",
                          tp, position.offset)

        requests = {}
        for node_id, next_partitions in fetchable.items():
            if self._enable_incremental_fetch_sessions:
                if node_id not in self._session_handlers:
                    self._session_handlers[node_id] = FetchSessionHandler(node_id)
                session = self._session_handlers[node_id].build_next(next_partitions)
            else:
                # No incremental fetch support
                session = FetchRequestData(next_partitions, None, FetchMetadata.LEGACY)

            min_version = FetchRequest.min_version_for_isolation_level(self._isolation_level)
            request = FetchRequest(
                max_wait_ms=self.config['fetch_max_wait_ms'],
                min_bytes=self.config['fetch_min_bytes'],
                max_bytes=self.config['fetch_max_bytes'],
                isolation_level=self._isolation_level,
                session_id=session.id,
                session_epoch=session.epoch,
                topics=session.to_send,
                forgotten_topics_data=session.to_forget,
                rack_id=self.config['client_rack'],
                min_version=min_version,
                max_version=max_version,
            )

            fetch_offsets = {tp: next_partitions[tp].fetch_offset for tp in next_partitions}
            requests[node_id] = (request, fetch_offsets)

        return requests

    def _handle_fetch_response(self, node_id, fetch_offsets, send_time, response):
        """The callback for fetch completion"""
        if response.API_VERSION >= 7 and self._enable_incremental_fetch_sessions:
            if node_id not in self._session_handlers:
                log.error("Unable to find fetch session handler for node %s. Ignoring fetch response", node_id)
                return
            if not self._session_handlers[node_id].handle_response(response):
                return

        partitions = set([
            TopicPartition(
                topic_data.topic,
                partition_data.partition_index)
            for topic_data in response.responses
            for partition_data in topic_data.partitions
        ])
        if self._sensors:
            metric_aggregator = FetchResponseMetricAggregator(self._sensors, partitions)
        else:
            metric_aggregator = None

        for topic_data in response.responses:
            for partition_data in topic_data.partitions:
                tp = TopicPartition(
                    topic_data.topic,
                    partition_data.partition_index
                )
                fetch_offset = fetch_offsets[tp]
                completed_fetch = CompletedFetch(
                    tp, fetch_offset,
                    response.API_VERSION,
                    partition_data,
                    metric_aggregator
                )
                self._completed_fetches.append(completed_fetch)

        if self._sensors:
            self._sensors.fetch_latency.record((time.monotonic() - send_time) * 1000)

    def _handle_fetch_error(self, node_id, exception):
        level = logging.INFO if isinstance(exception, Errors.Cancelled) else logging.ERROR
        log.log(level, 'Fetch to node %s failed: %s', node_id, exception)
        if node_id in self._session_handlers:
            self._session_handlers[node_id].handle_error(exception)

    def _clear_pending_fetch_request(self, node_id, _):
        try:
            self._nodes_with_pending_fetch_requests.remove(node_id)
        except KeyError:
            pass

    def _maybe_update_current_leader(self, tp, partition_data):
        """Apply a KIP-951 ``current_leader`` hint from a Fetch v12+ response.

        Updates the cluster's cached leader id/epoch when the broker advertises
        a newer leader. If the new leader id is not yet a known broker (v12 has
        no ``node_endpoints``), requests a metadata refresh so the consumer
        learns its address.
        """
        leader = partition_data.current_leader
        if leader is None or leader.leader_epoch < 0:
            return
        if self._manager.cluster.update_partition_leader(
                tp, leader.leader_id, leader.leader_epoch):
            log.debug("Fetch response advertised new leader for %s: node %s epoch %s",
                      tp, leader.leader_id, leader.leader_epoch)
            if self._manager.cluster.broker_metadata(leader.leader_id) is None:
                self._manager.cluster.request_update()

    def _parse_fetched_data(self, completed_fetch):
        tp = completed_fetch.topic_partition
        fetch_offset = completed_fetch.fetched_offset
        error_code = completed_fetch.partition_data.error_code
        highwater = completed_fetch.partition_data.high_watermark
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

                # KIP-320 / Fetch v12+: the broker can tell us our last_fetched_epoch
                # diverges from its log. Route into the existing OffsetForLeaderEpoch
                # validation flow rather than truncating directly here; the
                # validation path surfaces LogTruncationError uniformly.
                diverging_epoch = completed_fetch.partition_data.diverging_epoch
                if diverging_epoch is not None and diverging_epoch.end_offset >= 0:
                    log.info("Fetch for %s diverged at epoch %s offset %s;"
                             " marking position for validation",
                             tp, diverging_epoch.epoch, diverging_epoch.end_offset)
                    self._subscriptions.request_position_validation(tp)
                    self._manager.cluster.request_update()
                    return None

                records = MemoryRecords(completed_fetch.partition_data.records)
                aborted_transactions = completed_fetch.partition_data.aborted_transactions
                log.debug("Preparing to read %s bytes of data for partition %s with offset %d",
                          records.size_in_bytes(), tp, fetch_offset)
                parsed_records = self.PartitionRecords(fetch_offset, tp, records,
                                                       key_deserializer=self.config['key_deserializer'],
                                                       value_deserializer=self.config['value_deserializer'],
                                                       check_crcs=self.config['check_crcs'],
                                                       isolation_level=self._isolation_level,
                                                       aborted_transactions=aborted_transactions,
                                                       metric_aggregator=completed_fetch.metric_aggregator,
                                                       on_drain=self._on_partition_records_drain)
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

                preferred_read_replica = completed_fetch.partition_data.preferred_read_replica
                if self._subscriptions.assignment[tp].update_preferred_read_replica(
                        preferred_read_replica,
                        time.monotonic() + self.config['metadata_max_age_ms'] / 1000.0):
                    if preferred_read_replica is None or preferred_read_replica < 0:
                        log.debug("Cleared preferred read replica for partition %s", tp)
                    else:
                        log.debug("Updating preferred read replica for partition %s to %s",
                                  tp, preferred_read_replica)

            elif error_type in (Errors.NotLeaderForPartitionError,
                                Errors.ReplicaNotAvailableError,
                                Errors.UnknownTopicOrPartitionError,
                                Errors.KafkaStorageError):
                log.debug("Error fetching partition %s: %s", tp, error_type.__name__)
                self._maybe_update_current_leader(tp, completed_fetch.partition_data)
                self._manager.cluster.request_update()
            elif error_type in (Errors.FencedLeaderEpochError,
                                Errors.UnknownLeaderEpochError):
                # KIP-320: the broker has a different view of the leader epoch
                # than we do; ask for metadata refresh and queue position
                # validation so we detect any truncation before continuing.
                # The cache is cleared by maybe_validate_position once the
                # cluster cache catches up with the new epoch.
                log.debug("Fetch for %s returned %s; marking position for validation",
                          tp, error_type.__name__)
                self._maybe_update_current_leader(tp, completed_fetch.partition_data)
                self._subscriptions.request_position_validation(tp)
                self._manager.cluster.request_update()
            elif error_type is Errors.OffsetOutOfRangeError:
                position = self._subscriptions.assignment[tp].position
                if position is None or position.offset != fetch_offset:
                    log.debug("Discarding stale fetch response for partition %s"
                              " since the fetched offset %d does not match the"
                              " current offset %d", tp, fetch_offset, position.offset)
                else:
                    # KIP-392: a follower may be lagging behind the leader's
                    # high watermark such that our leader-side position is
                    # legitimately out of *its* range. If we'd been fetching
                    # from a follower, drop the cache and retry against the
                    # leader BEFORE concluding the offset is really out of
                    # range. Only when there was no cached follower do we
                    # proceed to reset / raise. Matches Java's behavior.
                    cleared = self._subscriptions.assignment[tp].clear_preferred_read_replica()
                    if cleared is not None:
                        log.debug("Fetch offset %s out of range for %s on follower %s;"
                                  " retrying from leader", fetch_offset, tp, cleared)
                    elif self._subscriptions.has_default_offset_reset_policy():
                        log.info("Fetch offset %s is out of range for topic-partition %s",
                                 fetch_offset, tp)
                        self._subscriptions.request_offset_reset(tp)
                    else:
                        raise Errors.OffsetOutOfRangeError({tp: fetch_offset})

            elif error_type is Errors.TopicAuthorizationFailedError:
                log.warning("Not authorized to read from topic %s.", tp.topic)
                raise Errors.TopicAuthorizationFailedError(set([tp.topic]))
            elif issubclass(error_type, Errors.RetriableError):
                log.debug("Retriable error fetching partition %s: %s", tp, error_type())
                if issubclass(error_type, Errors.InvalidMetadataError):
                    self._manager.cluster.request_update()
            else:
                raise error_type('Unexpected error while fetching data')

        finally:
            if parsed_records is None and completed_fetch.metric_aggregator:
                completed_fetch.metric_aggregator.record(tp, 0, 0)

            if error_type is not Errors.NoError:
                # Rotate this partition to the back of the iteration
                # order so we don't keep slamming the broken partition
                # first on the next poll - healthier partitions get
                # processed while this one's backoff / metadata
                # refresh runs. Cheap LRU-style fairness across the
                # assignment.
                self._subscriptions.move_partition_to_end(tp)

        return parsed_records

    def _on_partition_records_drain(self, partition_records):
        # Rotate this partition to the back of the iteration order so
        # the next poll prioritizes partitions we haven't drained from
        # recently. (Topic-grouping in the outgoing FetchRequest is
        # done unconditionally by FetchRequestData.to_send via
        # defaultdict, so this is purely round-robin fairness across
        # partitions, not a serialization-efficiency thing.)
        if partition_records.bytes_read > 0:
            self._subscriptions.move_partition_to_end(partition_records.topic_partition)

    def close(self):
        if self._next_partition_records is not None:
            self._next_partition_records.drain()
        for parked in self._paused_partition_records.values():
            parked.drain()
        self._paused_partition_records.clear()
        self._paused_completed_fetches.clear()
        self._next_in_line_exception_metadata = None

    class PartitionRecords:
        def __init__(self, fetch_offset, tp, records,
                     key_deserializer=None, value_deserializer=None,
                     check_crcs=True,
                     isolation_level=IsolationLevel.READ_UNCOMMITTED,
                     aborted_transactions=None, # AbortedTransaction data from FetchResponse
                     metric_aggregator=None, on_drain=lambda x: None):
            self.fetch_offset = fetch_offset
            self.topic_partition = tp
            self.leader_epoch = -1
            self.next_fetch_offset = fetch_offset
            self.bytes_read = 0
            self.records_read = 0
            self.isolation_level = isolation_level
            self.aborted_producer_ids = set()
            self.aborted_transactions = collections.deque(
                sorted(aborted_transactions or [], key=lambda txn: txn.first_offset)
            )
            self.metric_aggregator = metric_aggregator
            self.check_crcs = check_crcs
            self.record_iterator = itertools.dropwhile(
                self._maybe_skip_record,
                self._unpack_records(tp, records, key_deserializer, value_deserializer))
            self.on_drain = on_drain
            self._next_inline_exception = None

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

        def drain(self):
            if self.record_iterator is not None:
                self.record_iterator = None
                self._next_inline_exception = None
                if self.metric_aggregator:
                    self.metric_aggregator.record(self.topic_partition, self.bytes_read, self.records_read)
                self.on_drain(self)

        def _maybe_raise_next_inline_exception(self):
            if self._next_inline_exception:
                exc, self._next_inline_exception = self._next_inline_exception, None
                raise exc

        def take(self, n=None):
            self._maybe_raise_next_inline_exception()
            records = []
            try:
                # Note that records.extend(iter) will extend partially when exception raised mid-stream
                records.extend(itertools.islice(self.record_iterator, 0, n))
            except Exception as e:
                if not records:
                    raise e
                # To be thrown in the next call of this method
                self._next_inline_exception = e
            return records

        def _unpack_records(self, tp, records, key_deserializer, value_deserializer):
            try:
                batch = records.next_batch()
                last_batch = None
                while batch is not None:
                    last_batch = batch

                    if self.check_crcs and not batch.validate_crc():
                        raise Errors.CorruptRecordError(
                                "Record batch for partition %s at offset %s failed crc check" % (
                                    self.topic_partition, batch.base_offset))


                    # Try DefaultsRecordBatch / message log format v2
                    # base_offset, last_offset_delta, aborted transactions, and control batches
                    if batch.magic == 2:
                        self.leader_epoch = batch.leader_epoch
                        if self.isolation_level == IsolationLevel.READ_COMMITTED and batch.has_producer_id():
                            # remove from the aborted transaction queue all aborted transactions which have begun
                            # before the current batch's last offset and add the associated producerIds to the
                            # aborted producer set
                            self._consume_aborted_transactions_up_to(batch.last_offset)

                            producer_id = batch.producer_id
                            if self._contains_abort_marker(batch):
                                try:
                                    self.aborted_producer_ids.remove(producer_id)
                                except KeyError:
                                    pass
                            elif self._is_batch_aborted(batch):
                                log.debug("Skipping aborted record batch from partition %s with producer_id %s and"
                                          " offsets %s to %s",
                                          self.topic_partition, producer_id, batch.base_offset, batch.last_offset)
                                self.next_fetch_offset = batch.next_offset
                                batch = records.next_batch()
                                continue

                        # Control batches have a single record indicating whether a transaction
                        # was aborted or committed. These are not returned to the consumer.
                        if batch.is_control_batch:
                            self.next_fetch_offset = batch.next_offset
                            batch = records.next_batch()
                            continue

                    for record in batch:
                        if self.check_crcs and not record.validate_crc():
                            raise Errors.CorruptRecordError(
                                    "Record for partition %s at offset %s failed crc check" % (
                                        self.topic_partition, record.offset))
                        key_size = len(record.key) if record.key is not None else -1
                        value_size = len(record.value) if record.value is not None else -1
                        key = self._deserialize(key_deserializer, tp.topic, record.headers, record.key)
                        value = self._deserialize(value_deserializer, tp.topic, record.headers, record.value)
                        headers = record.headers
                        header_size = sum(
                            len(h_key.encode("utf-8")) + (len(h_val) if h_val is not None else 0) for h_key, h_val in
                            headers) if headers else -1
                        self.records_read += 1
                        self.bytes_read += record.size_in_bytes
                        self.next_fetch_offset = record.offset + 1
                        yield ConsumerRecord(
                            tp.topic, tp.partition, self.leader_epoch, record.offset, record.timestamp,
                            record.timestamp_type, key, value, record.headers, record.checksum,
                            key_size, value_size, header_size)

                    batch = records.next_batch()
                else:
                    # Message format v2 preserves the last offset in a batch even if the last record is removed
                    # through compaction. By using the next offset computed from the last offset in the batch,
                    # we ensure that the offset of the next fetch will point to the next batch, which avoids
                    # unnecessary re-fetching of the same batch (in the worst case, the consumer could get stuck
                    # fetching the same batch repeatedly).
                    if last_batch and last_batch.magic == 2:
                        self.next_fetch_offset = last_batch.next_offset
                    self.drain()

            # If unpacking raises StopIteration, it is erroneously
            # caught by the generator. We want all exceptions to be raised
            # back to the user. See Issue 545
            except StopIteration:
                log.exception('StopIteration raised unpacking messageset')
                raise RuntimeError('StopIteration raised unpacking messageset')

        def _deserialize(self, deserializer, topic, headers, data):
            if deserializer is None:
                return data
            try:
                return deserializer.deserialize(topic, headers, data)
            except TypeError:
                global _LOGGED_DESERIALIZE_WARNING
                if not _LOGGED_DESERIALIZE_WARNING:
                    warnings.warn('deserializer does not implement deserialize(topic, headers, data)', category=DeprecationWarning)
                    LOGGED_DESERIALIZE_WARNING = True
                return deserializer.deserialize(topic, data)

        def _consume_aborted_transactions_up_to(self, offset):
            if not self.aborted_transactions:
                return

            while self.aborted_transactions and self.aborted_transactions[0].first_offset <= offset:
                self.aborted_producer_ids.add(self.aborted_transactions.popleft().producer_id)

        def _is_batch_aborted(self, batch):
            return batch.is_transactional and batch.producer_id in self.aborted_producer_ids

        def _contains_abort_marker(self, batch):
            if not batch.is_control_batch:
                return False
            record = next(batch)
            if not record:
                return False
            return record.abort


class FetchSessionHandler:
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
        return {TopicPartition(topic_data.topic, partition_data.partition_index)
                for topic_data in response.responses
                for partition_data in topic_data.partitions}


class FetchMetadata:
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


class FetchRequestData:
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
        # Return as list of _FetchTopic data objects
        # so it can be passed directly to encoder
        partition_data = collections.defaultdict(list)
        for tp, partition_info in self._to_send.items():
            partition_data[tp.topic].append(partition_info)
        return [
            _FetchTopic(topic=topic, partitions=partitions)
            for topic, partitions in partition_data.items()
        ]

    @property
    def to_forget(self):
        # Return as list of _ForgottenTopic data objects
        # so it can be passed directly to encoder
        partition_data = collections.defaultdict(list)
        for tp in self._to_forget:
            partition_data[tp.topic].append(tp.partition)
        return [
            _ForgottenTopic(topic=topic, partitions=partitions)
            for topic, partitions in partition_data.items()
        ]


class FetchMetrics:
    __slots__ = ('total_bytes', 'total_records')

    def __init__(self):
        self.total_bytes = 0
        self.total_records = 0


class FetchResponseMetricAggregator:
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
            for topic, metrics in self.topic_fetch_metrics.items():
                self.sensors.record_topic_fetch_metrics(topic, metrics.total_bytes, metrics.total_records)


class FetchManagerMetrics:
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
