from __future__ import absolute_import

import collections
import copy
import logging
import threading
import time

import kafka.errors as Errors
from kafka.producer.future import FutureRecordMetadata, FutureProduceResult
from kafka.record.memory_records import MemoryRecordsBuilder
from kafka.structs import TopicPartition


log = logging.getLogger(__name__)


class AtomicInteger(object):
    def __init__(self, val=0):
        self._lock = threading.Lock()
        self._val = val

    def increment(self):
        with self._lock:
            self._val += 1
            return self._val

    def decrement(self):
        with self._lock:
            self._val -= 1
            return self._val

    def get(self):
        return self._val


class ProducerBatch(object):
    def __init__(self, tp, records, now=None):
        self.max_record_size = 0
        now = time.time() if now is None else now
        self.created = now
        self.drained = None
        self.attempts = 0
        self.last_attempt = now
        self.last_append = now
        self.records = records
        self.topic_partition = tp
        self.produce_future = FutureProduceResult(tp)
        self._retry = False

    @property
    def record_count(self):
        return self.records.next_offset()

    @property
    def producer_id(self):
        return self.records.producer_id if self.records else None

    @property
    def producer_epoch(self):
        return self.records.producer_epoch if self.records else None

    @property
    def has_sequence(self):
        return self.records.has_sequence if self.records else False

    def try_append(self, timestamp_ms, key, value, headers, now=None):
        metadata = self.records.append(timestamp_ms, key, value, headers)
        if metadata is None:
            return None

        now = time.time() if now is None else now
        self.max_record_size = max(self.max_record_size, metadata.size)
        self.last_append = now
        future = FutureRecordMetadata(self.produce_future, metadata.offset,
                                      metadata.timestamp, metadata.crc,
                                      len(key) if key is not None else -1,
                                      len(value) if value is not None else -1,
                                      sum(len(h_key.encode("utf-8")) + len(h_val) for h_key, h_val in headers) if headers else -1)
        return future

    def done(self, base_offset=None, timestamp_ms=None, exception=None, log_start_offset=None):
        if self.produce_future.is_done:
            log.warning('Batch is already closed -- ignoring batch.done()')
            return
        elif exception is None:
            log.debug("Produced messages to topic-partition %s with base offset"
                      " %s log start offset %s.", self.topic_partition, base_offset,
                      log_start_offset)  # trace
            self.produce_future.success((base_offset, timestamp_ms, log_start_offset))
        else:
            log.warning("Failed to produce messages to topic-partition %s with base offset"
                        " %s log start offset %s and error %s.", self.topic_partition, base_offset,
                        log_start_offset, exception)  # trace
            self.produce_future.failure(exception)

    def maybe_expire(self, request_timeout_ms, retry_backoff_ms, linger_ms, is_full, now=None):
        """Expire batches if metadata is not available

        A batch whose metadata is not available should be expired if one
        of the following is true:

          * the batch is not in retry AND request timeout has elapsed after
            it is ready (full or linger.ms has reached).

          * the batch is in retry AND request timeout has elapsed after the
            backoff period ended.
        """
        now = time.time() if now is None else now
        since_append = now - self.last_append
        since_ready = now - (self.created + linger_ms / 1000.0)
        since_backoff = now - (self.last_attempt + retry_backoff_ms / 1000.0)
        timeout = request_timeout_ms / 1000.0

        error = None
        if not self.in_retry() and is_full and timeout < since_append:
            error = "%d seconds have passed since last append" % (since_append,)
        elif not self.in_retry() and timeout < since_ready:
            error = "%d seconds have passed since batch creation plus linger time" % (since_ready,)
        elif self.in_retry() and timeout < since_backoff:
            error = "%d seconds have passed since last attempt plus backoff time" % (since_backoff,)

        if error:
            self.records.close()
            self.done(base_offset=-1, exception=Errors.KafkaTimeoutError(
                "Batch for %s containing %s record(s) expired: %s" % (
                self.topic_partition, self.records.next_offset(), error)))
            return True
        return False

    def in_retry(self):
        return self._retry

    def set_retry(self):
        self._retry = True

    @property
    def is_done(self):
        return self.produce_future.is_done

    def __str__(self):
        return 'ProducerBatch(topic_partition=%s, record_count=%d)' % (
            self.topic_partition, self.records.next_offset())


class RecordAccumulator(object):
    """
    This class maintains a dequeue per TopicPartition that accumulates messages
    into MessageSets to be sent to the server.

    The accumulator attempts to bound memory use, and append calls will block
    when that memory is exhausted.

    Keyword Arguments:
        batch_size (int): Requests sent to brokers will contain multiple
            batches, one for each partition with data available to be sent.
            A small batch size will make batching less common and may reduce
            throughput (a batch size of zero will disable batching entirely).
            Default: 16384
        compression_attrs (int): The compression type for all data generated by
            the producer. Valid values are gzip(1), snappy(2), lz4(3), or
            none(0).
            Compression is of full batches of data, so the efficacy of batching
            will also impact the compression ratio (more batching means better
            compression). Default: None.
        linger_ms (int): An artificial delay time to add before declaring a
            record batch (that isn't full) ready for sending. This allows
            time for more records to arrive. Setting a non-zero linger_ms
            will trade off some latency for potentially better throughput
            due to more batching (and hence fewer, larger requests).
            Default: 0
        retry_backoff_ms (int): An artificial delay time to retry the
            produce request upon receiving an error. This avoids exhausting
            all retries in a short period of time. Default: 100
    """
    DEFAULT_CONFIG = {
        'batch_size': 16384,
        'compression_attrs': 0,
        'linger_ms': 0,
        'retry_backoff_ms': 100,
        'transaction_manager': None,
        'message_version': 0,
    }

    def __init__(self, **configs):
        self.config = copy.copy(self.DEFAULT_CONFIG)
        for key in self.config:
            if key in configs:
                self.config[key] = configs.pop(key)

        self._closed = False
        self._transaction_manager = self.config['transaction_manager']
        self._flushes_in_progress = AtomicInteger()
        self._appends_in_progress = AtomicInteger()
        self._batches = collections.defaultdict(collections.deque) # TopicPartition: [ProducerBatch]
        self._tp_locks = {None: threading.Lock()} # TopicPartition: Lock, plus a lock to add entries
        self._incomplete = IncompleteProducerBatches()
        # The following variables should only be accessed by the sender thread,
        # so we don't need to protect them w/ locking.
        self.muted = set()
        self._drain_index = 0

    def append(self, tp, timestamp_ms, key, value, headers):
        """Add a record to the accumulator, return the append result.

        The append result will contain the future metadata, and flag for
        whether the appended batch is full or a new batch is created

        Arguments:
            tp (TopicPartition): The topic/partition to which this record is
                being sent
            timestamp_ms (int): The timestamp of the record (epoch ms)
            key (bytes): The key for the record
            value (bytes): The value for the record
            headers (List[Tuple[str, bytes]]): The header fields for the record

        Returns:
            tuple: (future, batch_is_full, new_batch_created)
        """
        assert isinstance(tp, TopicPartition), 'not TopicPartition'
        assert not self._closed, 'RecordAccumulator is closed'
        # We keep track of the number of appending thread to make sure we do
        # not miss batches in abortIncompleteBatches().
        self._appends_in_progress.increment()
        try:
            if tp not in self._tp_locks:
                with self._tp_locks[None]:
                    if tp not in self._tp_locks:
                        self._tp_locks[tp] = threading.Lock()

            with self._tp_locks[tp]:
                # check if we have an in-progress batch
                dq = self._batches[tp]
                if dq:
                    last = dq[-1]
                    future = last.try_append(timestamp_ms, key, value, headers)
                    if future is not None:
                        batch_is_full = len(dq) > 1 or last.records.is_full()
                        return future, batch_is_full, False

            with self._tp_locks[tp]:
                # Need to check if producer is closed again after grabbing the
                # dequeue lock.
                assert not self._closed, 'RecordAccumulator is closed'

                if dq:
                    last = dq[-1]
                    future = last.try_append(timestamp_ms, key, value, headers)
                    if future is not None:
                        # Somebody else found us a batch, return the one we
                        # waited for! Hopefully this doesn't happen often...
                        batch_is_full = len(dq) > 1 or last.records.is_full()
                        return future, batch_is_full, False

                if self._transaction_manager and self.config['message_version'] < 2:
                    raise Errors.UnsupportedVersionError("Attempting to use idempotence with a broker which"
                                                         " does not support the required message format (v2)."
                                                         " The broker must be version 0.11 or later.")
                records = MemoryRecordsBuilder(
                    self.config['message_version'],
                    self.config['compression_attrs'],
                    self.config['batch_size']
                )

                batch = ProducerBatch(tp, records)
                future = batch.try_append(timestamp_ms, key, value, headers)
                if not future:
                    raise Exception()

                dq.append(batch)
                self._incomplete.add(batch)
                batch_is_full = len(dq) > 1 or batch.records.is_full()
                return future, batch_is_full, True
        finally:
            self._appends_in_progress.decrement()

    def abort_expired_batches(self, request_timeout_ms, cluster):
        """Abort the batches that have been sitting in RecordAccumulator for
        more than the configured request_timeout due to metadata being
        unavailable.

        Arguments:
            request_timeout_ms (int): milliseconds to timeout
            cluster (ClusterMetadata): current metadata for kafka cluster

        Returns:
            list of ProducerBatch that were expired
        """
        expired_batches = []
        to_remove = []
        count = 0
        for tp in list(self._batches.keys()):
            assert tp in self._tp_locks, 'TopicPartition not in locks dict'

            # We only check if the batch should be expired if the partition
            # does not have a batch in flight. This is to avoid the later
            # batches get expired when an earlier batch is still in progress.
            # This protection only takes effect when user sets
            # max.in.flight.request.per.connection=1. Otherwise the expiration
            # order is not guranteed.
            if tp in self.muted:
                continue

            with self._tp_locks[tp]:
                # iterate over the batches and expire them if they have stayed
                # in accumulator for more than request_timeout_ms
                dq = self._batches[tp]
                for batch in dq:
                    is_full = bool(bool(batch != dq[-1]) or batch.records.is_full())
                    # check if the batch is expired
                    if batch.maybe_expire(request_timeout_ms,
                                          self.config['retry_backoff_ms'],
                                          self.config['linger_ms'],
                                          is_full):
                        expired_batches.append(batch)
                        to_remove.append(batch)
                        count += 1
                        self.deallocate(batch)
                    else:
                        # Stop at the first batch that has not expired.
                        break

                # Python does not allow us to mutate the dq during iteration
                # Assuming expired batches are infrequent, this is better than
                # creating a new copy of the deque for iteration on every loop
                if to_remove:
                    for batch in to_remove:
                        dq.remove(batch)
                    to_remove = []

        if expired_batches:
            log.warning("Expired %d batches in accumulator", count) # trace

        return expired_batches

    def reenqueue(self, batch, now=None):
        """Re-enqueue the given record batch in the accumulator to retry."""
        now = time.time() if now is None else now
        batch.attempts += 1
        batch.last_attempt = now
        batch.last_append = now
        batch.set_retry()
        assert batch.topic_partition in self._tp_locks, 'TopicPartition not in locks dict'
        assert batch.topic_partition in self._batches, 'TopicPartition not in batches'
        dq = self._batches[batch.topic_partition]
        with self._tp_locks[batch.topic_partition]:
            dq.appendleft(batch)

    def ready(self, cluster, now=None):
        """
        Get a list of nodes whose partitions are ready to be sent, and the
        earliest time at which any non-sendable partition will be ready;
        Also return the flag for whether there are any unknown leaders for the
        accumulated partition batches.

        A destination node is ready to send if:

         * There is at least one partition that is not backing off its send
         * and those partitions are not muted (to prevent reordering if
           max_in_flight_requests_per_connection is set to 1)
         * and any of the following are true:

           * The record set is full
           * The record set has sat in the accumulator for at least linger_ms
             milliseconds
           * The accumulator is out of memory and threads are blocking waiting
             for data (in this case all partitions are immediately considered
             ready).
           * The accumulator has been closed

        Arguments:
            cluster (ClusterMetadata):

        Returns:
            tuple:
                ready_nodes (set): node_ids that have ready batches
                next_ready_check (float): secs until next ready after backoff
                unknown_leaders_exist (bool): True if metadata refresh needed
        """
        ready_nodes = set()
        next_ready_check = 9999999.99
        unknown_leaders_exist = False
        now = time.time() if now is None else now

        # several threads are accessing self._batches -- to simplify
        # concurrent access, we iterate over a snapshot of partitions
        # and lock each partition separately as needed
        partitions = list(self._batches.keys())
        for tp in partitions:
            leader = cluster.leader_for_partition(tp)
            if leader is None or leader == -1:
                unknown_leaders_exist = True
                continue
            elif leader in ready_nodes:
                continue
            elif tp in self.muted:
                continue

            with self._tp_locks[tp]:
                dq = self._batches[tp]
                if not dq:
                    continue
                batch = dq[0]
                retry_backoff = self.config['retry_backoff_ms'] / 1000.0
                linger = self.config['linger_ms'] / 1000.0
                backing_off = bool(batch.attempts > 0 and
                                   batch.last_attempt + retry_backoff > now)
                waited_time = now - batch.last_attempt
                time_to_wait = retry_backoff if backing_off else linger
                time_left = max(time_to_wait - waited_time, 0)
                full = bool(len(dq) > 1 or batch.records.is_full())
                expired = bool(waited_time >= time_to_wait)

                sendable = (full or expired or self._closed or
                            self._flush_in_progress())

                if sendable and not backing_off:
                    ready_nodes.add(leader)
                else:
                    # Note that this results in a conservative estimate since
                    # an un-sendable partition may have a leader that will
                    # later be found to have sendable data. However, this is
                    # good enough since we'll just wake up and then sleep again
                    # for the remaining time.
                    next_ready_check = min(time_left, next_ready_check)

        return ready_nodes, next_ready_check, unknown_leaders_exist

    def has_undrained(self):
        """Check whether there are any batches which haven't been drained"""
        for tp in list(self._batches.keys()):
            with self._tp_locks[tp]:
                dq = self._batches[tp]
                if len(dq):
                    return True
        return False

    def drain(self, cluster, nodes, max_size, now=None):
        """
        Drain all the data for the given nodes and collate them into a list of
        batches that will fit within the specified size on a per-node basis.
        This method attempts to avoid choosing the same topic-node repeatedly.

        Arguments:
            cluster (ClusterMetadata): The current cluster metadata
            nodes (list): list of node_ids to drain
            max_size (int): maximum number of bytes to drain

        Returns:
            dict: {node_id: list of ProducerBatch} with total size less than the
                requested max_size.
        """
        if not nodes:
            return {}

        now = time.time() if now is None else now
        batches = {}
        for node_id in nodes:
            size = 0
            partitions = list(cluster.partitions_for_broker(node_id))
            ready = []
            # to make starvation less likely this loop doesn't start at 0
            self._drain_index %= len(partitions)
            start = self._drain_index
            while True:
                tp = partitions[self._drain_index]
                if tp in self._batches and tp not in self.muted:
                    with self._tp_locks[tp]:
                        dq = self._batches[tp]
                        if dq:
                            first = dq[0]
                            backoff = (
                                bool(first.attempts > 0) and
                                bool(first.last_attempt +
                                     self.config['retry_backoff_ms'] / 1000.0
                                     > now)
                            )
                            # Only drain the batch if it is not during backoff
                            if not backoff:
                                if (size + first.records.size_in_bytes() > max_size
                                    and len(ready) > 0):
                                    # there is a rare case that a single batch
                                    # size is larger than the request size due
                                    # to compression; in this case we will
                                    # still eventually send this batch in a
                                    # single request
                                    break
                                else:
                                    producer_id_and_epoch = None
                                    if self._transaction_manager:
                                        producer_id_and_epoch = self._transaction_manager.producer_id_and_epoch
                                        if not producer_id_and_epoch.is_valid:
                                            # we cannot send the batch until we have refreshed the PID
                                            log.debug("Waiting to send ready batches because transaction producer id is not valid")
                                            break

                                    batch = dq.popleft()
                                    if producer_id_and_epoch and not batch.in_retry():
                                        # If the batch is in retry, then we should not change the pid and
                                        # sequence number, since this may introduce duplicates. In particular,
                                        # the previous attempt may actually have been accepted, and if we change
                                        # the pid and sequence here, this attempt will also be accepted, causing
                                        # a duplicate.
                                        sequence_number = self._transaction_manager.sequence_number(batch.topic_partition)
                                        log.debug("Dest: %s: %s producer_id=%s epoch=%s sequence=%s",
                                                  node_id, batch.topic_partition, producer_id_and_epoch.producer_id, producer_id_and_epoch.epoch,
                                                  sequence_number)
                                        batch.records.set_producer_state(
                                            producer_id_and_epoch.producer_id,
                                            producer_id_and_epoch.epoch,
                                            sequence_number,
                                            self._transaction_manager.is_transactional()
                                        )
                                    batch.records.close()
                                    size += batch.records.size_in_bytes()
                                    ready.append(batch)
                                    batch.drained = now

                self._drain_index += 1
                self._drain_index %= len(partitions)
                if start == self._drain_index:
                    break

            batches[node_id] = ready
        return batches

    def deallocate(self, batch):
        """Deallocate the record batch."""
        self._incomplete.remove(batch)

    def _flush_in_progress(self):
        """Are there any threads currently waiting on a flush?"""
        return self._flushes_in_progress.get() > 0

    def begin_flush(self):
        """
        Initiate the flushing of data from the accumulator...this makes all
        requests immediately ready
        """
        self._flushes_in_progress.increment()

    def await_flush_completion(self, timeout=None):
        """
        Mark all partitions as ready to send and block until the send is complete
        """
        try:
            for batch in self._incomplete.all():
                log.debug('Waiting on produce to %s',
                          batch.produce_future.topic_partition)
                if not batch.produce_future.wait(timeout=timeout):
                    raise Errors.KafkaTimeoutError('Timeout waiting for future')
                if not batch.produce_future.is_done:
                    raise Errors.UnknownError('Future not done')

                if batch.produce_future.failed():
                    log.warning(batch.produce_future.exception)
        finally:
            self._flushes_in_progress.decrement()

    @property
    def has_incomplete(self):
        return bool(self._incomplete)

    def abort_incomplete_batches(self):
        """
        This function is only called when sender is closed forcefully. It will fail all the
        incomplete batches and return.
        """
        # We need to keep aborting the incomplete batch until no thread is trying to append to
        # 1. Avoid losing batches.
        # 2. Free up memory in case appending threads are blocked on buffer full.
        # This is a tight loop but should be able to get through very quickly.
        error = Errors.IllegalStateError("Producer is closed forcefully.")
        while True:
            self._abort_batches(error)
            if not self._appends_in_progress.get():
                break
        # After this point, no thread will append any messages because they will see the close
        # flag set. We need to do the last abort after no thread was appending in case the there was a new
        # batch appended by the last appending thread.
        self._abort_batches(error)
        self._batches.clear()

    def _abort_batches(self, error):
        """Go through incomplete batches and abort them."""
        for batch in self._incomplete.all():
            tp = batch.topic_partition
            # Close the batch before aborting
            with self._tp_locks[tp]:
                batch.records.close()
                self._batches[tp].remove(batch)
            batch.done(exception=error)
            self.deallocate(batch)

    def abort_undrained_batches(self, error):
        for batch in self._incomplete.all():
            tp = batch.topic_partition
            with self._tp_locks[tp]:
                aborted = False
                if not batch.is_done:
                    aborted = True
                    batch.records.close()
                    self._batches[tp].remove(batch)
            if aborted:
                batch.done(exception=error)
                self.deallocate(batch)

    def close(self):
        """Close this accumulator and force all the record buffers to be drained."""
        self._closed = True


class IncompleteProducerBatches(object):
    """A threadsafe helper class to hold ProducerBatches that haven't been ack'd yet"""

    def __init__(self):
        self._incomplete = set()
        self._lock = threading.Lock()

    def add(self, batch):
        with self._lock:
            self._incomplete.add(batch)

    def remove(self, batch):
        with self._lock:
            try:
                self._incomplete.remove(batch)
            except KeyError:
                pass

    def all(self):
        with self._lock:
            return list(self._incomplete)

    def __bool__(self):
        return bool(self._incomplete)


    __nonzero__ = __bool__
