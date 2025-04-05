from __future__ import absolute_import, division

import abc 
import collections
import heapq
import logging
import threading

from kafka.vendor import six

try:
    # enum in stdlib as of py3.4
    from enum import IntEnum  # pylint: disable=import-error
except ImportError:
    # vendored backport module
    from kafka.vendor.enum34 import IntEnum

import kafka.errors as Errors
from kafka.protocol.add_partitions_to_txn import AddPartitionsToTxnRequest
from kafka.protocol.end_txn import EndTxnRequest
from kafka.protocol.find_coordinator import FindCoordinatorRequest
from kafka.protocol.init_producer_id import InitProducerIdRequest
from kafka.structs import TopicPartition


log = logging.getLogger(__name__)


NO_PRODUCER_ID = -1
NO_PRODUCER_EPOCH = -1
NO_SEQUENCE = -1


class ProducerIdAndEpoch(object):
    __slots__ = ('producer_id', 'epoch')

    def __init__(self, producer_id, epoch):
        self.producer_id = producer_id
        self.epoch = epoch

    @property
    def is_valid(self):
        return NO_PRODUCER_ID < self.producer_id

    def match(self, batch):
        return self.producer_id == batch.producer_id and self.epoch == batch.producer_epoch

    def __str__(self):
        return "ProducerIdAndEpoch(producer_id={}, epoch={})".format(self.producer_id, self.epoch)


class TransactionState(IntEnum):
    UNINITIALIZED = 0
    INITIALIZING = 1
    READY = 2
    IN_TRANSACTION = 3
    COMMITTING_TRANSACTION = 4
    ABORTING_TRANSACTION = 5
    ABORTABLE_ERROR = 6
    FATAL_ERROR = 7

    @classmethod
    def is_transition_valid(cls, source, target):
        if target == cls.INITIALIZING:
            return source == cls.UNINITIALIZED
        elif target == cls.READY:
            return source in (cls.INITIALIZING, cls.COMMITTING_TRANSACTION, cls.ABORTING_TRANSACTION)
        elif target == cls.IN_TRANSACTION:
            return source == cls.READY
        elif target == cls.COMMITTING_TRANSACTION:
            return source == cls.IN_TRANSACTION
        elif target == cls.ABORTING_TRANSACTION:
            return source in (cls.IN_TRANSACTION, cls.ABORTABLE_ERROR)
        elif target == cls.ABORTABLE_ERROR:
            return source in (cls.IN_TRANSACTION, cls.COMMITTING_TRANSACTION, cls.ABORTABLE_ERROR)
        elif target == cls.UNINITIALIZED:
            # Disallow transitions to UNITIALIZED
            return False
        elif target == cls.FATAL_ERROR:
            # We can transition to FATAL_ERROR unconditionally.
            # FATAL_ERROR is never a valid starting state for any transition. So the only option is to close the
            # producer or do purely non transactional requests.
            return True


class Priority(IntEnum):
    # We use the priority to determine the order in which requests need to be sent out. For instance, if we have
    # a pending FindCoordinator request, that must always go first. Next, If we need a producer id, that must go second.
    # The endTxn request must always go last.
    FIND_COORDINATOR = 0
    INIT_PRODUCER_ID = 1
    ADD_PARTITIONS_OR_OFFSETS = 2
    END_TXN = 3


class TransactionManager(object):
    """
    A class which maintains state for transactions. Also keeps the state necessary to ensure idempotent production.
    """
    NO_INFLIGHT_REQUEST_CORRELATION_ID = -1
    # The retry_backoff_ms is overridden to the following value if the first AddPartitions receives a
    # CONCURRENT_TRANSACTIONS error.
    ADD_PARTITIONS_RETRY_BACKOFF_MS = 20

    def __init__(self, transactional_id=None, transaction_timeout_ms=0, retry_backoff_ms=100, api_version=(0, 11), metadata=None):
        self._api_version = api_version
        self._metadata = metadata
        # Keep track of the in flight batches bound for a partition, ordered by sequence. This helps us to ensure that
        # we continue to order batches by the sequence numbers even when the responses come back out of order during
        # leader failover. We add a batch to the queue when it is drained, and remove it when the batch completes
        # (either successfully or through a fatal failure).
        # use heapq methods to push/pop from queues
        self._in_flight_batches_by_sequence = collections.defaultdict(list)
        self._in_flight_batches_sort_id = 0

        # The base sequence of the next batch bound for a given partition.
        self._next_sequence = collections.defaultdict(lambda: 0)
        # The sequence of the last record of the last ack'd batch from the given partition. When there are no
        # in flight requests for a partition, the self._last_acked_sequence(topicPartition) == nextSequence(topicPartition) - 1.
        self._last_acked_sequence = collections.defaultdict(lambda: -1)
        self.transactional_id = transactional_id
        self.transaction_timeout_ms = transaction_timeout_ms
        self._transaction_coordinator = None
        self._consumer_group_coordinator = None
        self._new_partitions_in_transaction = set()
        self._pending_partitions_in_transaction = set()
        self._partitions_in_transaction = set()

        self._current_state = TransactionState.UNINITIALIZED
        self._last_error = None
        self.producer_id_and_epoch = ProducerIdAndEpoch(NO_PRODUCER_ID, NO_PRODUCER_EPOCH)

        self._transaction_started = False

        self._pending_requests = [] # priority queue via heapq
        self._pending_requests_sort_id = 0
        self._in_flight_request_correlation_id = self.NO_INFLIGHT_REQUEST_CORRELATION_ID

        # If a batch bound for a partition expired locally after being sent at least once, the partition has is considered
        # to have an unresolved state. We keep track fo such partitions here, and cannot assign any more sequence numbers
        # for this partition until the unresolved state gets cleared. This may happen if other inflight batches returned
        # successfully (indicating that the expired batch actually made it to the broker). If we don't get any successful
        # responses for the partition once the inflight request count falls to zero, we reset the producer id and
        # consequently clear this data structure as well.
        self._partitions_with_unresolved_sequences = set()
        self._inflight_batches_by_sequence = dict()
        # We keep track of the last acknowledged offset on a per partition basis in order to disambiguate UnknownProducer
        # responses which are due to the retention period elapsing, and those which are due to actual lost data.
        self._last_acked_offset = collections.defaultdict(lambda: -1)

        # This is used by the TxnRequestHandlers to control how long to back off before a given request is retried.
        # For instance, this value is lowered by the AddPartitionsToTxnHandler when it receives a CONCURRENT_TRANSACTIONS
        # error for the first AddPartitionsRequest in a transaction.
        self.retry_backoff_ms = retry_backoff_ms
        self._lock = threading.Condition()

    def initialize_transactions(self):
        with self._lock:
            self._ensure_transactional()
            self._transition_to(TransactionState.INITIALIZING)
            self.set_producer_id_and_epoch(ProducerIdAndEpoch(NO_PRODUCER_ID, NO_PRODUCER_EPOCH))
            self._next_sequence.clear()
            handler = InitProducerIdHandler(self, self.transactional_id, self.transaction_timeout_ms)
            self._enqueue_request(handler)
            return handler.result

    def begin_transaction(self):
        with self._lock:
            self._ensure_transactional()
            self._maybe_fail_with_error()
            self._transition_to(TransactionState.IN_TRANSACTION)

    def begin_commit(self):
        with self._lock:
            self._ensure_transactional()
            self._maybe_fail_with_error()
            self._transition_to(TransactionState.COMMITTING_TRANSACTION)
            return self._begin_completing_transaction(True)

    def begin_abort(self):
        with self._lock:
            self._ensure_transactional()
            if self._current_state != TransactionState.ABORTABLE_ERROR:
                self._maybe_fail_with_error()
            self._transition_to(TransactionState.ABORTING_TRANSACTION)

            # We're aborting the transaction, so there should be no need to add new partitions
            self._new_partitions_in_transaction.clear()
            return self._begin_completing_transaction(False)

    def _begin_completing_transaction(self, committed):
        if self._new_partitions_in_transaction:
            self._enqueue_request(self._add_partitions_to_transaction_handler())
        handler = EndTxnHandler(self, self.transactional_id, self.producer_id_and_epoch.producer_id, self.producer_id_and_epoch.epoch, committed)
        self._enqueue_request(handler)
        return handler.result

    def maybe_add_partition_to_transaction(self, topic_partition):
        with self._lock:
            self._fail_if_not_ready_for_send()

            if self.is_partition_added(topic_partition) or self.is_partition_pending_add(topic_partition):
                return

            log.debug("Begin adding new partition %s to transaction", topic_partition)
            self._new_partitions_in_transaction.add(topic_partition)

    def _fail_if_not_ready_for_send(self):
        with self._lock:
            if self.has_error():
                raise Errors.KafkaError(
                        "Cannot perform send because at least one previous transactional or"
                        " idempotent request has failed with errors.", self._last_error)

            if self.is_transactional():
                if not self.has_producer_id():
                    raise Errors.IllegalStateError(
                            "Cannot perform a 'send' before completing a call to initTransactions"
                            " when transactions are enabled.")

                if self._current_state != TransactionState.IN_TRANSACTION:
                    raise Errors.IllegalStateError("Cannot call send in state %s" % (self._current_state.name,))

    def is_send_to_partition_allowed(self, tp):
        with self._lock:
            if self.has_fatal_error():
                return False
            return not self.is_transactional() or tp in self._partitions_in_transaction

    def has_producer_id(self, producer_id=None):
        if producer_id is None:
            return self.producer_id_and_epoch.is_valid
        else:
            return self.producer_id_and_epoch.producer_id == producer_id

    def is_transactional(self):
        return self.transactional_id is not None

    def has_partitions_to_add(self):
        with self._lock:
            return bool(self._new_partitions_in_transaction) or bool(self._pending_partitions_in_transaction)

    def is_completing(self):
        with self._lock:
            return self._current_state in (
                TransactionState.COMMITTING_TRANSACTION,
                TransactionState.ABORTING_TRANSACTION)

    @property
    def last_error(self):
        return self._last_error

    def has_error(self):
        with self._lock:
            return self._current_state in (
                TransactionState.ABORTABLE_ERROR,
                TransactionState.FATAL_ERROR)

    def is_aborting(self):
        with self._lock:
            return self._current_state == TransactionState.ABORTING_TRANSACTION

    def transition_to_abortable_error(self, exc):
        with self._lock:
            if self._current_state == TransactionState.ABORTING_TRANSACTION:
                log.debug("Skipping transition to abortable error state since the transaction is already being "
                          " aborted. Underlying exception: ", exc)
                return
            self._transition_to(TransactionState.ABORTABLE_ERROR, error=exc)

    def transition_to_fatal_error(self, exc):
        with self._lock:
            self._transition_to(TransactionState.FATAL_ERROR, error=exc)

    # visible for testing
    def is_partition_added(self, partition):
        with self._lock:
            return partition in self._partitions_in_transaction

    # visible for testing
    def is_partition_pending_add(self, partition):
        return partition in self._new_partitions_in_transaction or partition in self._pending_partitions_in_transaction

    def has_producer_id_and_epoch(self, producer_id, producer_epoch):
        return (
            self.producer_id_and_epoch.producer_id == producer_id and
            self.producer_id_and_epoch.epoch == producer_epoch
        )

    def set_producer_id_and_epoch(self, producer_id_and_epoch):
        if not isinstance(producer_id_and_epoch, ProducerIdAndEpoch):
            raise TypeError("ProducerAndIdEpoch type required")
        log.info("ProducerId set to %s with epoch %s",
                 producer_id_and_epoch.producer_id, producer_id_and_epoch.epoch)
        self.producer_id_and_epoch = producer_id_and_epoch

    def reset_producer_id(self):
        """
        This method is used when the producer needs to reset its internal state because of an irrecoverable exception
        from the broker.

        We need to reset the producer id and associated state when we have sent a batch to the broker, but we either get
        a non-retriable exception or we run out of retries, or the batch expired in the producer queue after it was already
        sent to the broker.

        In all of these cases, we don't know whether batch was actually committed on the broker, and hence whether the
        sequence number was actually updated. If we don't reset the producer state, we risk the chance that all future
        messages will return an OutOfOrderSequenceNumberError.

        Note that we can't reset the producer state for the transactional producer as this would mean bumping the epoch
        for the same producer id. This might involve aborting the ongoing transaction during the initProducerIdRequest,
        and the user would not have any way of knowing this happened. So for the transactional producer,
        it's best to return the produce error to the user and let them abort the transaction and close the producer explicitly.
        """
        with self._lock:
            if self.is_transactional:
                raise Errors.IllegalStateError( 
                    "Cannot reset producer state for a transactional producer."
                    " You must either abort the ongoing transaction or"
                    " reinitialize the transactional producer instead")
            self.set_producer_id_and_epoch(ProducerIdAndEpoch(NO_PRODUCER_ID, NO_PRODUCER_EPOCH))
            self._next_sequence.clear()
            self._last_acked_sequence.clear()
            self._inflight_batches_by_sequence.clear()
            self._partitions_with_unresolved_sequences.clear()
            self._last_acked_offset.clear()

    def sequence_number(self, tp):
        with self._lock:
            return self._next_sequence[tp]

    def increment_sequence_number(self, tp, increment):
        with self._lock:
            if tp not in self._next_sequence:
                raise Errors.IllegalStateError("Attempt to increment sequence number for a partition with no current sequence.")
            # Sequence number wraps at java max int
            base = self._next_sequence[tp]
            if base > (2147483647 - increment):
              self._next_sequence[tp] = increment - (2147483647 - base) - 1
            else:
                self._next_sequence[tp] += increment

    def _next_in_flight_batches_sort_id(self):
        self._in_flight_batches_sort_id += 1
        return self._in_flight_batches_sort_id

    def add_in_flight_batch(self, batch):
        with self._lock:
            if not batch.has_sequence():
                raise Errors.IllegalStateError("Can't track batch for partition %s when sequence is not set." % (batch.topic_partition,))
            heapq.heappush(
                self._in_flight_batches_by_sequence[batch.topic_partition],
                (batch.base_sequence, self._next_in_flight_batches_sort_id(), batch)
            )

    def first_in_flight_sequence(self, tp):
        """
        Returns the first inflight sequence for a given partition. This is the base sequence of an inflight batch with
        the lowest sequence number.  If there are no inflight requests being tracked for this partition, this method will return -1
        """
        with self._lock:
            if not self._in_flight_batches_by_sequence[tp]:
                return NO_SEQUENCE
            else:
                return self._in_flight_batches_by_sequence[tp][0][2].base_sequence

    def next_batch_by_sequence(self, tp):
        with self._lock:
            if not self._in_flight_batches_by_sequence[tp]:
                return None
            else:
                return self._in_flight_batches_by_sequence[tp][0][2]

    def remove_in_flight_batch(self, batch):
        with self._lock:
            if not self._in_flight_batches_by_sequence[batch.topic_partition]:
                return
            else:
                try:
                    # see https://stackoverflow.com/questions/10162679/python-delete-element-from-heap
                    queue = self._in_flight_batches_by_sequence[batch.topic_partition]
                    idx = [item[2] for item in queue].index(batch)
                    queue[idx] = queue[-1]
                    queue.pop()
                    heapq.heapify(queue)
                except ValueError:
                    pass

    def maybe_update_last_acked_sequence(self, tp, sequence):
        with self._lock:
            if sequence > self._last_acked_sequence[tp]:
                self._last_acked_sequence[tp] = sequence

    def update_last_acked_offset(self, base_offset, batch):
        if base_offset == -1:
            return
        last_offset = base_offset + batch.record_count - 1
        if last_offset > self._last_acked_offset[batch.topic_partition]:
            self._last_acked_offset[batch.topic_partition] = last_offset
        else:
            log.debug("Partition %s keeps last_offset at %s", batch.topic_partition, last_offset)

    def adjust_sequences_due_to_failed_batch(self, batch):
        # If a batch is failed fatally, the sequence numbers for future batches bound for the partition must be adjusted
        # so that they don't fail with the OutOfOrderSequenceNumberError.
        #
        # This method must only be called when we know that the batch is question has been unequivocally failed by the broker,
        # ie. it has received a confirmed fatal status code like 'Message Too Large' or something similar.
        with self._lock:
            if batch.topic_partition not in self._next_sequence:
                # Sequence numbers are not being tracked for this partition. This could happen if the producer id was just
                # reset due to a previous OutOfOrderSequenceNumberError.
                return
            log.debug("producer_id: %s, send to partition %s failed fatally. Reducing future sequence numbers by %s",
                      batch.producer_id, batch.topic_partition, batch.record_count)
            current_sequence = self.sequence_number(batch.topic_partition)
            current_sequence -= batch.record_count
            if current_sequence < 0:
                raise Errors.IllegalStateError(
                    "Sequence number for partition %s is going to become negative: %s" % (batch.topic_partition, current_sequence))

            self._set_next_sequence(batch.topic_partition, current_sequence)

            for in_flight_batch in self._in_flight_batches_by_sequence[batch.topic_partition]:
                if in_flight_batch.base_sequence < batch.base_sequence:
                    continue
                new_sequence = in_flight_batch.base_sequence - batch.record_count
                if new_sequence < 0:
                    raise Errors.IllegalStateError(
                        "Sequence number for batch with sequence %s for partition %s is going to become negative: %s" % (
                            in_flight_batch.base_sequence, batch.topic_partition, new_sequence))

                log.info("Resetting sequence number of batch with current sequence %s for partition %s to %s",
                         in_flight_batch.base_sequence(), batch.topic_partition, new_sequence)
                in_flight_batch.reset_producer_state(
                    ProducerIdAndEpoch(in_flight_batch.producer_id, in_flight_batch.producer_epoch),
                    new_sequence,
                    in_flight_batch.is_transactional())

    def _start_sequences_at_beginning(self, tp):
        with self._lock:
            sequence = 0
            for in_flight_batch in self._in_flight_batches_by_sequence[tp]:
                log.info("Resetting sequence number of batch with current sequence %s for partition %s to %s",
                        in_flight_batch.base_sequence, in_flight_batch.topic_partition, sequence)
                in_flight_batch.reset_producer_state(
                    ProducerIdAndEpoch(in_flight_batch.producer_id, in_flight_batch.producer_epoch),
                    sequence,
                    in_flight_batch.is_transactional())
                sequence += in_flight_batch.record_count
            self._set_next_sequence(tp, sequence)
            try:
                del self._last_acked_sequence[tp]
            except KeyError:
                pass

    def has_in_flight_batches(self, tp):
        with self._lock:
            return len(self._in_flight_batches_by_sequence[tp]) > 0

    def has_unresolved_sequences(self):
        with self._lock:
            return len(self._partitions_with_unresolved_sequences) > 0

    def has_unresolved_sequence(self, tp):
        with self._lock:
            return tp in self._partitions_with_unresolved_sequences

    def mark_sequence_unresolved(self, tp):
        with self._lock:
            log.debug("Marking partition %s unresolved", tp)
            self._partitions_with_unresolved_sequences.add(tp)

    # Checks if there are any partitions with unresolved partitions which may now be resolved. Returns True if
    # the producer id needs a reset, False otherwise.
    def should_reset_producer_state_after_resolving_sequences(self):
        with self._lock:
            try:
                remove = set()
                if self.is_transactional():
                    # We should not reset producer state if we are transactional. We will transition to a fatal error instead.
                    return False
                for tp in self._partitions_with_unresolved_sequences:
                    if not self.has_in_flight_batches(tp):
                        # The partition has been fully drained. At this point, the last ack'd sequence should be once less than
                        # next sequence destined for the partition. If so, the partition is fully resolved. If not, we should
                        # reset the sequence number if necessary.
                        if self.is_next_sequence(tp, self.sequence_number(tp)):
                            # This would happen when a batch was expired, but subsequent batches succeeded.
                            remove.add(tp)
                        else:
                            # We would enter this branch if all in flight batches were ultimately expired in the producer.
                            log.info("No inflight batches remaining for %s, last ack'd sequence for partition is %s, next sequence is %s."
                                     " Going to reset producer state.", tp, self._last_acked_sequence(tp), self.sequence_number(tp))
                            return True
                return False
            finally:
                self._partitions_with_unresolved_sequences -= remove

    def is_next_sequence(self, tp, sequence):
        with self._lock:
            return sequence - self._last_acked_sequence(tp) == 1

    def _set_next_sequence(self, tp, sequence):
        with self._lock:
            if tp not in self._next_sequence and sequence != 0:
                raise Errors.IllegalStateError(
                    "Trying to set the sequence number for %s to %s but the sequence number was never set for this partition." % (
                        tp, sequence))
            self._next_sequence[tp] = sequence

    def next_request_handler(self, has_incomplete_batches):
        with self._lock:
            if self._new_partitions_in_transaction:
                self._enqueue_request(self._add_partitions_to_transaction_handler())

            if not self._pending_requests:
                return None

            _, _, next_request_handler = self._pending_requests[0]
            # Do not send the EndTxn until all batches have been flushed
            if isinstance(next_request_handler, EndTxnHandler) and has_incomplete_batches:
                return None

            heapq.heappop(self._pending_requests)
            if self._maybe_terminate_request_with_error(next_request_handler):
                log.debug("Not sending transactional request %s because we are in an error state",
                          next_request_handler.request)
                return None

            if isinstance(next_request_handler, EndTxnHandler) and not self._transaction_started:
                next_request_handler.result.done()
                if self._current_state != TransactionState.FATAL_ERROR:
                    log.debug("Not sending EndTxn for completed transaction since no partitions"
                              " or offsets were successfully added")
                    self._complete_transaction()
                try:
                    _, _, next_request_handler = heapq.heappop(self._pending_requests)
                except IndexError:
                    next_request_handler = None

            if next_request_handler:
                log.debug("Request %s dequeued for sending", next_request_handler.request)

            return next_request_handler

    def retry(self, request):
        with self._lock:
            request.set_retry()
            self._enqueue_request(request)

    def authentication_failed(self, exc):
        with self._lock:
            for _, _, request in self._pending_requests:
                request.fatal_error(exc)

    def coordinator(self, coord_type):
        if coord_type == 'group':
            return self._consumer_group_coordinator
        elif coord_type == 'transaction':
            return self._transaction_coordinator
        else:
            raise Errors.IllegalStateError("Received an invalid coordinator type: %s" % (coord_type,))

    def lookup_coordinator_for_request(self, request):
        self._lookup_coordinator(request.coordinator_type, request.coordinator_key)

    def next_in_flight_request_correlation_id(self):
        self._in_flight_request_correlation_id += 1
        return self._in_flight_request_correlation_id

    def clear_in_flight_transactional_request_correlation_id(self):
        self._in_flight_request_correlation_id = self.NO_INFLIGHT_REQUEST_CORRELATION_ID

    def has_in_flight_transactional_request(self):
        return self._in_flight_request_correlation_id != self.NO_INFLIGHT_REQUEST_CORRELATION_ID

    # visible for testing.
    def has_fatal_error(self):
        return self._current_state == TransactionState.FATAL_ERROR

    # visible for testing.
    def has_abortable_error(self):
        return self._current_state == TransactionState.ABORTABLE_ERROR

    # visible for testing
    def transactionContainsPartition(self, tp):
        with self._lock:
            return tp in self._partitions_in_transaction

    # visible for testing
    def has_ongoing_transaction(self):
        with self._lock:
            # transactions are considered ongoing once started until completion or a fatal error
            return self._current_state == TransactionState.IN_TRANSACTION or self.is_completing() or self.has_abortable_error()

    def can_retry(self, batch, error, log_start_offset):
        with self._lock:
            if not self.has_producer_id(batch.producer_id):
                return False

            elif (
                    error is Errors.OutOfOrderSequenceNumberError
                    and not self.has_unresolved_sequence(batch.topic_partition)
                    and (batch.sequence_has_been_reset() or not self.is_next_sequence(batch.topic_partition, batch.base_sequence))
                ):
                # We should retry the OutOfOrderSequenceNumberError if the batch is _not_ the next batch, ie. its base
                # sequence isn't the self._last_acked_sequence + 1. However, if the first in flight batch fails fatally, we will
                # adjust the sequences of the other inflight batches to account for the 'loss' of the sequence range in
                # the batch which failed. In this case, an inflight batch will have a base sequence which is
                # the self._last_acked_sequence + 1 after adjustment. When this batch fails with an OutOfOrderSequenceNumberError, we want to retry it.
                # To account for the latter case, we check whether the sequence has been reset since the last drain.
                # If it has, we will retry it anyway.
                return True

            elif error is Errors.UnknownProducerIdError:
                if log_start_offset == -1:
                    # We don't know the log start offset with this response. We should just retry the request until we get it.
                    # The UNKNOWN_PRODUCER_ID error code was added along with the new ProduceResponse which includes the
                    # logStartOffset. So the '-1' sentinel is not for backward compatibility. Instead, it is possible for
                    # a broker to not know the logStartOffset at when it is returning the response because the partition
                    # may have moved away from the broker from the time the error was initially raised to the time the
                    # response was being constructed. In these cases, we should just retry the request: we are guaranteed
                    # to eventually get a logStartOffset once things settle down.
                    return True

                if batch.sequence_has_been_reset():
                    # When the first inflight batch fails due to the truncation case, then the sequences of all the other
                    # in flight batches would have been restarted from the beginning. However, when those responses
                    # come back from the broker, they would also come with an UNKNOWN_PRODUCER_ID error. In this case, we should not
                    # reset the sequence numbers to the beginning.
                    return True
                elif self._last_acked_offset(batch.topic_partition) < log_start_offset:
                    # The head of the log has been removed, probably due to the retention time elapsing. In this case,
                    # we expect to lose the producer state. Reset the sequences of all inflight batches to be from the beginning
                    # and retry them.
                    self._start_sequences_at_beginning(batch.topic_partition)
                    return True
            return False

    # visible for testing
    def is_ready(self):
        with self._lock:
            return self.is_transactional() and self._current_state == TransactionState.READY

    def _transition_to(self, target, error=None):
        with self._lock:
            if not self._current_state.is_transition_valid(self._current_state, target):
                raise Errors.KafkaError("TransactionalId %s: Invalid transition attempted from state %s to state %s" % (
                    self.transactional_id, self._current_state.name, target.name))

            if target in (TransactionState.FATAL_ERROR, TransactionState.ABORTABLE_ERROR):
                if error is None:
                    raise Errors.IllegalArgumentError("Cannot transition to %s with an None exception" % (target.name,))
                self._last_error = error
            else:
                self._last_error = None

            if self._last_error is not None:
                log.debug("Transition from state %s to error state %s (%s)", self._current_state.name, target.name, self._last_error)
            else:
                log.debug("Transition from state %s to %s", self._current_state, target)
            self._current_state = target

    def _ensure_transactional(self):
        if not self.is_transactional():
            raise Errors.IllegalStateError("Transactional method invoked on a non-transactional producer.")

    def _maybe_fail_with_error(self):
        if self.has_error():
            raise Errors.KafkaError("Cannot execute transactional method because we are in an error state: %s" % (self._last_error,))

    def _maybe_terminate_request_with_error(self, request_handler):
        if self.has_error():
            if self.has_abortable_error() and isinstance(request_handler, FindCoordinatorHandler):
                # No harm letting the FindCoordinator request go through if we're expecting to abort
                return False
            request_handler.fail(self._last_error)
            return True
        return False

    def _next_pending_requests_sort_id(self):
        self._pending_requests_sort_id += 1
        return self._pending_requests_sort_id

    def _enqueue_request(self, request_handler):
        log.debug("Enqueuing transactional request %s", request_handler.request)
        heapq.heappush(
            self._pending_requests,
            (
                request_handler.priority, # keep lowest priority at head of queue
                self._next_pending_requests_sort_id(), # break ties
                request_handler
            )
        )

    def _lookup_coordinator(self, coord_type, coord_key):
        with self._lock:
            if coord_type == 'group':
                self._consumer_group_coordinator = None
            elif coord_type == 'transaction':
                self._transaction_coordinator = None
            else:
                raise Errors.IllegalStateError("Invalid coordinator type: %s" % (coord_type,))
        self._enqueue_request(FindCoordinatorHandler(self, coord_type, coord_key))

    def _complete_transaction(self):
        with self._lock:
            self._transition_to(TransactionState.READY)
            self._transaction_started = False
            self._new_partitions_in_transaction.clear()
            self._pending_partitions_in_transaction.clear()
            self._partitions_in_transaction.clear()

    def _add_partitions_to_transaction_handler(self):
        with self._lock:
            self._pending_partitions_in_transaction.update(self._new_partitions_in_transaction)
            self._new_partitions_in_transaction.clear()
            return AddPartitionsToTxnHandler(self, self.transactional_id, self.producer_id_and_epoch.producer_id, self.producer_id_and_epoch.epoch, self._pending_partitions_in_transaction)


class TransactionalRequestResult(object):
    def __init__(self):
        self._latch = threading.Event()
        self._error = None

    def done(self, error=None):
        self._error = error
        self._latch.set()

    def wait(self, timeout_ms=None):
        timeout = timeout_ms / 1000 if timeout_ms is not None else None
        success = self._latch.wait(timeout)
        if self._error:
            raise self._error
        return success

    @property
    def is_done(self):
        return self._latch.is_set()

    @property
    def succeeded(self):
        return self._error is None and self._latch.is_set()


@six.add_metaclass(abc.ABCMeta)
class TxnRequestHandler(object):
    def __init__(self, transaction_manager, result=None):
        self.transaction_manager = transaction_manager
        self.retry_backoff_ms = transaction_manager.retry_backoff_ms
        self.request = None
        self._result = result or TransactionalRequestResult()
        self._is_retry = False

    def fatal_error(self, exc):
        self.transaction_manager._transition_to_fatal_error(exc)
        self._result.done(error=exc)

    def abortable_error(self, exc):
        self.transaction_manager._transition_to_abortable_error(exc)
        self._result.done(error=exc)

    def fail(self, exc):
        self._result.done(error=exc)

    def reenqueue(self):
        with self.transaction_manager._lock:
            self._is_retry = True
            self.transaction_manager._enqueue_request(self)

    def on_complete(self, correlation_id, response_or_exc):
        if correlation_id != self.transaction_manager._in_flight_request_correlation_id:
            self.fatal_error(RuntimeError("Detected more than one in-flight transactional request."))
        else:
            self.transaction_manager.clear_in_flight_transactional_request_correlation_id()
            if isinstance(response_or_exc, Errors.KafkaConnectionError):
                log.debug("Disconnected from node. Will retry.")
                if self.needs_coordinator():
                    self.transaction_manager._lookup_coordinator(self.coordinator_type, self.coordinator_key)
                self.reenqueue()
            elif isinstance(response_or_exc, Errors.UnsupportedVersionError):
                self.fatal_error(response_or_exc)
            elif not isinstance(response_or_exc, (Exception, type(None))):
                log.debug("Received transactional response %s for request %s", response_or_exc, self.request)
                with self.transaction_manager._lock:
                    self.handle_response(response_or_exc)
            else:
                self.fatal_error(Errors.KafkaError("Could not execute transactional request for unknown reasons: %s" % response_or_exc))

    def needs_coordinator(self):
        return self.coordinator_type is not None

    @property
    def result(self):
        return self._result

    @property
    def coordinator_type(self):
        return 'transaction'

    @property
    def coordinator_key(self):
        return self.transaction_manager.transactional_id

    def set_retry(self):
        self._is_retry = True

    @property
    def is_retry(self):
        return self._is_retry

    @abc.abstractmethod
    def handle_response(self, response):
        pass

    @abc.abstractproperty
    def priority(self):
        pass


class InitProducerIdHandler(TxnRequestHandler):
    def __init__(self, transaction_manager, transactional_id, transaction_timeout_ms):
        super(InitProducerIdHandler, self).__init__(transaction_manager)

        self.transactional_id = transactional_id
        if transaction_manager._api_version >= (2, 0):
            version = 1
        else:
            version = 0
        self.request = InitProducerIdRequest[version](
            transactional_id=transactional_id,
            transaction_timeout_ms=transaction_timeout_ms)

    @property
    def priority(self):
        return Priority.INIT_PRODUCER_ID

    def handle_response(self, response):
        error = Errors.for_code(response.error_code)

        if error is Errors.NoError:
            self.transaction_manager.set_producer_id_and_epoch(ProducerIdAndEpoch(response.producer_id, response.producer_epoch))
            self.transaction_manager._transition_to(TransactionState.READY)
            self._result.done()
        elif error in (Errors.NotCoordinatorError, Errors.CoordinatorNotAvailableError):
            self.transaction_manager._lookup_coordinator('transaction', self.transactional_id)
            self.reenqueue()
        elif error in (Errors.CoordinatorLoadInProgressError, Errors.ConcurrentTransactionsError):
            self.reenqueue()
        elif error is Errors.TransactionalIdAuthorizationFailedError:
            self.fatal_error(error())
        else:
            self.fatal_error(Errors.KafkaError("Unexpected error in InitProducerIdResponse: %s" % (error())))

class AddPartitionsToTxnHandler(TxnRequestHandler):
    def __init__(self, transaction_manager, transactional_id, producer_id, producer_epoch, topic_partitions):
        super(AddPartitionsToTxnHandler, self).__init__(transaction_manager)

        self.transactional_id = transactional_id
        if transaction_manager._api_version >= (2, 7):
            version = 2
        elif transaction_manager._api_version >= (2, 0):
            version = 1
        else:
            version = 0
        topic_data = collections.defaultdict(list)
        for tp in topic_partitions:
            topic_data[tp.topic].append(tp.partition)
        self.request = AddPartitionsToTxnRequest[version](
            transactional_id=transactional_id,
            producer_id=producer_id,
            producer_epoch=producer_epoch,
            topics=list(topic_data.items()))

    @property
    def priority(self):
        return Priority.ADD_PARTITIONS_OR_OFFSETS

    def handle_response(self, response):
        has_partition_errors = False
        unauthorized_topics = set()
        self.retry_backoff_ms = self.transaction_manager.retry_backoff_ms

        results = {TopicPartition(topic, partition): Errors.for_code(error_code)
                   for topic, partition_data in response.results
                   for partition, error_code in partition_data}

        for tp, error in six.iteritems(results):
            if error is Errors.NoError:
                continue
            elif error in (Errors.CoordinatorNotAvailableError, Errors.NotCoordinatorError):
                self.transaction_manager._lookup_coordinator('transaction', self.transactiona_id)
                self.reenqueue()
                return
            elif error is Errors.ConcurrentTransactionError:
                self.maybe_override_retry_backoff_ms()
                self.reenqueue()
                return
            elif error in (Errors.CoordinatorLoadInProgressError, Errors.UnknownTopicOrPartitionError):
                self.reenqueue()
                return
            elif error is Errors.InvalidProducerEpochError:
                self.fatal_error(error())
                return
            elif error is Errors.TransactionalIdAuthorizationFailedError:
                self.fatal_error(error())
                return
            elif error in (Errors.InvalidProducerIdMappingError, Errors.InvalidTxnStateError):
                self.fatal_error(Errors.KafkaError(error()))
                return
            elif error is Errors.TopicAuthorizationFailedError:
                unauthorized_topics.add(tp.topic)
            elif error is Errors.OperationNotAttemptedError:
                log.debug("Did not attempt to add partition %s to transaction because other partitions in the"
                          " batch had errors.", tp)
                has_partition_errors = True
            else:
                log.error("Could not add partition %s due to unexpected error %s", tp, error())
                has_partition_errors = True

        partitions = set(results)

        # Remove the partitions from the pending set regardless of the result. We use the presence
        # of partitions in the pending set to know when it is not safe to send batches. However, if
        # the partitions failed to be added and we enter an error state, we expect the batches to be
        # aborted anyway. In this case, we must be able to continue sending the batches which are in
        # retry for partitions that were successfully added.
        self.transaction_manager._pending_partitions_in_transaction -= partitions

        if unauthorized_topics:
            self.abortable_error(Errors.TopicAuthorizationError(unauthorized_topics))
        elif has_partition_errors:
            self.abortable_error(Errors.KafkaError("Could not add partitions to transaction due to errors: %s" % (results)))
        else:
            log.debug("Successfully added partitions %s to transaction", partitions)
            self.transaction_manager._partitions_in_transaction.update(partitions)
            self.transaction_manager._transaction_started = True
            self._result.done()

    def maybe_override_retry_backoff_ms(self):
        # We only want to reduce the backoff when retrying the first AddPartition which errored out due to a
        # CONCURRENT_TRANSACTIONS error since this means that the previous transaction is still completing and
        # we don't want to wait too long before trying to start the new one.
        #
        # This is only a temporary fix, the long term solution is being tracked in
        # https://issues.apache.org/jira/browse/KAFKA-5482
        if not self._partitions_in_transaction:
            self.retry_backoff_ms = min(self.transaction_manager.ADD_PARTITIONS_RETRY_BACKOFF_MS, self.retry_backoff_ms)


class FindCoordinatorHandler(TxnRequestHandler):
    def __init__(self, transaction_manager, coord_type, coord_key):
        super(FindCoordinatorHandler, self).__init__(transaction_manager)

        self._coord_type = coord_type
        self._coord_key = coord_key
        if transaction_manager._api_version >= (2, 0):
            version = 2
        else:
            version = 1
        if coord_type == 'group':
            coord_type_int8 = 0
        elif coord_type == 'transaction':
            coord_type_int8 = 1
        else:
            raise ValueError("Unrecognized coordinator type: %s" % (coord_type,))
        self.request = FindCoordinatorRequest[version](
            coordinator_key=coord_key,
            coordinator_type=coord_type_int8,
        )

    @property
    def priority(self):
        return Priority.FIND_COORDINATOR

    @property
    def coordinator_type(self):
        return None

    @property
    def coordinator_key(self):
        return None

    def handle_response(self, response):
        error = Errors.for_code(response.error_code)

        if error is Errors.NoError:
            coordinator_id = self.transaction_manager._metadata.add_coordinator(
                response, self._coord_type, self._coord_key)
            if self._coord_type == 'group':
                self.transaction_manager._consumer_group_coordinator = coordinator_id
            elif self._coord_type == 'transaction':
                self.transaction_manager._transaction_coordinator = coordinator_id
            self._result.done()
        elif error is Errors.CoordinatorNotAvailableError:
            self.reenqueue()
        elif error is Errors.TransactionalIdAuthorizationFailedError:
            self.fatal_error(error())
        elif error is Errors.GroupAuthorizationFailedError:
            self.abortable_error(Errors.GroupAuthorizationError(self._coord_key))
        else:
            self.fatal_error(Errors.KafkaError(
                "Could not find a coordinator with type %s with key %s due to"
                " unexpected error: %s" % (self._coord_type, self._coord_key, error())))


class EndTxnHandler(TxnRequestHandler):
    def __init__(self, transaction_manager, transactional_id, producer_id, producer_epoch, committed):
        super(EndTxnHandler, self).__init__(transaction_manager)

        self.transactional_id = transactional_id
        if self.transaction_manager._api_version >= (2, 7):
            version = 2
        elif self.transaction_manager._api_version >= (2, 0):
            version = 1
        else:
            version = 0
        self.request = EndTxnRequest[version](
            transactional_id=transactional_id,
            producer_id=producer_id,
            producer_epoch=producer_epoch,
            committed=committed)

    @property
    def priority(self):
        return Priority.END_TXN

    def handle_response(self, response):
        error = Errors.for_code(response.error_code)

        if error is Errors.NoError:
            self.transaction_manager._complete_transaction()
            self._result.done()
        elif error in (Errors.CoordinatorNotAvailableError, Errors.NotCoordinatorError):
            self.transaction_manager._lookup_coordinator('transaction', self.transactional_id)
            self.reenqueue()
        elif error in (Errors.CoordinatorLoadInProgressError, Errors.ConcurrentTransactionsError):
            self.reenqueue()
        elif error is Errors.InvalidProducerEpochError:
            self.fatal_error(error())
        elif error is Errors.TransactionalIdAuthorizationFailedError:
            self.fatal_error(error())
        elif error is Errors.InvalidTxnStateError:
            self.fatal_error(error())
        else:
            self.fatal_error(Errors.KafkaError("Unhandled error in EndTxnResponse: %s" % (error())))
