from __future__ import absolute_import, division

import logging
import time

try:
    # enum in stdlib as of py3.4
    from enum import IntEnum  # pylint: disable=import-error
except ImportError:
    # vendored backport module
    from kafka.vendor.enum34 import IntEnum

import kafka.errors as Errors
from kafka.producer.future import FutureRecordMetadata, FutureProduceResult


log = logging.getLogger(__name__)


class FinalState(IntEnum):
    ABORTED = 0
    FAILED = 1
    SUCCEEDED = 2


class ProducerBatch(object):
    def __init__(self, tp, records, now=None):
        now = time.time() if now is None else now
        self.max_record_size = 0
        self.created = now
        self.drained = None
        self.attempts = 0
        self.last_attempt = now
        self.last_append = now
        self.records = records
        self.topic_partition = tp
        self.produce_future = FutureProduceResult(tp)
        self._retry = False
        self._final_state = None

    @property
    def final_state(self):
        return self._final_state

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
        future = FutureRecordMetadata(
            self.produce_future,
            metadata.offset,
            metadata.timestamp,
            metadata.crc,
            len(key) if key is not None else -1,
            len(value) if value is not None else -1,
            sum(len(h_key.encode("utf-8")) + len(h_val) for h_key, h_val in headers) if headers else -1)
        return future

    def abort(self, exception):
        """Abort the batch and complete the future and callbacks."""
        if self._final_state is not None:
            raise Errors.IllegalStateError("Batch has already been completed in final state: %s" % self._final_state)
        self._final_state = FinalState.ABORTED

        log.debug("Aborting batch for partition %s: %s", self.topic_partition, exception)
        self._complete_future(-1, -1, lambda _: exception)

    def complete(self, base_offset, log_append_time):
        """Complete the batch successfully.

        Arguments:
            base_offset (int): The base offset of the messages assigned by the server
            log_append_time (int): The log append time or -1 if CreateTime is being used

        Returns: True if the batch was completed as a result of this call, and False
            if it had been completed previously.
        """
        return self.done(base_offset=base_offset, timestamp_ms=log_append_time)

    def complete_exceptionally(self, top_level_exception, record_exceptions_fn):
        """
        Complete the batch exceptionally. The provided top-level exception will be used
        for each record future contained in the batch.

        Arguments:
            top_level_exception (Exception): top-level partition error.
            record_exceptions_fn (callable int -> Exception): Record exception function mapping
                batch_index to the respective record exception.
        Returns: True if the batch was completed as a result of this call, and False
            if it had been completed previously.
        """
        assert isinstance(top_level_exception, Exception)
        assert callable(record_exceptions_fn)
        return self.done(top_level_exception=top_level_exception, record_exceptions_fn=record_exceptions_fn)

    def done(self, base_offset=None, timestamp_ms=None, top_level_exception=None, record_exceptions_fn=None):
        """
        Finalize the state of a batch. Final state, once set, is immutable. This function may be called
        once or twice on a batch. It may be called twice if
            1. An inflight batch expires before a response from the broker is received. The batch's final
            state is set to FAILED. But it could succeed on the broker and second time around batch.done() may
            try to set SUCCEEDED final state.

            2. If a transaction abortion happens or if the producer is closed forcefully, the final state is
            ABORTED but again it could succeed if broker responds with a success.

        Attempted transitions from [FAILED | ABORTED] --> SUCCEEDED are logged.
        Attempted transitions from one failure state to the same or a different failed state are ignored.
        Attempted transitions from SUCCEEDED to the same or a failed state throw an exception.
        """
        final_state = FinalState.SUCCEEDED if top_level_exception is None else FinalState.FAILED
        if self._final_state is None:
            self._final_state = final_state
            if final_state is FinalState.SUCCEEDED:
                log.debug("Successfully produced messages to %s with base offset %s", self.topic_partition, base_offset)
            else:
                log.warning("Failed to produce messages to topic-partition %s with base offset %s: %s",
                            self.topic_partition, base_offset, top_level_exception)
            self._complete_future(base_offset, timestamp_ms, record_exceptions_fn)
            return True

        elif self._final_state is not FinalState.SUCCEEDED:
            if final_state is FinalState.SUCCEEDED:
                # Log if a previously unsuccessful batch succeeded later on.
                log.debug("ProduceResponse returned %s for %s after batch with base offset %s had already been %s.",
                          final_state, self.topic_partition, base_offset, self._final_state)
            else:
                # FAILED --> FAILED and ABORTED --> FAILED transitions are ignored.
                log.debug("Ignored state transition %s -> %s for %s batch with base offset %s",
                          self._final_state, final_state, self.topic_partition, base_offset)
        else:
            # A SUCCESSFUL batch must not attempt another state change.
            raise Errors.IllegalStateError("A %s batch must not attempt another state change to %s" % (self._final_state, final_state))
        return False

    def _complete_future(self, base_offset, timestamp_ms, record_exceptions_fn):
        if self.produce_future.is_done:
            raise Errors.IllegalStateError('Batch is already closed!')
        self.produce_future.success((base_offset, timestamp_ms, record_exceptions_fn))

    def has_reached_delivery_timeout(self, delivery_timeout_ms, now=None):
        now = time.time() if now is None else now
        return delivery_timeout_ms / 1000 <= now - self.created

    def in_retry(self):
        return self._retry

    def retry(self, now=None):
        now = time.time() if now is None else now
        self._retry = True
        self.attempts += 1
        self.last_attempt = now
        self.last_append = now

    @property
    def is_done(self):
        return self.produce_future.is_done

    def __str__(self):
        return 'ProducerBatch(topic_partition=%s, record_count=%d)' % (
            self.topic_partition, self.records.next_offset())

    # for heapq
    def __lt__(self, other):
        return self.created < other.created
