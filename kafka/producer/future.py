import collections
import threading

from kafka import errors as Errors
from kafka.future import Future
from kafka.util import Timer


class FutureProduceResult(Future):
    def __init__(self, topic_partition):
        super().__init__()
        self.topic_partition = topic_partition
        self._latch = threading.Event()

    def success(self, value):
        ret = super().success(value)
        self._latch.set()
        return ret

    def failure(self, error):
        ret = super().failure(error)
        self._latch.set()
        return ret

    def wait(self, timeout=None):
        # wait() on python2.6 returns None instead of the flag value
        return self._latch.wait(timeout) or self._latch.is_set()


class FutureRecordMetadata(Future):
    def __init__(self, produce_future, batch_index, timestamp_ms, checksum, serialized_key_size, serialized_value_size, serialized_header_size):
        super().__init__()
        self._produce_future = produce_future
        # packing args as a tuple is a minor speed optimization
        self.args = (batch_index, timestamp_ms, checksum, serialized_key_size, serialized_value_size, serialized_header_size)
        produce_future.add_callback(self._produce_success)
        produce_future.add_errback(self.failure)

    def _produce_success(self, result):
        offset, produce_timestamp_ms, record_exceptions_fn = result

        # Unpacking from args tuple is minor speed optimization
        (batch_index, timestamp_ms, checksum,
         serialized_key_size, serialized_value_size, serialized_header_size) = self.args

        if record_exceptions_fn is not None:
            self.failure(record_exceptions_fn(batch_index))
        else:
            # None is when Broker does not support the API (<0.10) and
            # -1 is when the broker is configured for CREATE_TIME timestamps
            if produce_timestamp_ms is not None and produce_timestamp_ms != -1:
                timestamp_ms = produce_timestamp_ms
            if offset != -1 and batch_index is not None:
                offset += batch_index
            tp = self._produce_future.topic_partition
            metadata = RecordMetadata(tp[0], tp[1], tp, offset, timestamp_ms,
                                      checksum, serialized_key_size,
                                      serialized_value_size, serialized_header_size)
            self.success(metadata)

    def rebind(self, new_produce_future, new_batch_index):
        """Rebind this future to a new produce future with a new batch index.

        Used when a batch is split due to MESSAGE_TOO_LARGE. The original
        FutureRecordMetadata is rebound to the new (smaller) batch's future.

        This must be called from the sender thread while the old produce_future
        has not been completed. Any user thread blocked in get() on the old
        produce_future's latch will be woken and will re-wait on the new one.
        """
        old_produce_future = self._produce_future
        self._produce_future = new_produce_future
        _, timestamp_ms, checksum, sk, sv, sh = self.args
        self.args = (new_batch_index, timestamp_ms, checksum, sk, sv, sh)
        new_produce_future.add_callback(self._produce_success)
        new_produce_future.add_errback(self.failure)
        # Wake any thread blocked in get() so it re-waits on the new future.
        # The old produce_future is never completed, so its stale callbacks
        # (registered in __init__) will never fire.
        old_produce_future._latch.set()

    def get(self, timeout=None):
        """Wait for up to timeout seconds for future to complete."""
        # Loop because rebind() may wake us from the old produce_future's
        # latch before the record is actually done. A batch may be split
        # multiple times, so each rebind wakes us and we re-wait on the
        # (possibly new) _produce_future.
        timer = Timer(timeout * 1000 if timeout is not None else None)
        while not self.is_done and not timer.expired:
            if not self._produce_future.wait(timer.timeout_secs):
                raise Errors.KafkaTimeoutError(
                    "Timeout after waiting for %s secs." % (timeout,))
        if self.failed():
            raise self.exception # pylint: disable-msg=raising-bad-type
        return self.value


RecordMetadata = collections.namedtuple(
    'RecordMetadata', ['topic', 'partition', 'topic_partition', 'offset', 'timestamp',
                       'checksum', 'serialized_key_size', 'serialized_value_size', 'serialized_header_size'])
