from __future__ import absolute_import

import collections
import threading

from .. import errors as Errors
from ..future import Future


class FutureProduceResult(Future):
    def __init__(self, topic_partition):
        super(FutureProduceResult, self).__init__()
        self.topic_partition = topic_partition
        self._latch = threading.Event()

    def success(self, value):
        ret = super(FutureProduceResult, self).success(value)
        self._latch.set()
        return ret

    def failure(self, error):
        ret = super(FutureProduceResult, self).failure(error)
        self._latch.set()
        return ret

    def wait(self, timeout=None):
        # wait() on python2.6 returns None instead of the flag value
        return self._latch.wait(timeout) or self._latch.is_set()


class FutureRecordMetadata(Future):
    def __init__(self, produce_future, relative_offset, timestamp_ms):
        super(FutureRecordMetadata, self).__init__()
        self._produce_future = produce_future
        self.relative_offset = relative_offset
        self.timestamp_ms = timestamp_ms
        produce_future.add_callback(self._produce_success)
        produce_future.add_errback(self.failure)

    def _produce_success(self, offset_and_timestamp):
        base_offset, timestamp_ms = offset_and_timestamp
        if timestamp_ms is None:
            timestamp_ms = self.timestamp_ms
        self.success(RecordMetadata(self._produce_future.topic_partition,
                                    base_offset, timestamp_ms,
                                    self.relative_offset))

    def get(self, timeout=None):
        if not self.is_done and not self._produce_future.wait(timeout):
            raise Errors.KafkaTimeoutError(
                "Timeout after waiting for %s secs." % timeout)
        assert self.is_done
        if self.failed():
            raise self.exception # pylint: disable-msg=raising-bad-type
        return self.value


class RecordMetadata(collections.namedtuple(
    'RecordMetadata', 'topic partition topic_partition offset timestamp')):
    def __new__(cls, tp, base_offset, timestamp, relative_offset=None):
        offset = base_offset
        if relative_offset is not None and base_offset != -1:
            offset += relative_offset
        return super(RecordMetadata, cls).__new__(cls, tp.topic, tp.partition,
                                                  tp, offset, timestamp)

    def __str__(self):
        return 'RecordMetadata(topic=%s, partition=%s, offset=%s)' % (
            self.topic, self.partition, self.offset)

    def __repr__(self):
        return str(self)
