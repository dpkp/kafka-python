# pylint: skip-file

import pytest

from kafka.errors import IllegalStateError, KafkaError
from kafka.producer.future import FutureRecordMetadata, RecordMetadata
from kafka.producer.producer_batch import ProducerBatch
from kafka.record.memory_records import MemoryRecordsBuilder
from kafka.structs import TopicPartition


@pytest.fixture
def tp():
    return TopicPartition('foo', 0)


@pytest.fixture
def memory_records_builder():
    return MemoryRecordsBuilder(magic=2, compression_type=0, batch_size=100000)


@pytest.fixture
def batch(tp, memory_records_builder):
    return ProducerBatch(tp, memory_records_builder)


def test_producer_batch_producer_id(tp, memory_records_builder):
    batch = ProducerBatch(tp, memory_records_builder)
    assert batch.producer_id == -1
    batch.records.set_producer_state(123, 456, 789, False)
    assert batch.producer_id == 123
    memory_records_builder.close()
    assert batch.producer_id == 123


@pytest.mark.parametrize("magic", [0, 1, 2])
def test_producer_batch_try_append(magic):
    tp = TopicPartition('foo', 0)
    records = MemoryRecordsBuilder(
        magic=magic, compression_type=0, batch_size=100000)
    batch = ProducerBatch(tp, records)
    assert batch.record_count == 0
    future = batch.try_append(0, b'key', b'value', [])
    assert isinstance(future, FutureRecordMetadata)
    assert not future.is_done
    batch.complete(123, 456)
    assert future.is_done
    # record-level checksum only provided in v0/v1 formats; payload includes magic-byte
    if magic == 0:
        checksum = 592888119
    elif magic == 1:
        checksum = 213653215
    else:
        checksum = None

    expected_metadata = RecordMetadata(
        topic=tp[0], partition=tp[1], topic_partition=tp,
        offset=123, timestamp=456, checksum=checksum,
        serialized_key_size=3, serialized_value_size=5, serialized_header_size=-1)
    assert future.value == expected_metadata


def test_producer_batch_retry(batch):
    assert not batch.in_retry()
    batch.retry()
    assert batch.in_retry()


def test_batch_abort(batch):
    future = batch.try_append(123, None, b'msg', [])
    batch.abort(KafkaError())
    assert future.is_done

    # subsequent completion should be ignored
    assert not batch.complete(500, 2342342341)
    assert not batch.complete_exceptionally(KafkaError('top_level'), lambda _: KafkaError('record'))

    assert future.is_done
    with pytest.raises(KafkaError):
        future.get()


def test_batch_cannot_abort_twice(batch):
    future = batch.try_append(123, None, b'msg', [])
    batch.abort(KafkaError())
    with pytest.raises(IllegalStateError):
        batch.abort(KafkaError())
    assert future.is_done
    with pytest.raises(KafkaError):
        future.get()


def test_batch_cannot_complete_twice(batch):
    future = batch.try_append(123, None, b'msg', [])
    batch.complete(500, 10)
    with pytest.raises(IllegalStateError):
        batch.complete(1000, 20)
    record_metadata = future.get()
    assert record_metadata.offset == 500
    assert record_metadata.timestamp == 10


def _test_complete_exceptionally(batch, record_count, top_level_exception, record_exceptions_fn):
    futures = []
    for i in range(record_count):
        futures.append(batch.try_append(0, b'key', b'value', []))

    assert record_count == batch.record_count

    batch.complete_exceptionally(top_level_exception, record_exceptions_fn)
    assert batch.is_done

    for i, future in enumerate(futures):
        assert future.is_done
        assert future.failed()
        assert isinstance(future.exception, RuntimeError)
        assert record_exceptions_fn(i) == future.exception


def test_complete_exceptionally_with_record_errors(batch):
    record_count = 5
    top_level_exception = RuntimeError()

    record_exceptions_map = {0: RuntimeError(), 3: RuntimeError()}
    record_exceptions_fn = lambda i: record_exceptions_map.get(i, top_level_exception)

    _test_complete_exceptionally(batch, record_count, top_level_exception, record_exceptions_fn)


def test_complete_exceptionally_with_null_record_errors(batch):
    record_count = 5
    top_level_exception = RuntimeError()

    with pytest.raises(AssertionError):
        _test_complete_exceptionally(batch, record_count, top_level_exception, None)


def test_producer_batch_lt(tp, memory_records_builder):
    b1 = ProducerBatch(tp, memory_records_builder, now=1)
    b2 = ProducerBatch(tp, memory_records_builder, now=2)

    assert b1 < b2
    assert not b1 < b1

    import heapq
    q = []
    heapq.heappush(q, b2)
    heapq.heappush(q, b1)
    assert q[0] == b1
    assert q[1] == b2
