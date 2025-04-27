# pylint: skip-file
from __future__ import absolute_import, division

import pytest

from kafka.cluster import ClusterMetadata
from kafka.errors import IllegalStateError, KafkaError
from kafka.producer.future import FutureRecordMetadata, RecordMetadata
from kafka.producer.record_accumulator import RecordAccumulator, ProducerBatch
from kafka.record.default_records import DefaultRecordBatchBuilder
from kafka.record.memory_records import MemoryRecordsBuilder
from kafka.structs import TopicPartition


@pytest.fixture
def tp():
    return TopicPartition('foo', 0)

@pytest.fixture
def cluster(tp, mocker):
    metadata = ClusterMetadata()
    mocker.patch.object(metadata, 'leader_for_partition', return_value=0)
    mocker.patch.object(metadata, 'partitions_for_broker', return_value=[tp])
    return metadata

def test_producer_batch_producer_id():
    tp = TopicPartition('foo', 0)
    records = MemoryRecordsBuilder(
        magic=2, compression_type=0, batch_size=100000)
    batch = ProducerBatch(tp, records)
    assert batch.producer_id == -1
    batch.records.set_producer_state(123, 456, 789, False)
    assert batch.producer_id == 123
    records.close()
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
    batch.done(base_offset=123, timestamp_ms=456)
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

def test_producer_batch_retry():
    tp = TopicPartition('foo', 0)
    records = MemoryRecordsBuilder(
        magic=2, compression_type=0, batch_size=100000)
    batch = ProducerBatch(tp, records)
    assert not batch.in_retry()
    batch.retry()
    assert batch.in_retry()

def test_batch_abort():
    tp = TopicPartition('foo', 0)
    records = MemoryRecordsBuilder(
        magic=2, compression_type=0, batch_size=100000)
    batch = ProducerBatch(tp, records)
    future = batch.try_append(123, None, b'msg', [])

    batch.abort(KafkaError())
    assert future.is_done

    # subsequent completion should be ignored
    batch.done(500, 2342342341)
    batch.done(exception=KafkaError())

    assert future.is_done
    with pytest.raises(KafkaError):
        future.get()

def test_batch_cannot_abort_twice():
    tp = TopicPartition('foo', 0)
    records = MemoryRecordsBuilder(
        magic=2, compression_type=0, batch_size=100000)
    batch = ProducerBatch(tp, records)
    future = batch.try_append(123, None, b'msg', [])

    batch.abort(KafkaError())

    with pytest.raises(IllegalStateError):
        batch.abort(KafkaError())

    assert future.is_done
    with pytest.raises(KafkaError):
        future.get()

def test_batch_cannot_complete_twice():
    tp = TopicPartition('foo', 0)
    records = MemoryRecordsBuilder(
        magic=2, compression_type=0, batch_size=100000)
    batch = ProducerBatch(tp, records)
    future = batch.try_append(123, None, b'msg', [])

    batch.done(500, 10, None)

    with pytest.raises(IllegalStateError):
        batch.done(1000, 20, None)

    record_metadata = future.get()

    assert record_metadata.offset == 500
    assert record_metadata.timestamp == 10

def test_linger(tp, cluster):
    now = 0
    accum = RecordAccumulator(linger_ms=10)
    accum.append(tp, 0, b'key', b'value', [], now=now)
    ready, next_ready_check, _unknown_leaders_exist = accum.ready(cluster, now=now)
    assert len(ready) == 0, 'No partitions should be ready'
    assert next_ready_check == .01 # linger_ms in secs
    now += .01
    ready, _next_ready_check, _unknown_leaders_exist = accum.ready(cluster, now=now)
    assert ready == set([0]), "Our partitions leader should be ready"
    batches = accum.drain(cluster, ready, 0, 2147483647)[0]
    assert len(batches) == 1
    batch = batches[0]
    assert batch.records.is_full()

    parsed = list(batch.records.records())
    assert len(parsed) == 1
    records = list(parsed[0])
    assert len(records) == 1
    assert records[0].key == b'key', 'Keys should match'
    assert records[0].value == b'value', 'Values should match'

def _advance_now_ms(now, ms):
    return now + ms / 1000 + 1/10000 # add extra .1 ms to each advance to avoid rounding issues when converting back to seconds

def _do_expire_batch_single(cluster, tp, delivery_timeout_ms):
    now = 0
    linger_ms = 300
    accum = RecordAccumulator(linger_ms=linger_ms, delivery_timeout_ms=delivery_timeout_ms, request_timeout_ms=(delivery_timeout_ms-linger_ms-100))

    # Make the batches ready due to linger. These batches are not in retry
    for mute in [False, True]:
        accum.append(tp, 0, b'key', b'value', [], now=now)
        ready, next_ready_check, _unknown_leaders_exist = accum.ready(cluster, now=now)
        assert len(ready) == 0, 'No partitions should be ready'
        assert next_ready_check == linger_ms / 1000

        now = _advance_now_ms(now, linger_ms)
        ready, _next_ready_check, _unknown_leaders_exist = accum.ready(cluster, now=now)
        assert ready == set([0]), "Our partitions leader should be ready"

        expired_batches = accum.expired_batches(now=now)
        assert len(expired_batches) == 0, "The batch should not expire when just linger has passed"

        if mute:
            accum.muted.add(tp)
        else:
            try:
                accum.muted.remove(tp)
            except KeyError:
                pass

        # Advance the clock to expire the batch.
        now = _advance_now_ms(now, delivery_timeout_ms - linger_ms)
        expired_batches = accum.expired_batches(now=now)
        assert len(expired_batches) == 1, "The batch may expire when the partition is muted"
        ready, _next_ready_check, _unknown_leaders_exist = accum.ready(cluster, now=now)
        assert len(ready) == 0, "No partitions should be ready."

def test_expired_batch_single(cluster, tp):
    _do_expire_batch_single(cluster, tp, 3200)

def test_expired_batch_single_max_value(cluster, tp):
    _do_expire_batch_single(cluster, tp, 2147483647)

def _expected_num_appends(batch_size):
    size = DefaultRecordBatchBuilder.header_size_in_bytes()
    offset_delta = 0
    while True:
        record_size = DefaultRecordBatchBuilder.size_in_bytes(offset_delta, 0, b'key', b'value', [])
        if size + record_size > batch_size:
            return offset_delta
        offset_delta += 1
        size += record_size

def test_expired_batches(cluster, tp):
    now = 0
    retry_backoff_ms = 100
    linger_ms = 30
    request_timeout_ms = 60
    delivery_timeout_ms = 3200
    batch_size = 1024
    accum = RecordAccumulator(linger_ms=linger_ms, delivery_timeout_ms=delivery_timeout_ms, request_timeout_ms=request_timeout_ms, retry_backoff_ms=retry_backoff_ms, batch_size=batch_size)
    appends = _expected_num_appends(batch_size)

    # Test batches not in retry
    for i in range(appends):
        accum.append(tp, 0, b'key', b'value', [], now=now)
        ready, next_ready_check, _unknown_leaders_exist = accum.ready(cluster, now=now)
        assert len(ready) == 0, 'No partitions should be ready'
        assert next_ready_check == linger_ms / 1000

    # Make the batches ready due to batch full
    accum.append(tp, 0, b'key', b'value', [], now=now)
    ready, _next_ready_check, _unknown_leaders_exist = accum.ready(cluster, now=now)
    assert ready == set([0]), "Our partitions leader should be ready"

    # Advance the clock to expire the batch.
    now = _advance_now_ms(now, delivery_timeout_ms + 1)
    accum.muted.add(tp)
    expired_batches = accum.expired_batches(now=now)
    assert len(expired_batches) == 2, "The batches will be expired no matter if the partition is muted or not"

    accum.muted.remove(tp)
    expired_batches = accum.expired_batches(now=now)
    assert len(expired_batches) == 0, "All batches should have been expired earlier"
    ready, _next_ready_check, _unknown_leaders_exist = accum.ready(cluster, now=now)
    assert len(ready) == 0, "No partitions should be ready."

    # Test batches in retry.
    # Create a retried batch
    accum.append(tp, 0, b'key', b'value', [], now=now)
    now = _advance_now_ms(now, linger_ms)
    ready, _next_ready_check, _unknown_leaders_exist = accum.ready(cluster, now=now)
    assert ready == set([0]), "Our partitions leader should be ready"

    drained = accum.drain(cluster, ready, 2147483647, now=now)
    assert len(drained[0]) == 1, "There should be only one batch."
    now = _advance_now_ms(now, 1000)
    accum.reenqueue(drained[0][0], now=now)

    # test expiration.
    now = _advance_now_ms(now, request_timeout_ms + retry_backoff_ms)
    expired_batches = accum.expired_batches(now=now)
    assert len(expired_batches) == 0, "The batch should not be expired."
    now = _advance_now_ms(now, 1)

    accum.muted.add(tp)
    expired_batches = accum.expired_batches(now=now)
    assert len(expired_batches) == 0, "The batch should not be expired when the partition is muted"

    accum.muted.remove(tp)
    expired_batches = accum.expired_batches(now=now)
    assert len(expired_batches) == 0, "The batch should not be expired when the partition is unmuted"

    now = _advance_now_ms(now, linger_ms)
    ready, _next_ready_check, _unknown_leaders_exist = accum.ready(cluster, now=now)
    assert ready == set([0]), "Our partitions leader should be ready"

    # Advance the clock to expire the batch.
    now = _advance_now_ms(now, delivery_timeout_ms + 1)
    accum.muted.add(tp)
    expired_batches = accum.expired_batches(now=now)
    assert len(expired_batches) == 1, "The batch should not be expired when the partition is muted"
