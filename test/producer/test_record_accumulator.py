# pylint: skip-file

import pytest

from kafka.cluster import ClusterMetadata
from kafka.producer.record_accumulator import RecordAccumulator
from kafka.record.default_records import DefaultRecordBatchBuilder
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


def test_abort_on_new_batch_returns_sentinel(tp):
    """KIP-480 plumbing: with abort_on_new_batch=True and no in-progress
    batch, append must return (None, False, False, True) instead of
    allocating a fresh batch."""
    accum = RecordAccumulator()
    future, batch_full, new_batch, abort = accum.append(
        tp, 0, b'k', b'v', [], now=0, abort_on_new_batch=True)
    assert future is None
    assert batch_full is False
    assert new_batch is False
    assert abort is True
    # And no batch was actually allocated.
    assert tp not in accum._batches or not accum._batches[tp]


def test_abort_on_new_batch_appends_to_existing(tp):
    """If a batch already exists with room, abort_on_new_batch=True still
    succeeds — only the new-batch-allocation path is gated."""
    accum = RecordAccumulator()
    # First append (with abort_on_new_batch=False) creates the batch.
    future1, _, new1, abort1 = accum.append(
        tp, 0, b'k1', b'v1', [], now=0, abort_on_new_batch=False)
    assert future1 is not None
    assert new1 is True
    assert abort1 is False
    # Second append with abort_on_new_batch=True lands in the existing batch.
    future2, _, new2, abort2 = accum.append(
        tp, 0, b'k2', b'v2', [], now=0, abort_on_new_batch=True)
    assert future2 is not None
    assert new2 is False
    assert abort2 is False
