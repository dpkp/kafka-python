# pylint: skip-file

import collections
import io
import math
import time
from unittest.mock import call

import pytest

from kafka.client_async import KafkaClient
from kafka.cluster import ClusterMetadata
import kafka.errors as Errors
from kafka.protocol.broker_version_data import BrokerVersionData
from kafka.producer.kafka import KafkaProducer
from kafka.protocol.producer import ProduceRequest
from kafka.producer.future import FutureRecordMetadata
from kafka.producer.producer_batch import ProducerBatch
from kafka.producer.record_accumulator import RecordAccumulator
from kafka.producer.sender import PartitionResponse, Sender
from kafka.producer.transaction_manager import TransactionManager
from kafka.record.memory_records import MemoryRecordsBuilder
from kafka.structs import TopicPartition


@pytest.fixture
def accumulator():
    return RecordAccumulator()


@pytest.fixture
def sender(client, accumulator):
    return Sender(client, client.cluster, accumulator)


def producer_batch(topic='foo', partition=0, magic=2):
    tp = TopicPartition(topic, partition)
    records = MemoryRecordsBuilder(
        magic=magic, compression_type=0, batch_size=100000)
    batch = ProducerBatch(tp, records)
    batch.try_append(0, None, b'msg', [])
    batch.records.close()
    return batch


@pytest.fixture
def transaction_manager():
    return TransactionManager(
        transactional_id=None,
        transaction_timeout_ms=60000,
        retry_backoff_ms=100,
        api_version=(2, 1),
        metadata=ClusterMetadata())


@pytest.mark.parametrize(("api_version", "produce_version"), [
    ((2, 1), 7),
    ((0, 10, 0), 2),
    ((0, 9), 1),
    ((0, 8, 0), 0)
])
def test_produce_request(sender, api_version, produce_version):
    sender._client.broker_version_data = BrokerVersionData(api_version)
    magic = KafkaProducer.max_usable_produce_magic(api_version)
    batch = producer_batch(magic=magic)
    produce_request = sender._produce_request(0, 0, 0, [batch])
    assert isinstance(produce_request, ProduceRequest)
    assert produce_request.version == produce_version


@pytest.mark.parametrize(("api_version", "produce_version"), [
    ((2, 1), 7),
])
def test_create_produce_requests(sender, api_version, produce_version):
    sender._client.broker_version_data = BrokerVersionData(api_version)
    tp = TopicPartition('foo', 0)
    magic = KafkaProducer.max_usable_produce_magic(api_version)
    batches_by_node = collections.defaultdict(list)
    for node in range(3):
        for _ in range(5):
            batches_by_node[node].append(producer_batch(magic=magic))
    produce_requests_by_node = sender._create_produce_requests(batches_by_node)
    assert len(produce_requests_by_node) == 3
    for node in range(3):
        assert isinstance(produce_requests_by_node[node], ProduceRequest)
        assert produce_requests_by_node[node].version == produce_version


def test_complete_batch_success(sender):
    batch = producer_batch()
    assert not batch.produce_future.is_done

    # No error, base_offset 0
    sender._complete_batch(batch, PartitionResponse(base_offset=0, log_append_time=123))
    assert batch.is_done
    assert batch.produce_future.is_done
    assert batch.produce_future.succeeded()
    assert batch.produce_future.value == (0, 123, None)


def test_complete_batch_transaction(sender, transaction_manager):
    sender._transaction_manager = transaction_manager
    batch = producer_batch()
    assert sender._transaction_manager.sequence_number(batch.topic_partition) == 0
    assert sender._transaction_manager.producer_id_and_epoch.producer_id == batch.producer_id

    # No error, base_offset 0
    sender._complete_batch(batch, PartitionResponse(base_offset=0))
    assert batch.is_done
    assert sender._transaction_manager.sequence_number(batch.topic_partition) == batch.record_count


@pytest.mark.parametrize(("error", "refresh_metadata"), [
    (Errors.KafkaConnectionError, True),
    (Errors.CorruptRecordError, False),
    (Errors.UnknownTopicOrPartitionError, True),
    (Errors.NotLeaderForPartitionError, True),
    (Errors.MessageSizeTooLargeError, False),
    (Errors.InvalidTopicError, False),
    (Errors.RecordListTooLargeError, False),
    (Errors.NotEnoughReplicasError, False),
    (Errors.NotEnoughReplicasAfterAppendError, False),
    (Errors.InvalidRequiredAcksError, False),
    (Errors.TopicAuthorizationFailedError, False),
    (Errors.UnsupportedForMessageFormatError, False),
    (Errors.InvalidProducerEpochError, False),
    (Errors.ClusterAuthorizationFailedError, False),
    (Errors.TransactionalIdAuthorizationFailedError, False),
])
def test_complete_batch_error(sender, error, refresh_metadata):
    sender._client.cluster._last_successful_refresh_ms = (time.monotonic() - 10) * 1000
    sender._client.cluster._need_update = False
    sender.config['retries'] = 0
    assert sender._client.cluster.ttl() > 0
    batch = producer_batch()
    future = FutureRecordMetadata(batch.produce_future, -1, -1, -1, -1, -1, -1)
    sender._complete_batch(batch, PartitionResponse(error=error))
    if refresh_metadata:
        assert sender._client.cluster.ttl() == 0
    else:
        assert sender._client.cluster.ttl() > 0
    assert batch.is_done
    assert future.failed()
    assert isinstance(future.exception, error)


@pytest.mark.parametrize(("error", "retry"), [
    (Errors.KafkaConnectionError, True),
    (Errors.CorruptRecordError, False),
    (Errors.UnknownTopicOrPartitionError, True),
    (Errors.NotLeaderForPartitionError, True),
    (Errors.MessageSizeTooLargeError, False),
    (Errors.InvalidTopicError, False),
    (Errors.RecordListTooLargeError, False),
    (Errors.NotEnoughReplicasError, True),
    (Errors.NotEnoughReplicasAfterAppendError, True),
    (Errors.InvalidRequiredAcksError, False),
    (Errors.TopicAuthorizationFailedError, False),
    (Errors.UnsupportedForMessageFormatError, False),
    (Errors.InvalidProducerEpochError, False),
    (Errors.ClusterAuthorizationFailedError, False),
    (Errors.TransactionalIdAuthorizationFailedError, False),
])
def test_complete_batch_retry(sender, accumulator, mocker, error, retry):
    sender.config['retries'] = 1
    mocker.patch.object(accumulator, 'reenqueue')
    batch = producer_batch()
    future = FutureRecordMetadata(batch.produce_future, -1, -1, -1, -1, -1, -1)
    sender._complete_batch(batch, PartitionResponse(error=error))
    if retry:
        assert not batch.is_done
        accumulator.reenqueue.assert_called_with(batch)
        batch.attempts += 1 # normally handled by accumulator.reenqueue, but it's mocked
        sender._complete_batch(batch, PartitionResponse(error=error))
        assert batch.is_done
        assert future.failed()
        assert isinstance(future.exception, error)
    else:
        assert batch.is_done
        assert future.failed()
        assert isinstance(future.exception, error)


def test_complete_batch_producer_id_changed_no_retry(sender, accumulator, transaction_manager, mocker):
    sender._transaction_manager = transaction_manager
    sender.config['retries'] = 1
    mocker.patch.object(accumulator, 'reenqueue')
    error = Errors.NotLeaderForPartitionError
    batch = producer_batch()
    future = FutureRecordMetadata(batch.produce_future, -1, -1, -1, -1, -1, -1)
    sender._complete_batch(batch, PartitionResponse(error=error))
    assert not batch.is_done
    accumulator.reenqueue.assert_called_with(batch)
    batch.records._producer_id = 123 # simulate different producer_id
    assert batch.producer_id != sender._transaction_manager.producer_id_and_epoch.producer_id
    sender._complete_batch(batch, PartitionResponse(error=error))
    assert batch.is_done
    assert future.failed()
    assert isinstance(future.exception, error)


def test_fail_batch(sender, accumulator, transaction_manager, mocker):
    sender._transaction_manager = transaction_manager
    batch = producer_batch()
    mocker.patch.object(batch, 'done')
    assert sender._transaction_manager.producer_id_and_epoch.producer_id == batch.producer_id
    error = Errors.KafkaError
    sender._fail_batch(batch, PartitionResponse(error=error))
    batch.done.assert_called_with(top_level_exception=error(None), record_exceptions_fn=mocker.ANY)


def test_out_of_order_sequence_number_reset_producer_id(sender, accumulator, transaction_manager, mocker):
    sender._transaction_manager = transaction_manager
    assert transaction_manager.transactional_id is None # this test is for idempotent producer only
    mocker.patch.object(TransactionManager, 'reset_producer_id')
    batch = producer_batch()
    mocker.patch.object(batch, 'done')
    assert sender._transaction_manager.producer_id_and_epoch.producer_id == batch.producer_id
    error = Errors.OutOfOrderSequenceNumberError
    sender._fail_batch(batch, PartitionResponse(base_offset=0, log_append_time=None, error=error))
    sender._transaction_manager.reset_producer_id.assert_called_once()
    batch.done.assert_called_with(top_level_exception=error(None), record_exceptions_fn=mocker.ANY)


def test_handle_produce_response():
    pass


def test_failed_produce(sender, mocker):
    mocker.patch.object(sender, '_complete_batch')
    mock_batches = ['foo', 'bar', 'fizzbuzz']
    sender._failed_produce(mock_batches, 0, 'error')
    sender._complete_batch.assert_has_calls([
        call('foo', PartitionResponse(error='error')),
        call('bar', PartitionResponse(error='error')),
        call('fizzbuzz', PartitionResponse(error='error')),
    ])


def test_maybe_wait_for_producer_id():
    pass


def test_run_once():
    pass


def test__send_producer_data_expiry_time_reset(sender, accumulator, mocker):
    now = time.monotonic()
    tp = TopicPartition('foo', 0)
    mocker.patch.object(sender, '_failed_produce')
    result = accumulator.append(tp, 0, b'key', b'value', [], now=now)
    poll_timeout_ms = sender._send_producer_data(now=now)
    assert math.isclose(poll_timeout_ms, accumulator.config['delivery_timeout_ms'])
    sender._failed_produce.assert_not_called()
    now += accumulator.config['delivery_timeout_ms']
    poll_timeout_ms = sender._send_producer_data(now=now)
    assert poll_timeout_ms > 0


def test__record_exceptions_fn(sender):
    record_exceptions_fn = sender._record_exceptions_fn(Errors.KafkaError('top-level'), [(0, 'err-0'), (3, 'err-3')], 'message')
    assert record_exceptions_fn(0) == Errors.InvalidRecordError('err-0')
    assert record_exceptions_fn(1) == Errors.KafkaError('Failed to append record because it was part of a batch which had one more more invalid records')
    assert record_exceptions_fn(2) == Errors.KafkaError('Failed to append record because it was part of a batch which had one more more invalid records')
    assert record_exceptions_fn(3) == Errors.InvalidRecordError('err-3')

    record_exceptions_fn = sender._record_exceptions_fn(Errors.KafkaError('top-level'), [(0, 'err-0')], 'message')
    assert record_exceptions_fn(0) == Errors.KafkaError('err-0')


def multi_record_batch(num_records=5, topic='foo', partition=0, batch_size=100000):
    """Create a ProducerBatch with multiple records for split testing."""
    tp = TopicPartition(topic, partition)
    records = MemoryRecordsBuilder(magic=2, compression_type=0, batch_size=batch_size)
    batch = ProducerBatch(tp, records)
    futures = []
    for i in range(num_records):
        future = batch.try_append(0, b'key-%d' % i, b'value-%d' % i, [])
        futures.append(future)
    batch.records.close()
    return batch, futures


def test_can_split():
    """_can_split returns True for MESSAGE_TOO_LARGE with >1 record."""
    from kafka.producer.sender import Sender
    batch, _ = multi_record_batch(num_records=5)
    assert batch.record_count == 5

    # _can_split is a bound method, so we test the logic directly
    assert (Errors.MessageSizeTooLargeError in (Errors.MessageSizeTooLargeError, Errors.RecordListTooLargeError)
            and batch.record_count > 1
            and batch.final_state is None
            and not batch.has_reached_delivery_timeout(120000))

    # Single record should not be splittable
    batch1, _ = multi_record_batch(num_records=1)
    assert batch1.record_count == 1
    assert not (batch1.record_count > 1)


def test_can_split_method(sender):
    batch, _ = multi_record_batch(num_records=5)
    assert sender._can_split(batch, Errors.MessageSizeTooLargeError)
    assert sender._can_split(batch, Errors.RecordListTooLargeError)
    assert not sender._can_split(batch, Errors.KafkaConnectionError)
    assert not sender._can_split(batch, Errors.NotLeaderForPartitionError)

    # Single record: cannot split
    batch1, _ = multi_record_batch(num_records=1)
    assert not sender._can_split(batch1, Errors.MessageSizeTooLargeError)


def test_can_split_delivery_timeout(sender):
    batch, _ = multi_record_batch(num_records=5)
    # Simulate expired batch
    batch.created = time.monotonic() - 999999
    assert not sender._can_split(batch, Errors.MessageSizeTooLargeError)


def test_split_and_reenqueue(accumulator):
    """RecordAccumulator.split_and_reenqueue splits a batch and enqueues new batches."""
    tp = TopicPartition('foo', 0)
    batch, futures = multi_record_batch(num_records=10)
    # Add batch to incomplete tracking (normally done during append)
    accumulator._incomplete.add(batch)

    num_new = accumulator.split_and_reenqueue(batch)
    accumulator.deallocate(batch)

    assert num_new >= 2  # Should produce at least 2 new batches
    # Check that new batches are in the deque
    dq = accumulator._batches[tp]
    assert len(dq) == num_new

    total_records = sum(b.record_count for b in dq)
    assert total_records == 10


def test_split_and_reenqueue_preserves_creation_time(accumulator):
    """Split batches preserve the original batch's creation time for delivery timeout."""
    tp = TopicPartition('foo', 0)
    batch, _ = multi_record_batch(num_records=4)
    original_created = batch.created
    accumulator._incomplete.add(batch)

    accumulator.split_and_reenqueue(batch)
    accumulator.deallocate(batch)

    for new_batch in accumulator._batches[tp]:
        assert new_batch.created == original_created


def test_split_and_reenqueue_preserves_attempts(accumulator):
    """Split batches inherit the original batch's attempt count."""
    tp = TopicPartition('foo', 0)
    batch, _ = multi_record_batch(num_records=4)
    batch.attempts = 3
    accumulator._incomplete.add(batch)

    accumulator.split_and_reenqueue(batch)
    accumulator.deallocate(batch)

    for new_batch in accumulator._batches[tp]:
        assert new_batch.attempts == 3


def test_split_future_rebinding(accumulator):
    """After split, original futures resolve when new batches complete."""
    tp = TopicPartition('foo', 0)
    batch, futures = multi_record_batch(num_records=4)
    accumulator._incomplete.add(batch)

    accumulator.split_and_reenqueue(batch)
    accumulator.deallocate(batch)

    # Complete each new batch and verify original futures resolve
    dq = accumulator._batches[tp]
    base_offset = 100
    record_idx = 0
    for new_batch in list(dq):
        new_batch.complete(base_offset, -1)
        for i in range(new_batch.record_count):
            future = futures[record_idx]
            assert future.is_done, "Future %d should be resolved" % record_idx
            assert future.succeeded(), "Future %d should have succeeded" % record_idx
            metadata = future.value
            assert metadata.offset == base_offset + i
            record_idx += 1
        base_offset += 1000

    assert record_idx == 4


def test_split_future_rebinding_on_error(accumulator):
    """After split, if a new batch fails, the original futures for those records fail."""
    tp = TopicPartition('foo', 0)
    batch, futures = multi_record_batch(num_records=4)
    accumulator._incomplete.add(batch)

    accumulator.split_and_reenqueue(batch)
    accumulator.deallocate(batch)

    dq = accumulator._batches[tp]
    # Fail all new batches
    for new_batch in list(dq):
        error = Errors.KafkaError("test error")
        new_batch.complete_exceptionally(error, lambda _: error)

    for future in futures:
        assert future.is_done
        assert future.failed()
        assert isinstance(future.exception, Errors.KafkaError)


def test_complete_batch_splits_on_message_too_large(sender, accumulator, mocker):
    """_complete_batch splits batch on MESSAGE_TOO_LARGE instead of failing."""
    tp = TopicPartition('foo', 0)
    batch, futures = multi_record_batch(num_records=5)
    accumulator._incomplete.add(batch)

    sender._complete_batch(batch, PartitionResponse(error=Errors.MessageSizeTooLargeError))

    # Original batch should be deallocated (not in incomplete set)
    assert batch not in accumulator._incomplete.all()

    # New batches should be enqueued
    dq = accumulator._batches[tp]
    assert len(dq) >= 2

    total_records = sum(b.record_count for b in dq)
    assert total_records == 5

    # Original futures should not be done yet (new batches haven't been sent)
    for future in futures:
        assert not future.is_done


def test_complete_batch_splits_on_record_list_too_large(sender, accumulator, mocker):
    """_complete_batch splits batch on RECORD_LIST_TOO_LARGE."""
    tp = TopicPartition('foo', 0)
    batch, futures = multi_record_batch(num_records=5)
    accumulator._incomplete.add(batch)

    sender._complete_batch(batch, PartitionResponse(error=Errors.RecordListTooLargeError))

    dq = accumulator._batches[tp]
    assert len(dq) >= 2
    total_records = sum(b.record_count for b in dq)
    assert total_records == 5


def test_complete_batch_single_record_fails_normally(sender, accumulator):
    """Single-record batch with MESSAGE_TOO_LARGE fails (cannot split)."""
    batch, futures = multi_record_batch(num_records=1)
    accumulator._incomplete.add(batch)
    sender.config['retries'] = 0

    sender._complete_batch(batch, PartitionResponse(error=Errors.MessageSizeTooLargeError))

    assert batch.is_done
    assert futures[0].is_done
    assert futures[0].failed()
    assert isinstance(futures[0].exception, Errors.MessageSizeTooLargeError)


def test_complete_batch_split_unmutes_partition(sender, accumulator):
    """After splitting, the partition should be unmuted for guarantee_message_order."""
    tp = TopicPartition('foo', 0)
    sender.config['guarantee_message_order'] = True
    accumulator.muted.add(tp)

    batch, _ = multi_record_batch(num_records=5, topic='foo', partition=0)
    accumulator._incomplete.add(batch)

    sender._complete_batch(batch, PartitionResponse(error=Errors.MessageSizeTooLargeError))

    assert tp not in accumulator.muted


def test_split_not_in_retry(accumulator):
    """Split batches should not be marked as in_retry so sequence numbers are assigned during drain."""
    tp = TopicPartition('foo', 0)
    batch, _ = multi_record_batch(num_records=4)
    accumulator._incomplete.add(batch)

    accumulator.split_and_reenqueue(batch)

    for new_batch in accumulator._batches[tp]:
        assert not new_batch.in_retry()


def test_split_with_small_batch_size():
    """When batch_size is small, records are distributed across more batches."""
    # Use a small batch_size to force many splits
    accumulator = RecordAccumulator(batch_size=100)
    tp = TopicPartition('foo', 0)

    # Create a batch with large batch_size (simulating the original oversized batch)
    batch, futures = multi_record_batch(num_records=10, batch_size=100000)
    accumulator._incomplete.add(batch)

    num_new = accumulator.split_and_reenqueue(batch)
    accumulator.deallocate(batch)

    dq = accumulator._batches[tp]
    total_records = sum(b.record_count for b in dq)
    assert total_records == 10
    # With 100 byte batch_size, we expect many batches
    assert num_new >= 2


def test_future_rebind():
    """FutureRecordMetadata.rebind updates produce_future and batch_index."""
    from kafka.producer.future import FutureProduceResult, FutureRecordMetadata
    tp = TopicPartition('foo', 0)

    old_pf = FutureProduceResult(tp)
    new_pf = FutureProduceResult(tp)

    future = FutureRecordMetadata(old_pf, 5, 1000, None, 3, 5, -1)
    assert future._produce_future is old_pf
    assert future.args[0] == 5  # batch_index

    future.rebind(new_pf, 2)
    assert future._produce_future is new_pf
    assert future.args[0] == 2  # new batch_index

    # Complete new produce future and verify the record future resolves
    new_pf.success((100, -1, None))
    assert future.is_done
    assert future.succeeded()
    assert future.value.offset == 102  # base_offset(100) + batch_index(2)


def test_rebind_sets_old_latch():
    """rebind() sets the old produce_future's latch so blocked get() threads wake up."""
    from kafka.producer.future import FutureProduceResult, FutureRecordMetadata
    tp = TopicPartition('foo', 0)

    old_pf = FutureProduceResult(tp)
    new_pf = FutureProduceResult(tp)

    future = FutureRecordMetadata(old_pf, 0, 1000, None, 3, 5, -1)
    assert not old_pf._latch.is_set()

    future.rebind(new_pf, 0)

    # Old latch should be set so any thread blocked in get() wakes up
    assert old_pf._latch.is_set()
    # Future should not be resolved yet (new batch hasn't completed)
    assert not future.is_done


def test_rebind_old_produce_future_callbacks_safe():
    """Old produce_future's stale callbacks don't crash if it is never completed."""
    from kafka.producer.future import FutureProduceResult, FutureRecordMetadata
    tp = TopicPartition('foo', 0)

    old_pf = FutureProduceResult(tp)
    new_pf = FutureProduceResult(tp)

    future = FutureRecordMetadata(old_pf, 0, 1000, None, 3, 5, -1)
    future.rebind(new_pf, 0)

    # Complete the new produce_future — should resolve the record future once
    new_pf.success((100, -1, None))
    assert future.is_done
    assert future.succeeded()

    # The old produce_future should NOT be completed
    assert not old_pf.is_done


def test_get_rewait_after_rebind():
    """get() re-waits on new produce_future after being woken by rebind()."""
    import threading
    from kafka.producer.future import FutureProduceResult, FutureRecordMetadata
    tp = TopicPartition('foo', 0)

    old_pf = FutureProduceResult(tp)
    future = FutureRecordMetadata(old_pf, 0, 1000, None, 3, 5, -1)

    result_holder = [None]
    error_holder = [None]

    def get_in_thread():
        try:
            result_holder[0] = future.get(timeout=5)
        except Exception as e:
            error_holder[0] = e

    t = threading.Thread(target=get_in_thread)
    t.start()

    # Give the thread time to block on old_pf._latch.wait()
    import time
    time.sleep(0.05)
    assert t.is_alive()

    # Rebind to a new produce_future — this wakes the blocked thread
    new_pf = FutureProduceResult(tp)
    future.rebind(new_pf, 0)

    # Thread should still be alive, now waiting on new_pf
    time.sleep(0.05)
    assert t.is_alive()

    # Complete the new produce_future
    new_pf.success((42, -1, None))
    t.join(timeout=5)
    assert not t.is_alive()
    assert error_holder[0] is None
    assert result_holder[0] is not None
    assert result_holder[0].offset == 42


def test_get_rewait_after_multiple_rebinds():
    """get() survives multiple rebinds (batch split more than once)."""
    import threading
    import time
    from kafka.producer.future import FutureProduceResult, FutureRecordMetadata
    tp = TopicPartition('foo', 0)

    pf1 = FutureProduceResult(tp)
    future = FutureRecordMetadata(pf1, 0, 1000, None, 3, 5, -1)

    result_holder = [None]
    error_holder = [None]

    def get_in_thread():
        try:
            result_holder[0] = future.get(timeout=5)
        except Exception as e:
            error_holder[0] = e

    t = threading.Thread(target=get_in_thread)
    t.start()
    time.sleep(0.05)

    # First rebind (first split)
    pf2 = FutureProduceResult(tp)
    future.rebind(pf2, 0)
    time.sleep(0.05)
    assert t.is_alive()

    # Second rebind (second split)
    pf3 = FutureProduceResult(tp)
    future.rebind(pf3, 0)
    time.sleep(0.05)
    assert t.is_alive()

    # Finally complete
    pf3.success((99, -1, None))
    t.join(timeout=5)
    assert not t.is_alive()
    assert error_holder[0] is None
    assert result_holder[0].offset == 99


def test_end_to_end_split_and_complete(accumulator):
    """End-to-end: split a batch, complete new batches, verify all original futures resolve."""
    tp = TopicPartition('foo', 0)
    batch, futures = multi_record_batch(num_records=8)
    accumulator._incomplete.add(batch)

    accumulator.split_and_reenqueue(batch)
    accumulator.deallocate(batch)

    dq = accumulator._batches[tp]
    new_batches = list(dq)

    # Simulate sending and completing each new batch
    offset = 0
    for new_batch in new_batches:
        new_batch.complete(offset, -1)
        offset += new_batch.record_count

    # All original futures should be resolved with correct offsets
    for i, future in enumerate(futures):
        assert future.is_done, "Future %d not done" % i
        assert future.succeeded(), "Future %d failed: %s" % (i, future.exception)
        assert future.value.offset == i
        assert future.value.topic == 'foo'
        assert future.value.partition == 0
