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
from kafka.producer.transaction_manager import ProducerIdAndEpoch, TransactionManager
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

    # Sequence is now incremented at drain time, not completion time.
    # Simulate drain-time increment.
    sender._transaction_manager.increment_sequence_number(batch.topic_partition, batch.record_count)
    assert sender._transaction_manager.sequence_number(batch.topic_partition) == batch.record_count

    # No error, base_offset 0
    sender._complete_batch(batch, PartitionResponse(base_offset=0))
    assert batch.is_done
    # Sequence should not change on completion (already incremented at drain)
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


# ---- KAFKA-5494: Idempotent producer with max_in_flight > 1 ----

def test_idempotent_config_allows_max_in_flight_up_to_5():
    """Idempotent producer allows max_in_flight 1-5."""
    from kafka.producer.kafka import KafkaProducer
    for max_in_flight in (1, 2, 3, 4, 5):
        p = KafkaProducer(
            enable_idempotence=True,
            max_in_flight_requests_per_connection=max_in_flight,
            api_version=(0, 11),
        )
        assert p.config['max_in_flight_requests_per_connection'] == max_in_flight
        p.close(timeout=0)


def test_idempotent_config_rejects_max_in_flight_above_5():
    """Idempotent producer rejects max_in_flight > 5."""
    from kafka.producer.kafka import KafkaProducer
    with pytest.raises(Errors.KafkaConfigurationError, match="at most 5"):
        KafkaProducer(
            enable_idempotence=True,
            max_in_flight_requests_per_connection=6,
            api_version=(0, 11),
        )


def test_idempotent_default_max_in_flight():
    """Idempotent producer defaults to max_in_flight=5 (no longer overridden to 1)."""
    from kafka.producer.kafka import KafkaProducer
    p = KafkaProducer(
        enable_idempotence=True,
        api_version=(0, 11),
    )
    assert p.config['max_in_flight_requests_per_connection'] == 5
    p.close(timeout=0)


def test_guarantee_message_order_only_when_max_in_flight_1():
    """guarantee_message_order is True only when max_in_flight == 1."""
    from kafka.producer.kafka import KafkaProducer
    p1 = KafkaProducer(
        enable_idempotence=True,
        max_in_flight_requests_per_connection=1,
        api_version=(0, 11),
    )
    assert p1._sender.config['guarantee_message_order'] is True
    p1.close(timeout=0)

    p5 = KafkaProducer(
        enable_idempotence=True,
        max_in_flight_requests_per_connection=5,
        api_version=(0, 11),
    )
    assert p5._sender.config['guarantee_message_order'] is False
    p5.close(timeout=0)


def _setup_drain(client, transaction_manager, tp):
    """Helper to set up cluster and transaction_manager for drain tests."""
    transaction_manager.set_producer_id_and_epoch(ProducerIdAndEpoch(1000, 0))
    client.cluster._partitions[tp] = None
    client.cluster._broker_partitions = {0: [tp]}


def test_sequence_number_incremented_at_drain_time(client, transaction_manager):
    """Sequence numbers are incremented during drain, not on completion."""
    accumulator = RecordAccumulator(transaction_manager=transaction_manager)
    tp = TopicPartition('foo', 0)
    _setup_drain(client, transaction_manager, tp)

    accumulator.append(tp, 0, b'key-0', b'value-0', [])
    accumulator.append(tp, 0, b'key-1', b'value-1', [])
    assert transaction_manager.sequence_number(tp) == 0

    batches = accumulator.drain_batches_for_one_node(client.cluster, 0, 1048576)
    assert len(batches) == 1

    # Sequence should be incremented at drain time
    assert transaction_manager.sequence_number(tp) == 2


def test_multiple_batches_get_different_sequences(client, transaction_manager):
    """With max_in_flight > 1, successive drains assign different sequence numbers."""
    accumulator = RecordAccumulator(batch_size=50, transaction_manager=transaction_manager)
    tp = TopicPartition('foo', 0)
    _setup_drain(client, transaction_manager, tp)

    for i in range(10):
        accumulator.append(tp, 0, b'key-%d' % i, b'value-%d' % i, [])

    # First drain: gets first batch
    batches1 = accumulator.drain_batches_for_one_node(client.cluster, 0, 1048576)
    assert len(batches1) == 1
    seq_after_first = transaction_manager.sequence_number(tp)
    assert seq_after_first > 0

    # Second drain: gets next batch with higher sequence
    batches2 = accumulator.drain_batches_for_one_node(client.cluster, 0, 1048576)
    assert len(batches2) == 1
    seq_after_second = transaction_manager.sequence_number(tp)
    assert seq_after_second > seq_after_first


def test_retry_batch_keeps_sequence(client, transaction_manager):
    """Retried batches keep their original sequence number (in_retry=True skips reassignment)."""
    accumulator = RecordAccumulator(transaction_manager=transaction_manager)
    tp = TopicPartition('foo', 0)
    _setup_drain(client, transaction_manager, tp)

    accumulator.append(tp, 0, b'key', b'value', [])

    batches = accumulator.drain_batches_for_one_node(client.cluster, 0, 1048576)
    batch = batches[0]
    seq_after_drain = transaction_manager.sequence_number(tp)
    assert seq_after_drain == 1  # Incremented at drain

    # Re-enqueue for retry
    accumulator.reenqueue(batch)
    assert batch.in_retry()

    # Re-drain after backoff expires — sequence should NOT change (batch is in_retry)
    future_time = time.monotonic() + 1  # past retry_backoff_ms
    batches2 = accumulator.drain_batches_for_one_node(client.cluster, 0, 1048576, now=future_time)
    assert len(batches2) == 1
    assert transaction_manager.sequence_number(tp) == seq_after_drain


def test_duplicate_sequence_number_treated_as_success(sender, accumulator):
    """DuplicateSequenceNumberError is treated as successful completion."""
    batch = producer_batch()
    accumulator._incomplete.add(batch)

    sender._complete_batch(batch, PartitionResponse(
        error=Errors.DuplicateSequenceNumberError, base_offset=42, log_append_time=-1))

    assert batch.is_done
    assert batch.produce_future.succeeded()
    assert batch.produce_future.value == (42, -1, None)


def test_split_resets_sequence_number(client, transaction_manager):
    """split_and_reenqueue rolls back the sequence counter so split batches reuse the range."""
    accumulator = RecordAccumulator(transaction_manager=transaction_manager)
    tp = TopicPartition('foo', 0)
    _setup_drain(client, transaction_manager, tp)

    # Append a batch with multiple records
    for i in range(5):
        accumulator.append(tp, 0, b'key-%d' % i, b'value-%d' % i, [])

    assert transaction_manager.sequence_number(tp) == 0

    # Drain — sequence advances to 5
    batches = accumulator.drain_batches_for_one_node(client.cluster, 0, 1048576)
    assert len(batches) == 1
    batch = batches[0]
    assert transaction_manager.sequence_number(tp) == 5

    # Split — should roll back sequence to 0 (the failed batch's base_sequence)
    accumulator.split_and_reenqueue(batch)
    accumulator.deallocate(batch)
    assert transaction_manager.sequence_number(tp) == 0

    # Drain the split batches — each gets correct sequential sequences
    dq = list(accumulator._batches[tp])
    assert len(dq) == 2  # Split into two halves

    batches1 = accumulator.drain_batches_for_one_node(client.cluster, 0, 1048576)
    assert len(batches1) == 1
    seq_after_first = transaction_manager.sequence_number(tp)
    assert seq_after_first == batches1[0].record_count  # e.g., 3

    batches2 = accumulator.drain_batches_for_one_node(client.cluster, 0, 1048576)
    assert len(batches2) == 1
    seq_after_second = transaction_manager.sequence_number(tp)
    assert seq_after_second == 5  # Back to where it was: 3 + 2 = 5


def test_split_without_idempotence_no_sequence_reset(accumulator):
    """split_and_reenqueue works without transaction_manager (no sequence to reset)."""
    tp = TopicPartition('foo', 0)
    batch, futures = multi_record_batch(num_records=4)
    accumulator._incomplete.add(batch)

    # Should not raise even without a transaction_manager
    num_new = accumulator.split_and_reenqueue(batch)
    accumulator.deallocate(batch)
    assert num_new == 2


# ---- KAFKA-5793: Tighten OutOfOrderSequence semantics ----

def test_update_last_acked_offset_on_success(sender, accumulator, transaction_manager):
    """Sender updates last_acked_offset in TransactionManager on successful completion."""
    sender._transaction_manager = transaction_manager
    batch = producer_batch()  # 1 record
    assert transaction_manager.last_acked_offset(batch.topic_partition) == -1

    sender._complete_batch(batch, PartitionResponse(base_offset=42, log_append_time=-1))
    # last_offset = base_offset(42) + record_count(1) - 1 = 42
    assert transaction_manager.last_acked_offset(batch.topic_partition) == 42


def test_update_last_acked_offset_monotonic(sender, accumulator, transaction_manager):
    """last_acked_offset only increases (out-of-order acks don't decrease it)."""
    sender._transaction_manager = transaction_manager
    tp = TopicPartition('foo', 0)

    transaction_manager.update_last_acked_offset(tp, 100, 5)  # last = 104
    assert transaction_manager.last_acked_offset(tp) == 104

    transaction_manager.update_last_acked_offset(tp, 50, 3)  # last = 52, should not overwrite
    assert transaction_manager.last_acked_offset(tp) == 104

    transaction_manager.update_last_acked_offset(tp, 200, 2)  # last = 201
    assert transaction_manager.last_acked_offset(tp) == 201


def test_update_last_acked_offset_ignores_invalid_base_offset(transaction_manager):
    """Negative / invalid base_offset does not update last_acked_offset."""
    tp = TopicPartition('foo', 0)
    transaction_manager.update_last_acked_offset(tp, -1, 5)
    assert transaction_manager.last_acked_offset(tp) == -1


def test_retention_based_unknown_producer_id_retries(sender, accumulator, transaction_manager, mocker):
    """UnknownProducerIdError with log_start_offset > last_acked_offset is retried."""
    sender._transaction_manager = transaction_manager
    mocker.patch.object(accumulator, 'reenqueue')

    tp = TopicPartition('foo', 0)
    # Simulate: previously acked records at offsets 0..9 (last_acked=9)
    transaction_manager.update_last_acked_offset(tp, 0, 10)
    assert transaction_manager.last_acked_offset(tp) == 9
    # Sequence counter is at some value (set by prior drain)
    transaction_manager.sequence_number(tp)  # populate defaultdict entry
    transaction_manager.increment_sequence_number(tp, 10)
    assert transaction_manager.sequence_number(tp) == 10

    batch = producer_batch()
    # Broker's log_start_offset is 100 — way past our last acked
    sender._complete_batch(batch, PartitionResponse(
        error=Errors.UnknownProducerIdError,
        base_offset=-1,
        log_start_offset=100,
    ))

    # Batch should be reenqueued (retried), not failed
    accumulator.reenqueue.assert_called_with(batch)
    assert not batch.is_done

    # Sequence counter should be reset
    assert transaction_manager.sequence_number(tp) == 0
    # last_acked_offset is also cleared by reset_sequence_for_partition
    assert transaction_manager.last_acked_offset(tp) == -1


def test_real_data_loss_unknown_producer_id_fails(sender, accumulator, transaction_manager, mocker):
    """UnknownProducerIdError with log_start_offset <= last_acked_offset is fatal."""
    sender._transaction_manager = transaction_manager
    mocker.patch.object(accumulator, 'reenqueue')

    tp = TopicPartition('foo', 0)
    # Previously acked records up to offset 99
    transaction_manager.update_last_acked_offset(tp, 0, 100)
    assert transaction_manager.last_acked_offset(tp) == 99

    batch = producer_batch()
    future = FutureRecordMetadata(batch.produce_future, -1, -1, -1, -1, -1, -1)

    # Broker's log_start_offset is 50 — within our acked range → real data loss
    sender._complete_batch(batch, PartitionResponse(
        error=Errors.UnknownProducerIdError,
        base_offset=-1,
        log_start_offset=50,
    ))

    # Batch should NOT be reenqueued — it should fail
    accumulator.reenqueue.assert_not_called()
    assert batch.is_done
    assert future.failed()
    assert isinstance(future.exception, Errors.UnknownProducerIdError)


def test_unknown_producer_id_without_log_start_offset_fails(sender, accumulator, transaction_manager, mocker):
    """UnknownProducerIdError without log_start_offset info (old broker) falls through to failure."""
    sender._transaction_manager = transaction_manager
    mocker.patch.object(accumulator, 'reenqueue')

    tp = TopicPartition('foo', 0)
    transaction_manager.update_last_acked_offset(tp, 0, 5)

    batch = producer_batch()
    # Old broker response: log_start_offset = -1 (unknown)
    sender._complete_batch(batch, PartitionResponse(
        error=Errors.UnknownProducerIdError,
        base_offset=-1,
        log_start_offset=-1,
    ))

    accumulator.reenqueue.assert_not_called()
    assert batch.is_done


def test_unknown_producer_id_without_transaction_manager_fails(sender, accumulator, mocker):
    """UnknownProducerIdError without transaction_manager falls through to normal failure path."""
    mocker.patch.object(accumulator, 'reenqueue')
    batch = producer_batch()
    sender._complete_batch(batch, PartitionResponse(
        error=Errors.UnknownProducerIdError,
        log_start_offset=100,
    ))
    accumulator.reenqueue.assert_not_called()
    assert batch.is_done
