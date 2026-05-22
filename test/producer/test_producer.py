import gc
import platform
import threading
from unittest.mock import MagicMock

import pytest

from kafka import KafkaProducer
from kafka.partitioner import DefaultPartitioner, StickyPartitioner
from kafka.producer.transaction_manager import TransactionManager, ProducerIdAndEpoch


def test_kafka_producer_thread_close():
    threads = threading.active_count()
    producer = KafkaProducer(api_version=(2, 1)) # set api_version explicitly to avoid auto-detection
    assert threading.active_count() == threads + 1
    producer.close()
    assert threading.active_count() == threads


def test_kafka_producer_context_manager_closes_on_exit():
    threads = threading.active_count()
    with KafkaProducer(api_version=(2, 1)) as producer:
        assert threading.active_count() == threads + 1
        assert producer._closed is False
    assert producer._closed is True
    assert threading.active_count() == threads


def test_partition_uses_topic_aware_api_when_available():
    """_partition routes through partitioner.partition(topic, ...) when
    the configured partitioner exposes it (KIP-480 sticky path)."""
    producer = KafkaProducer.__new__(KafkaProducer)
    producer._metadata = MagicMock()
    producer._metadata.partitions_for_topic.return_value = {0, 1, 2}
    producer._metadata.available_partitions_for_topic.return_value = {0, 1, 2}

    partitioner = MagicMock()
    partitioner.partition.return_value = 1
    producer.config = {'partitioner': partitioner}

    result = producer._partition('t', None, None, None, b'key-bytes', b'val')
    assert result == 1
    partitioner.partition.assert_called_once_with('t', b'key-bytes', [0, 1, 2], [0, 1, 2])


def test_partition_falls_back_to_legacy_callable():
    """Custom partitioners written against the legacy callable signature
    (no .partition method) keep working unchanged."""
    producer = KafkaProducer.__new__(KafkaProducer)
    producer._metadata = MagicMock()
    producer._metadata.partitions_for_topic.return_value = {0, 1, 2}
    producer._metadata.available_partitions_for_topic.return_value = {0, 1, 2}

    # A plain function - no .partition attribute - must still work.
    calls = []
    def legacy_partitioner(key, all_partitions, available):
        calls.append((key, all_partitions, available))
        return 2
    producer.config = {'partitioner': legacy_partitioner}

    result = producer._partition('t', None, None, None, b'k', b'v')
    assert result == 2
    assert calls == [(b'k', [0, 1, 2], [0, 1, 2])]


def _producer_for_send_test(partitioner):
    """Build a real KafkaProducer but replace the accumulator + sender
    with mocks so ``send()`` doesn't try to actually push data."""
    producer = KafkaProducer(api_version=(2, 1), partitioner=partitioner)
    producer._accumulator = MagicMock()
    producer._sender = MagicMock()
    producer._metadata = MagicMock()
    producer._metadata.partitions_for_topic.return_value = set(range(20))
    producer._metadata.available_partitions_for_topic.return_value = set(range(20))
    return producer


def _success_result():
    from kafka.producer.future import FutureRecordMetadata, FutureProduceResult
    from kafka.structs import TopicPartition
    return (FutureRecordMetadata(
                FutureProduceResult(TopicPartition('t', 0)),
                0, 0, 0, 0, 0, 0),
            False, True, False)


def test_send_null_key_triggers_on_new_batch_via_abort_retry():
    """KIP-480 Java-faithful flow: a null-key send whose accumulator has
    no in-progress batch must invoke ``partitioner.on_new_batch`` (rotate
    the sticky) and re-pick the partition before the actual append,
    matching KafkaProducer.doSend's abort-for-new-batch retry path."""
    partitioner = MagicMock(spec=['partition', 'on_new_batch'])
    partitioner.partition.side_effect = [3, 7]  # initial pick, post-rotate
    producer = _producer_for_send_test(partitioner)
    abort = (None, False, False, True)
    producer._accumulator.append.side_effect = [abort, _success_result()]

    try:
        producer.send('t', value=b'msg')
        # Initial pick + post-rotate re-pick.
        assert partitioner.partition.call_count == 2
        # on_new_batch fired exactly once, with the *initial* sticky.
        partitioner.on_new_batch.assert_called_once_with('t', sorted(range(20)), 3)
        # Two appends: first aborted, second landed the record on partition 7.
        assert producer._accumulator.append.call_count == 2
        second_call = producer._accumulator.append.call_args_list[1]
        tp_arg = second_call.args[0]
        assert tp_arg.partition == 7
    finally:
        producer.close(timeout=1)


def test_send_keyed_skips_on_new_batch():
    """Keyed records bypass the sticky abort-retry path — on_new_batch
    must not fire."""
    partitioner = MagicMock(spec=['partition', 'on_new_batch'])
    partitioner.partition.return_value = 0
    producer = _producer_for_send_test(partitioner)
    producer._accumulator.append.return_value = _success_result()

    try:
        producer.send('t', key=b'k', value=b'v')
        partitioner.on_new_batch.assert_not_called()
        # Keyed records pass abort_on_new_batch=False directly — one append.
        assert producer._accumulator.append.call_count == 1
        kwargs = producer._accumulator.append.call_args.kwargs
        assert kwargs.get('abort_on_new_batch') is False
    finally:
        producer.close(timeout=1)


def test_send_with_explicit_partition_skips_on_new_batch():
    """Explicit partition overrides the partitioner entirely — no
    rotation hook should fire."""
    partitioner = MagicMock(spec=['partition', 'on_new_batch'])
    producer = _producer_for_send_test(partitioner)
    producer._accumulator.append.return_value = _success_result()

    try:
        producer.send('t', value=b'v', partition=1)
        partitioner.partition.assert_not_called()
        partitioner.on_new_batch.assert_not_called()
        # Explicit partition also goes straight to abort_on_new_batch=False.
        kwargs = producer._accumulator.append.call_args.kwargs
        assert kwargs.get('abort_on_new_batch') is False
    finally:
        producer.close(timeout=1)


def test_idempotent_producer_reset_producer_id(cluster):
    transaction_manager = TransactionManager(
        transactional_id=None,
        transaction_timeout_ms=1000,
        retry_backoff_ms=100,
        api_version=(0, 11),
        metadata=cluster,
    )

    test_producer_id_and_epoch = ProducerIdAndEpoch(123, 456)
    transaction_manager.set_producer_id_and_epoch(test_producer_id_and_epoch)
    assert transaction_manager.producer_id_and_epoch == test_producer_id_and_epoch
    transaction_manager.reset_producer_id()
    assert transaction_manager.producer_id_and_epoch == ProducerIdAndEpoch(-1, -1)
