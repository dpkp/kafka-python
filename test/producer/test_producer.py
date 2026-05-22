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
