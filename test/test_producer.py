from __future__ import absolute_import

import gc
import platform
import threading

import pytest

from kafka import KafkaProducer
from kafka.cluster import ClusterMetadata
from kafka.producer.transaction_manager import TransactionManager, ProducerIdAndEpoch


def test_kafka_producer_thread_close():
    threads = threading.active_count()
    producer = KafkaProducer(api_version=(2, 1)) # set api_version explicitly to avoid auto-detection
    assert threading.active_count() == threads + 1
    producer.close()
    assert threading.active_count() == threads


def test_idempotent_producer_reset_producer_id():
    transaction_manager = TransactionManager(
        transactional_id=None,
        transaction_timeout_ms=1000,
        retry_backoff_ms=100,
        api_version=(0, 11),
        metadata=ClusterMetadata(),
    )

    test_producer_id_and_epoch = ProducerIdAndEpoch(123, 456)
    transaction_manager.set_producer_id_and_epoch(test_producer_id_and_epoch)
    assert transaction_manager.producer_id_and_epoch == test_producer_id_and_epoch
    transaction_manager.reset_producer_id()
    assert transaction_manager.producer_id_and_epoch == ProducerIdAndEpoch(-1, -1)
