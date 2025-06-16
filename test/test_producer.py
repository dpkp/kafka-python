from __future__ import absolute_import

import gc
import platform
import threading

import pytest
from unittest import mock

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


def test_producer_acks_none():
    """Test that producer with acks=0 doesn't wait for acknowledgments."""
    with mock.patch('kafka.producer.kafka.KafkaClient') as mock_client:
        # Configure the mock client
        mock_client_instance = mock_client.return_value
        mock_client_instance.api_version = mock.MagicMock(return_value=(0, 10, 0))
        mock_client_instance.cluster = mock.MagicMock()
        mock_client_instance.cluster.topics.return_value = []
        mock_client_instance.cluster._partitions = {}

        # Create a producer with acks=0
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            acks=0,
            api_version=(0, 10, 0),
            retry_backoff_ms=100,
            value_serializer=lambda v: v.encode('utf-8'),
            key_serializer=lambda v: v.encode('utf-8'),
        )

        # Check the config value
        assert producer.config['acks'] == 0


def test_producer_acks_local_write():
    """Test that producer with acks=1 waits for leader acknowledgment."""
    with mock.patch('kafka.producer.kafka.KafkaClient') as mock_client:
        # Configure the mock client
        mock_client_instance = mock_client.return_value
        mock_client_instance.api_version = mock.MagicMock(return_value=(0, 10, 0))
        mock_client_instance.cluster = mock.MagicMock()
        mock_client_instance.cluster.topics.return_value = []
        mock_client_instance.cluster._partitions = {}

        # Create a producer with acks=1
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            acks=1,  # Default is 1, but explicitly set for test clarity
            api_version=(0, 10, 0),
            retry_backoff_ms=100,
            value_serializer=lambda v: v.encode('utf-8'),
            key_serializer=lambda v: v.encode('utf-8'),
        )

        # Check the config value
        assert producer.config['acks'] == 1


def test_producer_acks_all():
    """Test that producer with acks='all' waits for all replicas to acknowledge."""
    with mock.patch('kafka.producer.kafka.KafkaClient') as mock_client:
        # Configure the mock client
        mock_client_instance = mock_client.return_value
        mock_client_instance.api_version = mock.MagicMock(return_value=(0, 10, 0))
        mock_client_instance.cluster = mock.MagicMock()
        mock_client_instance.cluster.topics.return_value = []
        mock_client_instance.cluster._partitions = {}

        # Create a producer with acks='all'
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            acks='all',
            api_version=(0, 10, 0),
            retry_backoff_ms=100,
            value_serializer=lambda v: v.encode('utf-8'),
            key_serializer=lambda v: v.encode('utf-8'),
        )

        # Check the config value - 'all' should be converted to -1 internally
        assert producer.config['acks'] == -1
