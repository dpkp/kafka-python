from __future__ import absolute_import

import pytest
import time
from contextlib import contextmanager

from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError

@contextmanager
def producer_factory(**kwargs):
    # Set a proper api_version if not provided
    if 'api_version' not in kwargs:
        kwargs['api_version'] = (1, 0, 0)
    producer = KafkaProducer(**kwargs)
    try:
        yield producer
    finally:
        producer.close(timeout=1)

@pytest.mark.integration
class TestKafkaProducerIntegrationAcks(object):
    """Test various acks configurations for KafkaProducer.

    These tests verify the behavior of different acknowledgment settings:
    - acks=0: Fire and forget (no guarantee of delivery, highest throughput)
    - acks=1: Leader acknowledgment (message persisted to leader, but might be lost if leader fails)
    - acks='all': Full cluster acknowledgment (message persisted to all in-sync replicas, highest durability)

    The tests verify both the configuration values and the runtime behavior under different conditions.
    """

    @pytest.fixture
    def topic(self, kafka_broker):
        return kafka_broker.create_topic("test-acks-topic", replication_factor=3)

    @pytest.fixture
    def producer_config(self, kafka_broker, metadata_timeout_ms, acks_test_timeout):
        return {
            'bootstrap_servers': kafka_broker.bootstrap_server,
            'api_version': (1, 0, 0),  # Use a consistent version
            'request_timeout_ms': acks_test_timeout,
            'metadata_max_age_ms': metadata_timeout_ms,
            'max_block_ms': metadata_timeout_ms
        }

    @pytest.fixture
    def acks_test_timeout(self):
        # Default timeout for most acks tests (in milliseconds)
        return 30000  # 30 seconds

    def test_acks_none(self, kafka_broker, topic, producer_config):
        """Test that producer with acks=0 doesn't wait for acknowledgments."""
        # Create a producer with acks=0 (no acknowledgment)
        with producer_factory(
            **producer_config,
            acks=0,
            retries=0,  # No retries to verify behavior
            batch_size=1,  # Small batch size to ensure immediate sending
            linger_ms=0  # No linger, send immediately
        ) as producer:
            # Send a message without waiting for any acknowledgment
            future = producer.send(topic, value=b'test-acks-0')

            # With acks=0, future should be resolved immediately
            # without waiting for server response
            result = future.get(timeout=5)
            assert result is not None
            # With acks=0, the offset should be set to -1
            assert result.offset == -1

    def test_acks_local_write(self, kafka_broker, topic, producer_config):
        """Test that producer with acks=1 waits for leader acknowledgment."""
        # Create a producer with acks=1 (leader acknowledgment)
        with producer_factory(
            **producer_config,
            acks=1,
            retries=0,
            batch_size=1,
            linger_ms=0
        ) as producer:
            # Send a message and wait for leader acknowledgment
            future = producer.send(topic, value=b'test-acks-1')

            # With acks=1, we should get a proper offset from the broker
            result = future.get(timeout=5)
            assert result is not None
            assert result.offset >= 0

    def test_acks_cluster_commit(self, kafka_broker, topic, producer_config):
        """Test that producer with acks='all' waits for all replicas to acknowledge."""
        # Create a producer with acks='all' (all replicas acknowledgment)
        with producer_factory(
            **producer_config,
            acks='all',
            retries=0,
            batch_size=1,
            linger_ms=0
        ) as producer:
            # Send a message and wait for all replicas to acknowledge
            future = producer.send(topic, value=b'test-acks-all')

            # With acks='all', we should also get a proper offset
            result = future.get(timeout=5)
            assert result is not None
            assert result.offset >= 0

    def test_acks_timeout_behavior(self, kafka_broker, topic, producer_config):
        """Test how different acks values behave under timeout conditions."""
        # This test simulates high latency scenarios by setting a very short timeout
        # We expect acks='all' to be more likely to timeout since it needs acknowledgment from all replicas

        # Set an extremely short timeout to simulate slow replicas
        short_timeout = 1  # 1ms timeout - likely to trigger timeout with acks='all'

        # Test with acks=0 (should never timeout as it doesn't wait for acks)
        with producer_factory(
            **producer_config,
            acks=0,
            request_timeout_ms=short_timeout,
            retries=0
        ) as producer:
            # Even with a short timeout, acks=0 should never fail
            future = producer.send(topic, value=b'timeout-test-acks-0')
            # Should complete without timeout
            future.get(timeout=5)

        # Test with acks=1 (might timeout, but less likely than 'all')
        with producer_factory(
            **producer_config,
            acks=1,
            request_timeout_ms=short_timeout,
            retries=0
        ) as producer:
            future = producer.send(topic, value=b'timeout-test-acks-1')
            # May or may not timeout, we just observe the behavior
            try:
                future.get(timeout=5)
                acks1_timeout = False
            except KafkaTimeoutError:
                acks1_timeout = True

        # Test with acks='all' (most likely to timeout)
        with producer_factory(
            **producer_config,
            acks='all',
            request_timeout_ms=short_timeout,
            retries=0
        ) as producer:
            future = producer.send(topic, value=b'timeout-test-acks-all')
            # Most likely to timeout since it needs all replicas to acknowledge
            try:
                future.get(timeout=5)
                acksall_timeout = False
            except KafkaTimeoutError:
                acksall_timeout = True

        # We can't guarantee timeouts in all environments, but we can log what happened
        print(f"\nTimeout behavior with {short_timeout}ms request timeout:")
        print(f"acks=1 timed out: {acks1_timeout}")
        print(f"acks='all' timed out: {acksall_timeout}")

        # The main assertion is that acks=0 should never timeout
        # For acks=1 and acks='all', we just observe the behavior

    def test_acks_effect_on_durability(self, kafka_broker, topic):
        """Test how different acks values affect message durability."""
        # This is a test template for future implementation.
        # Testing actual durability would require network partitioning 
        # or broker failure scenarios which are difficult to simulate 
        # in standard integration tests.

        # A full test would include:
        # 1. Send messages with acks=0
        # 2. Cause immediate broker failure
        # 3. Verify if messages were lost
        # 4. Repeat with acks=1 and acks='all'

        # For now, we're testing the configuration values and measuring performance differences
        # between different acks settings, which indirectly relate to durability guarantees

        # Test acks=0 (fastest, no durability guarantee)
        start_time = time.time()
        with producer_factory(
            bootstrap_servers=kafka_broker.bootstrap_server,
            acks=0,
            batch_size=1,
            linger_ms=0
        ) as producer:
            assert producer.config['acks'] == 0
            # Send and measure time with no acknowledgment
            future = producer.send(topic, value=b'test-durability-acks-0')
            # With acks=0, future should complete immediately without server acknowledgment
            result = future.get(timeout=5)
            assert result.offset == -1  # acks=0 always returns -1 for offset
        acks0_time = time.time() - start_time

        # Test acks=1 (medium durability - leader acknowledgment)
        start_time = time.time()
        with producer_factory(
            bootstrap_servers=kafka_broker.bootstrap_server,
            acks=1,
            batch_size=1,
            linger_ms=0
        ) as producer:
            assert producer.config['acks'] == 1
            # Send and measure time with leader acknowledgment
            future = producer.send(topic, value=b'test-durability-acks-1')
            result = future.get(timeout=5)
            assert result.offset >= 0  # Should get a real offset
        acks1_time = time.time() - start_time

        # Test acks='all' (highest durability - all replicas acknowledge)
        start_time = time.time()
        with producer_factory(
            bootstrap_servers=kafka_broker.bootstrap_server,
            acks='all',
            batch_size=1,
            linger_ms=0
        ) as producer:
            assert producer.config['acks'] == -1  # 'all' is represented as -1 internally
            # Send and measure time with all-replicas acknowledgment
            future = producer.send(topic, value=b'test-durability-acks-all')
            result = future.get(timeout=5)
            assert result.offset >= 0  # Should get a real offset
        acksall_time = time.time() - start_time

        # Verify the performance implications of different acks settings
        # Generally: acks=0 < acks=1 < acks='all' in terms of latency
        # But we can't guarantee exact ordering in all test environments
        # so we just log the values for observation
        print(f"\nPerformance comparison of different acks settings:")
        print(f"acks=0 time: {acks0_time:.6f}s")
        print(f"acks=1 time: {acks1_time:.6f}s")
        print(f"acks='all' time: {acksall_time:.6f}s")