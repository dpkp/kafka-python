from __future__ import absolute_import

import pytest
from unittest import mock

from kafka import KafkaProducer
from kafka.producer.future import FutureProduceResult, FutureRecordMetadata
from kafka.producer.kafka import TopicPartition


class TestErrorOnCallbacks(object):
    """Test error_on_callbacks behavior in KafkaProducer."""

    def test_future_error_on_callbacks_default(self):
        """Test the default behavior (suppress errors) in callbacks."""
        future = FutureProduceResult(TopicPartition('test_topic', 0))

        # Verify default is False
        assert future.error_on_callbacks is False

        # Create a callback that raises an exception
        def error_callback(value):
            raise ValueError("Test error")

        # Add the callback and trigger it
        future.add_callback(error_callback)
        # This should not raise an exception (just log it)
        future.success("test value")

    def test_future_error_on_callbacks_enabled(self):
        """Test that enabling error_on_callbacks raises exceptions in callbacks."""
        future = FutureProduceResult(TopicPartition('test_topic', 0))

        # Set error_on_callbacks to True
        future.error_on_callbacks = True

        # Create a callback that raises an exception
        def error_callback(value):
            raise ValueError("Test error")

        # Add the callback
        future.add_callback(error_callback)

        # This should raise the exception
        with pytest.raises(ValueError, match="Test error"):
            future.success("test value")

    def test_producer_error_on_callbacks_constructor(self):
        """Test setting error_on_callbacks via producer constructor."""
        with mock.patch('kafka.producer.kafka.KafkaClient'):
            # Create a producer with error_on_callbacks=True
            producer = KafkaProducer(
                bootstrap_servers='localhost:9092',
                error_on_callbacks=True,
                api_version=(0, 10, 0)
            )

            # Verify the config value
            assert producer.config['error_on_callbacks'] is True

            # Mock the necessary parts to test send
            producer._wait_on_metadata = mock.MagicMock()
            producer._serialize = mock.MagicMock(return_value=b'')
            producer._partition = mock.MagicMock(return_value=0)

            # Create a future with error_on_callbacks=True
            future = FutureProduceResult(TopicPartition('test_topic', 0))
            future.error_on_callbacks = True

            # Mock _accumulator.append to return our future
            producer._accumulator.append = mock.MagicMock(return_value=(future, False, False))

            # Send a message
            result = producer.send('test_topic', value=b'test')

            # Verify the future has error_on_callbacks=True
            assert result.error_on_callbacks is True

    def test_producer_send_with_error_on_callbacks(self):
        """Test specifying error_on_callbacks in send method."""
        with mock.patch('kafka.producer.kafka.KafkaClient'):
            # Create a producer with default error_on_callbacks=False
            producer = KafkaProducer(
                bootstrap_servers='localhost:9092',
                api_version=(0, 10, 0)
            )

            # Mock the necessary parts to test send
            producer._wait_on_metadata = mock.MagicMock()
            producer._serialize = mock.MagicMock(return_value=b'')
            producer._partition = mock.MagicMock(return_value=0)

            # Create a future that will receive error_on_callbacks=True
            future = FutureProduceResult(TopicPartition('test_topic', 0))

            # Mock _accumulator.append to return our future
            producer._accumulator.append = mock.MagicMock(return_value=(future, False, False))

            # Send a message with error_on_callbacks=True
            result = producer.send('test_topic', value=b'test', error_on_callbacks=True)

            # Verify the future has error_on_callbacks=True
            assert result.error_on_callbacks is True

            # Reset the mock and create a new future for another test
            future = FutureProduceResult(TopicPartition('test_topic', 0))
            producer._accumulator.append = mock.MagicMock(return_value=(future, False, False))

            # Send a message with error_on_callbacks=False (explicit)
            result = producer.send('test_topic', value=b'test', error_on_callbacks=False)

            # Verify the future has error_on_callbacks=False
            assert result.error_on_callbacks is False
