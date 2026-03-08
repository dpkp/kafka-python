from unittest.mock import patch, MagicMock
import pytest

from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaConfigurationError, IllegalStateError


def test_session_timeout_larger_than_request_timeout_raises():
    with pytest.raises(KafkaConfigurationError):
        KafkaConsumer(bootstrap_servers='localhost:9092', api_version=(0, 9), group_id='foo', session_timeout_ms=50000, request_timeout_ms=40000)


def test_fetch_max_wait_larger_than_request_timeout_raises():
    with pytest.raises(KafkaConfigurationError):
        KafkaConsumer(bootstrap_servers='localhost:9092', fetch_max_wait_ms=50000, request_timeout_ms=40000)


def test_request_timeout_larger_than_connections_max_idle_ms_raises():
    with pytest.raises(KafkaConfigurationError):
        KafkaConsumer(bootstrap_servers='localhost:9092', api_version=(0, 9), request_timeout_ms=50000, connections_max_idle_ms=40000)


def test_subscription_copy():
    consumer = KafkaConsumer('foo', api_version=(0, 10, 0))
    sub = consumer.subscription()
    assert sub is not consumer.subscription()
    assert sub == set(['foo'])
    sub.add('fizz')
    assert consumer.subscription() == set(['foo'])


def test_assign():
    # Consumer w/ subscription to topic 'foo'
    consumer = KafkaConsumer('foo', api_version=(0, 10, 0))
    assert consumer.assignment() == set()
    # Cannot assign manually
    with pytest.raises(IllegalStateError):
        consumer.assign([TopicPartition('foo', 0)])

    assert 'foo' in consumer._client._topics

    consumer = KafkaConsumer(api_version=(0, 10, 0))
    assert consumer.assignment() == set()
    consumer.assign([TopicPartition('foo', 0)])
    assert consumer.assignment() == set([TopicPartition('foo', 0)])
    assert 'foo' in consumer._client._topics
    # Cannot subscribe
    with pytest.raises(IllegalStateError):
        consumer.subscribe(topics=['foo'])
    consumer.assign([])
    assert consumer.assignment() == set()


def test_poll_timeout_zero_returns_buffered_records():
    """Test that poll(timeout_ms=0) returns records already in the fetch buffer.

    Regression test for https://github.com/dpkp/kafka-python/issues/2692
    Buffered records are checked before coordinator.poll(), so they are
    returned regardless of coordinator state (rebalance, timeout, etc.).
    """
    consumer = KafkaConsumer(api_version=(0, 10, 0))
    tp = TopicPartition('test-topic', 0)
    consumer.assign([tp])

    mock_records = {tp: [MagicMock()]}

    # Fetcher already has records buffered from a previous network poll.
    # coordinator.poll() should never be reached in this path.
    with patch.object(consumer._fetcher, 'fetched_records', return_value=(mock_records, False)), \
         patch.object(consumer._fetcher, 'send_fetches', return_value=[]), \
         patch.object(consumer._client, 'poll'), \
         patch.object(consumer._coordinator, 'poll', return_value=False) as mock_coord_poll:

        result = consumer.poll(timeout_ms=0)
        assert result == mock_records, (
            "poll(timeout_ms=0) should return buffered records "
            "without needing a successful coordinator.poll()"
        )
        mock_coord_poll.assert_not_called()


def test_poll_timeout_zero_returns_empty_when_no_buffered_records():
    """Test that poll(timeout_ms=0) returns empty when no records are buffered.

    Ensures that the non-blocking behavior is preserved -- if there are no
    records in the buffer and coordinator times out, return empty immediately.
    """
    consumer = KafkaConsumer(api_version=(0, 10, 0))
    tp = TopicPartition('test-topic', 0)
    consumer.assign([tp])

    with patch.object(consumer._fetcher, 'fetched_records', return_value=({}, False)), \
         patch.object(consumer._coordinator, 'poll', return_value=False):

        result = consumer.poll(timeout_ms=0)
        assert result == {}, (
            "poll(timeout_ms=0) should return empty dict when no records are buffered"
        )
