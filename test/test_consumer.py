import pytest

from kafka import KafkaConsumer
from kafka.errors import KafkaConfigurationError


class TestKafkaConsumer:
    def test_session_timeout_larger_than_request_timeout_raises(self):
        with pytest.raises(KafkaConfigurationError):
            KafkaConsumer(bootstrap_servers='localhost:9092', api_version=(0, 9), group_id='foo', session_timeout_ms=50000, request_timeout_ms=40000)

    def test_fetch_max_wait_larger_than_request_timeout_raises(self):
        with pytest.raises(KafkaConfigurationError):
            KafkaConsumer(bootstrap_servers='localhost:9092', fetch_max_wait_ms=50000, request_timeout_ms=40000)

    def test_request_timeout_larger_than_connections_max_idle_ms_raises(self):
        with pytest.raises(KafkaConfigurationError):
            KafkaConsumer(bootstrap_servers='localhost:9092', api_version=(0, 9), request_timeout_ms=50000, connections_max_idle_ms=40000)

    def test_subscription_copy(self):
        consumer = KafkaConsumer('foo', api_version=(0, 10, 0))
        sub = consumer.subscription()
        assert sub is not consumer.subscription()
        assert sub == set(['foo'])
        sub.add('fizz')
        assert consumer.subscription() == set(['foo'])
