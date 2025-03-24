from __future__ import absolute_import

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
