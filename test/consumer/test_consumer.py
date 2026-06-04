import pytest

from kafka import ConsumerGroupMetadata, KafkaConsumer, TopicPartition
from kafka.errors import KafkaConfigurationError, IllegalStateError


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
    consumer.close()


def test_assign():
    # Consumer w/ subscription to topic 'foo'
    consumer = KafkaConsumer('foo', api_version=(0, 10, 0))
    assert consumer.assignment() == set()
    # Cannot assign manually
    with pytest.raises(IllegalStateError):
        consumer.assign([TopicPartition('foo', 0)])

    assert 'foo' in consumer._client.cluster._topics
    consumer.close()

    consumer = KafkaConsumer(api_version=(0, 10, 0))
    assert consumer.assignment() == set()
    consumer.assign([TopicPartition('foo', 0)])
    assert consumer.assignment() == set([TopicPartition('foo', 0)])
    assert 'foo' in consumer._client.cluster._topics
    # Cannot subscribe
    with pytest.raises(IllegalStateError):
        consumer.subscribe(topics=['foo'])
    consumer.assign([])
    assert consumer.assignment() == set()
    consumer.close()


def test_context_manager_closes_on_exit():
    with KafkaConsumer(api_version=(0, 10, 0)) as consumer:
        assert consumer._closed is False
    assert consumer._closed is True


def test_context_manager_suppresses_autocommit_on_exception():
    # Verify the __exit__ -> close(autocommit=...) wiring. We don't need a
    # real coordinator for this; just check that an exception propagates and
    # that close() is reached.
    consumer = KafkaConsumer(api_version=(0, 10, 0))
    with pytest.raises(RuntimeError):
        with consumer:
            raise RuntimeError('boom')
    assert consumer._closed is True


def test_group_metadata_without_group_id_returns_empty_snapshot():
    """Manual-assignment consumers have no group_id; group_metadata() must
    still return a valid snapshot so users can pass it through to
    producer.send_offsets_to_transaction without first checking."""
    consumer = KafkaConsumer(api_version=(0, 10, 0))
    gm = consumer.group_metadata()
    assert isinstance(gm, ConsumerGroupMetadata)
    assert gm.group_id is None
    assert gm.generation_id == -1
    assert gm.member_id == ''
    consumer.close()


def test_group_metadata_with_group_id_delegates_to_coordinator():
    """When configured with a group_id, group_metadata() returns the live
    coordinator snapshot (unjoined defaults until poll() drives a join)."""
    consumer = KafkaConsumer(api_version=(0, 10, 0), group_id='grp')
    gm = consumer.group_metadata()
    assert gm.group_id == 'grp'
    assert gm.generation_id == -1
    assert gm.member_id == ''
    consumer.close()
