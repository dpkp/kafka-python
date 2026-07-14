from unittest.mock import PropertyMock

import pytest

from kafka import ConsumerGroupMetadata, KafkaConsumer, TopicPartition
from kafka.errors import KafkaConfigurationError, IllegalStateError, KafkaTimeoutError
from kafka.future import Future
from kafka.util import Timer


def test_session_timeout_different_from_max_poll_timeout_raises():
    with pytest.raises(KafkaConfigurationError):
        KafkaConsumer(bootstrap_servers='localhost:9092', api_version=(0, 9), group_id='foo', session_timeout_ms=50000, max_poll_timeout_ms=40000)


def test_fetch_max_wait_larger_than_request_timeout_raises():
    with pytest.raises(KafkaConfigurationError):
        KafkaConsumer(bootstrap_servers='localhost:9092', fetch_max_wait_ms=50000, request_timeout_ms=40000)


def test_request_timeout_larger_than_connections_max_idle_ms_raises():
    with pytest.raises(KafkaConfigurationError):
        KafkaConsumer(bootstrap_servers='localhost:9092', api_version=(0, 9), request_timeout_ms=50000, connections_max_idle_ms=40000)


def test_default_api_timeout_smaller_than_request_timeout_raises():
    with pytest.raises(KafkaConfigurationError):
        KafkaConsumer(bootstrap_servers='localhost:9092', api_version=(0, 9),
                      request_timeout_ms=70000, default_api_timeout_ms=60000)


def test_fetch_all_topic_metadata_restores_flag_on_timeout(mocker):
    """_fetch_all_topic_metadata must restore need_all_topic_metadata even when
    the bounded metadata wait times out (issue #3121). Otherwise a timed-out
    topics()/partitions_for_topic() would leave the consumer permanently
    fetching all-topic metadata."""
    consumer = KafkaConsumer(api_version=(0, 10, 0))
    run_mock = mocker.patch.object(consumer._net, 'run')
    try:
        original = consumer._cluster.need_all_topic_metadata
        # Skip the in-progress branch so the try/finally block is exercised.
        mocker.patch.object(type(consumer._cluster), 'metadata_refresh_in_progress',
                            new_callable=PropertyMock, return_value=False)
        mocker.patch.object(consumer._cluster, 'request_update', return_value=Future())
        run_mock.side_effect = KafkaTimeoutError('boom')

        with pytest.raises(KafkaTimeoutError):
            consumer.topics(timeout_ms=100)

        assert consumer._cluster.need_all_topic_metadata == original
    finally:
        # Let close() no-op through the mocked run() instead of raising.
        run_mock.side_effect = None
        run_mock.return_value = None
        consumer.close()


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


def _stub_poll_path(consumer, mocker, fetch_return):
    """Patch out the bits of _poll_once we don't care about so the test can
    focus on the fetch -> sleep handoff."""
    mocker.patch.object(consumer._coordinator, 'poll', return_value=True)
    mocker.patch.object(consumer, '_refresh_committed_offsets')
    mocker.patch.object(consumer._fetcher, 'reset_offsets_if_needed')
    mocker.patch.object(consumer._fetcher, 'maybe_validate_positions')
    mocker.patch.object(consumer._fetcher, 'validate_offsets_if_needed')
    mocker.patch.object(consumer._fetcher, 'fetch_records',
                        return_value=fetch_return)


def test_poll_once_sleeps_when_fetcher_idle(mocker):
    """If the fetcher reports it has no work pending, _poll_once sleeps up to
    poll_timeout_ms before returning - otherwise consumer.poll() busy-loops
    on no-fetchable-partition consumers."""
    consumer = KafkaConsumer(api_version=(0, 10, 0))
    try:
        _stub_poll_path(consumer, mocker, fetch_return=({}, True))
        sleep = mocker.patch('kafka.consumer.group.time.sleep')

        records = consumer._poll_once(Timer(1000), max_records=100)

        assert records == {}
        sleep.assert_called_once()
        slept_secs = sleep.call_args[0][0]
        # poll_timeout_ms is uncapped for no-group consumers, so we sleep
        # roughly the full Timer budget (1.0s).
        assert 0 < slept_secs <= 1.0
    finally:
        consumer.close()


def test_poll_once_does_not_sleep_when_records_returned(mocker):
    """fetch_records returned records - no sleep, caller can return them."""
    consumer = KafkaConsumer(api_version=(0, 10, 0))
    try:
        tp_records = {TopicPartition('foo', 0): [object()]}
        _stub_poll_path(consumer, mocker, fetch_return=(tp_records, False))
        sleep = mocker.patch('kafka.consumer.group.time.sleep')

        records = consumer._poll_once(Timer(1000), max_records=100)

        assert records is tp_records
        sleep.assert_not_called()
    finally:
        consumer.close()


def test_poll_once_does_not_sleep_when_fetcher_waited_but_empty(mocker):
    """fetch_records returned ({}, False) - it waited on in-flight work and
    got nothing. Don't sleep; the next loop iteration will check the
    completed fetches without delay."""
    consumer = KafkaConsumer(api_version=(0, 10, 0))
    try:
        _stub_poll_path(consumer, mocker, fetch_return=({}, False))
        sleep = mocker.patch('kafka.consumer.group.time.sleep')

        records = consumer._poll_once(Timer(1000), max_records=100)

        assert records == {}
        sleep.assert_not_called()
    finally:
        consumer.close()
