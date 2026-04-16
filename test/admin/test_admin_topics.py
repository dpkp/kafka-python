import pytest

from kafka.admin import KafkaAdminClient, NewTopic, NewPartitions
from kafka.errors import KafkaTimeoutError, UnknownTopicOrPartitionError


def test_new_partitions():
    good_partitions = NewPartitions(6)
    assert good_partitions.total_count == 6
    assert good_partitions.new_assignments is None
    good_partitions = NewPartitions(7, [[1, 2, 3]])
    assert good_partitions.total_count == 7
    assert good_partitions.new_assignments == [[1, 2, 3]]


def test_new_topic():
    good_topic = NewTopic('foo')
    assert good_topic.name == 'foo'
    assert good_topic.num_partitions == -1
    assert good_topic.replication_factor == -1
    assert good_topic.replica_assignments == {}
    assert good_topic.topic_configs == {}

    good_topic = NewTopic('foo', 1)
    assert good_topic.name == 'foo'
    assert good_topic.num_partitions == 1
    assert good_topic.replication_factor == -1
    assert good_topic.replica_assignments == {}
    assert good_topic.topic_configs == {}

    good_topic = NewTopic('foo', 1, 1, {1: [1, 1, 1]})
    assert good_topic.name == 'foo'
    assert good_topic.num_partitions == 1
    assert good_topic.replication_factor == 1
    assert good_topic.replica_assignments == {1: [1, 1, 1]}
    assert good_topic.topic_configs == {}

    good_topic = NewTopic('foo', 1, 2)
    assert good_topic.name == 'foo'
    assert good_topic.num_partitions == 1
    assert good_topic.replication_factor == 2
    assert good_topic.replica_assignments == {}
    assert good_topic.topic_configs == {}

    good_topic = NewTopic('bar', -1, -1, {1: [1, 2, 3]}, {'key': 'value'})
    assert good_topic.name == 'bar'
    assert good_topic.num_partitions == -1
    assert good_topic.replication_factor == -1
    assert good_topic.replica_assignments == {1: [1, 2, 3]}
    assert good_topic.topic_configs == {'key': 'value'}


# ---------------------------------------------------------------------------
# _topic_not_ready_reason (pure function, no network)
# ---------------------------------------------------------------------------


def _ready_topic(name='foo', num_partitions=1):
    return {
        'name': name,
        'error_code': 0,
        'partitions': [
            {'error_code': 0, 'partition_index': i, 'leader_id': 0}
            for i in range(num_partitions)
        ],
    }


def test_topic_not_ready_reason_missing():
    assert KafkaAdminClient._topic_not_ready_reason(None) == 'missing from metadata response'


def test_topic_not_ready_reason_topic_error():
    assert KafkaAdminClient._topic_not_ready_reason(
        {'name': 'foo', 'error_code': 3, 'partitions': []}
    ) == 'UnknownTopicOrPartitionError'


def test_topic_not_ready_reason_no_partitions():
    assert KafkaAdminClient._topic_not_ready_reason(
        {'name': 'foo', 'error_code': 0, 'partitions': []}
    ) == 'no partitions reported'


def test_topic_not_ready_reason_no_leader():
    assert KafkaAdminClient._topic_not_ready_reason(
        {'name': 'foo', 'error_code': 0, 'partitions': [
            {'error_code': 0, 'partition_index': 0, 'leader_id': -1},
            {'error_code': 0, 'partition_index': 1, 'leader_id': 0},
        ]}
    ) == 'p0=no leader'


def test_topic_not_ready_reason_partition_error():
    assert KafkaAdminClient._topic_not_ready_reason(
        {'name': 'foo', 'error_code': 0, 'partitions': [
            {'error_code': 5, 'partition_index': 0, 'leader_id': -1},
        ]}
    ) == 'p0=LeaderNotAvailableError'


def test_topic_not_ready_reason_partial_partition_errors():
    # Multiple partitions each with their own issue -> all reasons joined.
    reason = KafkaAdminClient._topic_not_ready_reason(
        {'name': 'foo', 'error_code': 0, 'partitions': [
            {'error_code': 0, 'partition_index': 0, 'leader_id': -1},
            {'error_code': 5, 'partition_index': 1, 'leader_id': -1},
        ]}
    )
    assert 'p0=no leader' in reason
    assert 'p1=LeaderNotAvailableError' in reason


def test_topic_not_ready_reason_ready():
    assert KafkaAdminClient._topic_not_ready_reason(_ready_topic()) is None


# ---------------------------------------------------------------------------
# wait_for_topics (mocks describe_topics; does not hit the network)
# ---------------------------------------------------------------------------


def _bare_admin():
    """Return a KafkaAdminClient instance without running __init__ (which
    would try to bootstrap a real broker). All attributes needed by the
    method under test are provided by the test.
    """
    return object.__new__(KafkaAdminClient)


def test_wait_for_topics_empty_list_returns_immediately():
    admin = _bare_admin()
    # No describe_topics monkey-patch: if it were called the test would
    # crash with AttributeError, proving the empty-list fast path.
    admin.wait_for_topics([])


def test_wait_for_topics_ready_on_first_call(monkeypatch):
    admin = _bare_admin()
    calls = []
    def fake_describe_topics(topics):
        calls.append(topics)
        return [_ready_topic(name=t, num_partitions=2) for t in topics]
    monkeypatch.setattr(admin, 'describe_topics', fake_describe_topics)
    admin.wait_for_topics(['foo', 'bar'], timeout_ms=1000)
    assert len(calls) == 1
    assert set(calls[0]) == {'foo', 'bar'}


def test_wait_for_topics_becomes_ready_after_retry(monkeypatch):
    admin = _bare_admin()
    responses = [
        # First call: topic missing
        [],
        # Second call: topic exists but no leader yet
        [{'name': 'foo', 'error_code': 0, 'partitions': [
            {'error_code': 0, 'partition_index': 0, 'leader_id': -1}]}],
        # Third call: ready
        [_ready_topic(name='foo')],
    ]
    def fake_describe_topics(topics):
        return responses.pop(0)
    monkeypatch.setattr(admin, 'describe_topics', fake_describe_topics)
    admin.wait_for_topics(['foo'], timeout_ms=5000)
    assert responses == []  # all three calls consumed


def test_wait_for_topics_timeout(monkeypatch):
    admin = _bare_admin()
    def fake_describe_topics(topics):
        return [{'name': 'foo', 'error_code': 3, 'partitions': []}]
    monkeypatch.setattr(admin, 'describe_topics', fake_describe_topics)
    with pytest.raises(KafkaTimeoutError) as exc_info:
        admin.wait_for_topics(['foo'], timeout_ms=200)
    assert 'foo' in str(exc_info.value)
    assert 'UnknownTopicOrPartitionError' in str(exc_info.value)


def test_wait_for_topics_describe_exception_keeps_retrying(monkeypatch):
    """A transient exception from describe_topics should be logged and
    retried, not propagated - otherwise a flaky broker could turn a
    recoverable wait into a hard failure."""
    admin = _bare_admin()
    state = {'calls': 0}
    def fake_describe_topics(topics):
        state['calls'] += 1
        if state['calls'] == 1:
            raise RuntimeError('transient')
        return [_ready_topic(name=t) for t in topics]
    monkeypatch.setattr(admin, 'describe_topics', fake_describe_topics)
    admin.wait_for_topics(['foo'], timeout_ms=5000)
    assert state['calls'] == 2
