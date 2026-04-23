import pytest

import kafka.errors as Errors
from kafka.admin import KafkaAdminClient
from kafka.protocol.admin import (
    AlterReplicaLogDirsRequest, AlterReplicaLogDirsResponse,
)
from kafka.protocol.metadata import MetadataResponse
from kafka.structs import TopicPartitionReplica

from test.mock_broker import MockBroker


@pytest.fixture
def broker(request):
    broker_version = getattr(request, 'param', (4, 2))
    return MockBroker(broker_version=broker_version)


@pytest.fixture
def multi_broker(broker):
    Broker = MetadataResponse.MetadataResponseBroker
    broker.set_metadata(brokers=[
        Broker(node_id=0, host=broker.host, port=broker.port, rack=None),
        Broker(node_id=1, host=broker.host, port=broker.port, rack=None),
    ])
    return broker


@pytest.fixture
def admin(broker):
    admin = KafkaAdminClient(
        kafka_client=broker.client_factory(),
        bootstrap_servers='%s:%d' % (broker.host, broker.port),
        request_timeout_ms=5000,
    )
    try:
        yield admin
    finally:
        admin.close()


def _alter_response(results):
    """results: iterable of (topic, partition, error_code)."""
    Topic = AlterReplicaLogDirsResponse.AlterReplicaLogDirTopicResult
    Partition = Topic.AlterReplicaLogDirPartitionResult
    by_topic = {}
    for topic, partition, err in results:
        by_topic.setdefault(topic, []).append(
            Partition(partition_index=partition, error_code=err))
    return AlterReplicaLogDirsResponse(
        throttle_time_ms=0,
        results=[Topic(topic_name=t, partitions=parts)
                 for t, parts in by_topic.items()],
    )


class TestAlterReplicaLogDirsMockBroker:

    def test_empty_input_is_noop(self, admin):
        assert admin.alter_replica_log_dirs({}) == {}

    def test_success_returns_per_replica_errors(self, broker, admin):
        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            captured['request'] = AlterReplicaLogDirsRequest.decode(
                request_bytes, version=api_version, header=True)
            return _alter_response([('topic-a', 0, 0), ('topic-a', 1, 0)])

        broker.respond_fn(AlterReplicaLogDirsRequest, handler)

        result = admin.alter_replica_log_dirs({
            TopicPartitionReplica('topic-a', 0, 0): '/tmp/kafka-logs-a',
            TopicPartitionReplica('topic-a', 1, 0): '/tmp/kafka-logs-a',
        })

        assert result == {
            TopicPartitionReplica('topic-a', 0, 0): Errors.NoError,
            TopicPartitionReplica('topic-a', 1, 0): Errors.NoError,
        }

        req = captured['request']
        assert len(req.dirs) == 1
        assert req.dirs[0].path == '/tmp/kafka-logs-a'
        assert req.dirs[0].topics[0].name == 'topic-a'
        assert list(req.dirs[0].topics[0].partitions) == [0, 1]

    def test_groups_by_dir_within_broker(self, broker, admin):
        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            captured['request'] = AlterReplicaLogDirsRequest.decode(
                request_bytes, version=api_version, header=True)
            return _alter_response([('t', 0, 0), ('t', 1, 0)])

        broker.respond_fn(AlterReplicaLogDirsRequest, handler)

        admin.alter_replica_log_dirs({
            TopicPartitionReplica('t', 0, 0): '/disk/a',
            TopicPartitionReplica('t', 1, 0): '/disk/b',
        })

        req = captured['request']
        by_dir = {d.path: d for d in req.dirs}
        assert set(by_dir) == {'/disk/a', '/disk/b'}

    def test_tuple_key_accepted(self, broker, admin):
        broker.respond(
            AlterReplicaLogDirsRequest,
            _alter_response([('topic-a', 0, 0)]))

        result = admin.alter_replica_log_dirs({
            ('topic-a', 0, 0): '/tmp/kafka-logs-a',
        })
        assert result == {TopicPartitionReplica('topic-a', 0, 0): Errors.NoError}

    def test_one_request_per_broker(self, multi_broker, admin):
        captured = []

        def handler(api_key, api_version, correlation_id, request_bytes):
            captured.append(AlterReplicaLogDirsRequest.decode(
                request_bytes, version=api_version, header=True))
            return _alter_response([('topic-a', 0, 0)])

        multi_broker.respond_fn(AlterReplicaLogDirsRequest, handler)
        multi_broker.respond_fn(AlterReplicaLogDirsRequest, handler)

        result = admin.alter_replica_log_dirs({
            TopicPartitionReplica('topic-a', 0, 0): '/disk/a',
            TopicPartitionReplica('topic-a', 0, 1): '/disk/b',
        })

        assert len(captured) == 2
        paths_sent = {r.dirs[0].path for r in captured}
        assert paths_sent == {'/disk/a', '/disk/b'}
        assert set(result) == {
            TopicPartitionReplica('topic-a', 0, 0),
            TopicPartitionReplica('topic-a', 0, 1),
        }

    def test_error_surfaces_per_replica(self, broker, admin):
        broker.respond(
            AlterReplicaLogDirsRequest,
            _alter_response([
                ('topic-a', 0, 0),
                ('topic-a', 1, Errors.LogDirNotFoundError.errno),
            ]))

        result = admin.alter_replica_log_dirs({
            TopicPartitionReplica('topic-a', 0, 0): '/disk/a',
            TopicPartitionReplica('topic-a', 1, 0): '/disk/a',
        })

        assert result[TopicPartitionReplica('topic-a', 0, 0)] is Errors.NoError
        assert result[TopicPartitionReplica('topic-a', 1, 0)] is Errors.LogDirNotFoundError
