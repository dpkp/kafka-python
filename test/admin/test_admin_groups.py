import pytest

from kafka.admin import KafkaAdminClient
from kafka.errors import (
    GroupIdNotFoundError,
    GroupSubscribedToTopicError,
    NoError,
    UnknownMemberIdError,
)
from kafka.protocol.consumer import (
    OffsetCommitRequest, OffsetCommitResponse,
    OffsetDeleteRequest, OffsetDeleteResponse,
)
from kafka.protocol.consumer.group import DEFAULT_GENERATION_ID, UNKNOWN_MEMBER_ID
from kafka.structs import OffsetAndMetadata, TopicPartition

from test.mock_broker import MockBroker


def _make_admin(broker):
    return KafkaAdminClient(
        kafka_client=broker.client_factory(),
        bootstrap_servers='%s:%d' % (broker.host, broker.port),
        api_version=broker.broker_version,
        request_timeout_ms=5000,
    )


# ---------------------------------------------------------------------------
# alter_group_offsets
# ---------------------------------------------------------------------------


class TestAlterGroupOffsetsMockBroker:

    def test_success_returns_tp_to_noerror(self):
        broker = MockBroker()
        _Topic = OffsetCommitResponse.OffsetCommitResponseTopic
        _Partition = _Topic.OffsetCommitResponsePartition
        broker.respond(
            OffsetCommitRequest,
            OffsetCommitResponse(
                throttle_time_ms=0,
                topics=[
                    _Topic(name='topic-a', partitions=[
                        _Partition(partition_index=0, error_code=0),
                        _Partition(partition_index=1, error_code=0),
                    ]),
                ],
            ),
        )

        admin = _make_admin(broker)
        try:
            result = admin.alter_group_offsets(
                'g1',
                {
                    TopicPartition('topic-a', 0): OffsetAndMetadata(10, '', None),
                    TopicPartition('topic-a', 1): OffsetAndMetadata(20, 'm', 5),
                },
                group_coordinator_id=0,
            )
        finally:
            admin.close()

        assert result == {
            TopicPartition('topic-a', 0): NoError,
            TopicPartition('topic-a', 1): NoError,
        }

    def test_request_uses_standalone_member_fields(self):
        broker = MockBroker()
        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            captured['request'] = OffsetCommitRequest.decode(
                request_bytes, version=api_version, header=True)
            return OffsetCommitResponse(throttle_time_ms=0, topics=[])

        broker.respond_fn(OffsetCommitRequest, handler)

        admin = _make_admin(broker)
        try:
            admin.alter_group_offsets(
                'g1',
                {TopicPartition('topic-a', 0): OffsetAndMetadata(10, 'meta', 7)},
                group_coordinator_id=0,
            )
        finally:
            admin.close()

        req = captured['request']
        assert req.group_id == 'g1'
        assert req.generation_id_or_member_epoch == DEFAULT_GENERATION_ID
        assert req.member_id == UNKNOWN_MEMBER_ID
        assert req.group_instance_id is None
        assert req.retention_time_ms == -1
        assert len(req.topics) == 1
        topic = req.topics[0]
        assert topic.name == 'topic-a'
        assert len(topic.partitions) == 1
        p = topic.partitions[0]
        assert p.partition_index == 0
        assert p.committed_offset == 10
        assert p.committed_metadata == 'meta'
        assert p.committed_leader_epoch == 7

    def test_leader_epoch_none_sent_as_minus_one(self):
        broker = MockBroker()
        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            captured['request'] = OffsetCommitRequest.decode(
                request_bytes, version=api_version, header=True)
            return OffsetCommitResponse(throttle_time_ms=0, topics=[])

        broker.respond_fn(OffsetCommitRequest, handler)

        admin = _make_admin(broker)
        try:
            admin.alter_group_offsets(
                'g1',
                {TopicPartition('topic-a', 0): OffsetAndMetadata(10, '', None)},
                group_coordinator_id=0,
            )
        finally:
            admin.close()

        assert captured['request'].topics[0].partitions[0].committed_leader_epoch == -1

    def test_partition_level_error_returned_not_raised(self):
        broker = MockBroker()
        _Topic = OffsetCommitResponse.OffsetCommitResponseTopic
        _Partition = _Topic.OffsetCommitResponsePartition
        broker.respond(
            OffsetCommitRequest,
            OffsetCommitResponse(
                throttle_time_ms=0,
                topics=[
                    _Topic(name='topic-a', partitions=[
                        _Partition(partition_index=0,
                                   error_code=UnknownMemberIdError.errno),
                    ]),
                ],
            ),
        )

        admin = _make_admin(broker)
        try:
            result = admin.alter_group_offsets(
                'g1',
                {TopicPartition('topic-a', 0): OffsetAndMetadata(1, '', None)},
                group_coordinator_id=0,
            )
        finally:
            admin.close()

        assert result == {TopicPartition('topic-a', 0): UnknownMemberIdError}

    def test_empty_offsets_is_noop(self):
        broker = MockBroker()
        admin = _make_admin(broker)
        try:
            result = admin.alter_group_offsets('g1', {}, group_coordinator_id=0)
        finally:
            admin.close()
        assert result == {}


# ---------------------------------------------------------------------------
# delete_group_offsets
# ---------------------------------------------------------------------------


class TestDeleteGroupOffsetsMockBroker:

    def test_success_returns_tp_to_noerror(self):
        broker = MockBroker()
        _Topic = OffsetDeleteResponse.OffsetDeleteResponseTopic
        _Partition = _Topic.OffsetDeleteResponsePartition
        broker.respond(
            OffsetDeleteRequest,
            OffsetDeleteResponse(
                error_code=0,
                throttle_time_ms=0,
                topics=[
                    _Topic(name='topic-a', partitions=[
                        _Partition(partition_index=0, error_code=0),
                        _Partition(partition_index=1, error_code=0),
                    ]),
                ],
            ),
        )

        admin = _make_admin(broker)
        try:
            result = admin.delete_group_offsets(
                'g1',
                [TopicPartition('topic-a', 0), TopicPartition('topic-a', 1)],
                group_coordinator_id=0,
            )
        finally:
            admin.close()

        assert result == {
            TopicPartition('topic-a', 0): NoError,
            TopicPartition('topic-a', 1): NoError,
        }

    def test_groups_partitions_by_topic(self):
        broker = MockBroker()
        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            captured['request'] = OffsetDeleteRequest.decode(
                request_bytes, version=api_version, header=True)
            return OffsetDeleteResponse(error_code=0, throttle_time_ms=0, topics=[])

        broker.respond_fn(OffsetDeleteRequest, handler)

        admin = _make_admin(broker)
        try:
            admin.delete_group_offsets(
                'g1',
                [
                    TopicPartition('topic-a', 0),
                    TopicPartition('topic-b', 2),
                    TopicPartition('topic-a', 1),
                ],
                group_coordinator_id=0,
            )
        finally:
            admin.close()

        req = captured['request']
        assert req.group_id == 'g1'
        topics_by_name = {t.name: t for t in req.topics}
        assert set(topics_by_name.keys()) == {'topic-a', 'topic-b'}
        a_indexes = sorted(p.partition_index for p in topics_by_name['topic-a'].partitions)
        b_indexes = sorted(p.partition_index for p in topics_by_name['topic-b'].partitions)
        assert a_indexes == [0, 1]
        assert b_indexes == [2]

    def test_top_level_error_raises(self):
        broker = MockBroker()
        broker.respond(
            OffsetDeleteRequest,
            OffsetDeleteResponse(
                error_code=GroupIdNotFoundError.errno,
                throttle_time_ms=0,
                topics=[],
            ),
        )

        admin = _make_admin(broker)
        try:
            with pytest.raises(GroupIdNotFoundError):
                admin.delete_group_offsets(
                    'g1',
                    [TopicPartition('topic-a', 0)],
                    group_coordinator_id=0,
                )
        finally:
            admin.close()

    def test_partition_level_error_returned_not_raised(self):
        broker = MockBroker()
        _Topic = OffsetDeleteResponse.OffsetDeleteResponseTopic
        _Partition = _Topic.OffsetDeleteResponsePartition
        broker.respond(
            OffsetDeleteRequest,
            OffsetDeleteResponse(
                error_code=0,
                throttle_time_ms=0,
                topics=[
                    _Topic(name='topic-a', partitions=[
                        _Partition(partition_index=0,
                                   error_code=GroupSubscribedToTopicError.errno),
                    ]),
                ],
            ),
        )

        admin = _make_admin(broker)
        try:
            result = admin.delete_group_offsets(
                'g1',
                [TopicPartition('topic-a', 0)],
                group_coordinator_id=0,
            )
        finally:
            admin.close()

        assert result == {TopicPartition('topic-a', 0): GroupSubscribedToTopicError}

    def test_empty_partitions_is_noop(self):
        broker = MockBroker()
        admin = _make_admin(broker)
        try:
            result = admin.delete_group_offsets('g1', [], group_coordinator_id=0)
        finally:
            admin.close()
        assert result == {}
