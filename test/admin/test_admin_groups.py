import pytest

from kafka.admin import KafkaAdminClient, MemberToRemove
from kafka.errors import (
    GroupIdNotFoundError,
    GroupSubscribedToTopicError,
    NoError,
    UnknownMemberIdError,
    UnsupportedVersionError,
)
from kafka.protocol.consumer import (
    LeaveGroupRequest, LeaveGroupResponse,
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


# ---------------------------------------------------------------------------
# remove_group_members
# ---------------------------------------------------------------------------


class TestRemoveGroupMembersMockBroker:

    def test_batch_success_returns_member_to_noerror(self):
        broker = MockBroker()  # broker_version=(4, 2) -> LeaveGroup v5
        _MemberResp = LeaveGroupResponse.MemberResponse
        broker.respond(
            LeaveGroupRequest,
            LeaveGroupResponse(
                throttle_time_ms=0,
                error_code=0,
                members=[
                    _MemberResp(member_id='m1', group_instance_id=None, error_code=0),
                    _MemberResp(member_id='', group_instance_id='static-1', error_code=0),
                ],
            ),
        )

        admin = _make_admin(broker)
        try:
            result = admin.remove_group_members(
                'g1',
                [
                    MemberToRemove(member_id='m1'),
                    MemberToRemove(group_instance_id='static-1'),
                ],
                group_coordinator_id=0,
            )
        finally:
            admin.close()

        assert result == {
            'm1': NoError,
            'static-1': NoError,
        }

    def test_batch_request_fields(self):
        broker = MockBroker()
        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            captured['version'] = api_version
            captured['request'] = LeaveGroupRequest.decode(
                request_bytes, version=api_version, header=True)
            return LeaveGroupResponse(
                throttle_time_ms=0, error_code=0, members=[])

        broker.respond_fn(LeaveGroupRequest, handler)

        admin = _make_admin(broker)
        try:
            admin.remove_group_members(
                'g1',
                [
                    MemberToRemove(member_id='m1', reason='rebalance'),
                    MemberToRemove(group_instance_id='inst-2', reason='shutdown'),
                ],
                group_coordinator_id=0,
            )
        finally:
            admin.close()

        assert captured['version'] >= 3
        req = captured['request']
        assert req.group_id == 'g1'
        assert len(req.members) == 2
        m1 = req.members[0]
        assert m1.member_id == 'm1'
        assert m1.group_instance_id is None
        m2 = req.members[1]
        assert m2.member_id == ''
        assert m2.group_instance_id == 'inst-2'
        if captured['version'] >= 5:
            assert m1.reason == 'rebalance'
            assert m2.reason == 'shutdown'

    def test_batch_top_level_error_raises(self):
        broker = MockBroker()
        broker.respond(
            LeaveGroupRequest,
            LeaveGroupResponse(
                throttle_time_ms=0,
                error_code=GroupIdNotFoundError.errno,
                members=[],
            ),
        )

        admin = _make_admin(broker)
        try:
            with pytest.raises(GroupIdNotFoundError):
                admin.remove_group_members(
                    'g1',
                    [MemberToRemove(member_id='m1')],
                    group_coordinator_id=0,
                )
        finally:
            admin.close()

    def test_batch_per_member_error_returned(self):
        broker = MockBroker()
        _MemberResp = LeaveGroupResponse.MemberResponse
        broker.respond(
            LeaveGroupRequest,
            LeaveGroupResponse(
                throttle_time_ms=0,
                error_code=0,
                members=[
                    _MemberResp(member_id='m1', group_instance_id=None,
                                error_code=UnknownMemberIdError.errno),
                ],
            ),
        )

        admin = _make_admin(broker)
        try:
            result = admin.remove_group_members(
                'g1',
                [MemberToRemove(member_id='m1')],
                group_coordinator_id=0,
            )
        finally:
            admin.close()

        assert result == {'m1': UnknownMemberIdError}

    def test_empty_members_is_noop(self):
        broker = MockBroker()
        admin = _make_admin(broker)
        try:
            result = admin.remove_group_members('g1', [], group_coordinator_id=0)
        finally:
            admin.close()
        assert result == {}

    def test_fallback_fans_out_one_request_per_member(self):
        # (2, 3) broker: LeaveGroup v0-v2 only, no batch support
        broker = MockBroker(broker_version=(2, 3))
        captured = []

        def handler(api_key, api_version, correlation_id, request_bytes):
            captured.append(LeaveGroupRequest.decode(
                request_bytes, version=api_version, header=True))
            return LeaveGroupResponse(
                version=api_version, throttle_time_ms=0, error_code=0)

        broker.respond_fn(LeaveGroupRequest, handler)
        broker.respond_fn(LeaveGroupRequest, handler)

        admin = _make_admin(broker)
        try:
            result = admin.remove_group_members(
                'g1',
                [
                    MemberToRemove(member_id='m1'),
                    MemberToRemove(member_id='m2'),
                ],
                group_coordinator_id=0,
            )
        finally:
            admin.close()

        assert len(captured) == 2
        assert captured[0].group_id == 'g1'
        assert captured[0].member_id == 'm1'
        assert captured[1].member_id == 'm2'
        assert result == {
            'm1': NoError,
            'm2': NoError,
        }

    def test_fallback_rejects_group_instance_id(self):
        broker = MockBroker(broker_version=(2, 3))
        admin = _make_admin(broker)
        try:
            with pytest.raises(UnsupportedVersionError):
                admin.remove_group_members(
                    'g1',
                    [MemberToRemove(group_instance_id='inst-1')],
                    group_coordinator_id=0,
                )
        finally:
            admin.close()

    def test_fallback_requires_member_id(self):
        broker = MockBroker(broker_version=(2, 3))
        admin = _make_admin(broker)
        try:
            with pytest.raises(ValueError):
                admin.remove_group_members(
                    'g1',
                    [MemberToRemove()],
                    group_coordinator_id=0,
                )
        finally:
            admin.close()
