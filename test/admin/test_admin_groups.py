import pytest

from kafka.admin import (
    GroupState, GroupType, MemberToRemove,
    OffsetTimestamp,
)
import kafka.errors as Errors
from kafka.errors import (
    GroupIdNotFoundError,
    GroupSubscribedToTopicError,
    NoError,
    UnknownMemberIdError,
    UnsupportedVersionError,
)
from kafka.protocol.admin import (
    DeleteGroupsRequest, DeleteGroupsResponse,
    DescribeGroupsRequest, DescribeGroupsResponse,
    ListGroupsRequest, ListGroupsResponse,
)
from kafka.protocol.consumer import (
    LeaveGroupRequest, LeaveGroupResponse,
    ListOffsetsRequest, ListOffsetsResponse,
    OffsetCommitRequest, OffsetCommitResponse,
    OffsetDeleteRequest, OffsetDeleteResponse,
    OffsetFetchRequest, OffsetFetchResponse,
)
from kafka.protocol.metadata import (
    CoordinatorType, MetadataResponse,
    FindCoordinatorRequest, FindCoordinatorResponse,
)
from kafka.protocol.consumer.group import DEFAULT_GENERATION_ID, UNKNOWN_MEMBER_ID
from kafka.structs import OffsetAndMetadata, TopicPartition


def _find_coordinator_response(group_id, node_id=0):
    """Build a FindCoordinatorResponse that decodes correctly at any version.

    Sets both the v0-v3 top-level fields and the v4+ coordinators array;
    the encoder picks whichever shape matches the negotiated version.
    """
    Coordinator = FindCoordinatorResponse.Coordinator
    return FindCoordinatorResponse(
        throttle_time_ms=0,
        error_code=0,
        error_message=None,
        node_id=node_id, host='localhost', port=9092,
        coordinators=[Coordinator(
            key=group_id, node_id=node_id, host='localhost', port=9092,
            error_code=0, error_message=None)],
    )


# ---------------------------------------------------------------------------
# alter_group_offsets
# ---------------------------------------------------------------------------


class TestAlterGroupOffsetsMockBroker:
    def test_success_returns_tp_to_noerror(self, broker, admin):
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

        result = admin.alter_group_offsets(
            'g1',
            {
                TopicPartition('topic-a', 0): OffsetAndMetadata(10, '', None),
                TopicPartition('topic-a', 1): OffsetAndMetadata(20, 'm', 5),
            },
            group_coordinator_id=0,
        )

        assert result == {
            TopicPartition('topic-a', 0): NoError,
            TopicPartition('topic-a', 1): NoError,
        }

    def test_request_uses_standalone_member_fields(self, broker, admin):
        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            captured['request'] = OffsetCommitRequest.decode(
                request_bytes, version=api_version, header=True)
            return OffsetCommitResponse(throttle_time_ms=0, topics=[])

        broker.respond_fn(OffsetCommitRequest, handler)

        admin.alter_group_offsets(
            'g1',
            {TopicPartition('topic-a', 0): OffsetAndMetadata(10, 'meta', 7)},
            group_coordinator_id=0,
        )

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

    def test_leader_epoch_none_sent_as_minus_one(self, broker, admin):
        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            captured['request'] = OffsetCommitRequest.decode(
                request_bytes, version=api_version, header=True)
            return OffsetCommitResponse(throttle_time_ms=0, topics=[])

        broker.respond_fn(OffsetCommitRequest, handler)

        admin.alter_group_offsets(
            'g1',
            {TopicPartition('topic-a', 0): OffsetAndMetadata(10, '', None)},
            group_coordinator_id=0,
        )

        assert captured['request'].topics[0].partitions[0].committed_leader_epoch == -1

    def test_partition_level_error_returned_not_raised(self, broker, admin):
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

        result = admin.alter_group_offsets(
            'g1',
            {TopicPartition('topic-a', 0): OffsetAndMetadata(1, '', None)},
            group_coordinator_id=0,
        )

        assert result == {TopicPartition('topic-a', 0): UnknownMemberIdError}

    def test_empty_offsets_is_noop(self, broker, admin):
        result = admin.alter_group_offsets('g1', {}, group_coordinator_id=0)
        assert result == {}


# ---------------------------------------------------------------------------
# delete_group_offsets
# ---------------------------------------------------------------------------


class TestDeleteGroupOffsetsMockBroker:
    def test_success_returns_tp_to_noerror(self, broker, admin):
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

        result = admin.delete_group_offsets(
            'g1',
            [TopicPartition('topic-a', 0), TopicPartition('topic-a', 1)],
            group_coordinator_id=0,
        )

        assert result == {
            TopicPartition('topic-a', 0): NoError,
            TopicPartition('topic-a', 1): NoError,
        }

    def test_groups_partitions_by_topic(self, broker, admin):
        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            captured['request'] = OffsetDeleteRequest.decode(
                request_bytes, version=api_version, header=True)
            return OffsetDeleteResponse(error_code=0, throttle_time_ms=0, topics=[])

        broker.respond_fn(OffsetDeleteRequest, handler)

        admin.delete_group_offsets(
            'g1',
            [
                TopicPartition('topic-a', 0),
                TopicPartition('topic-b', 2),
                TopicPartition('topic-a', 1),
            ],
            group_coordinator_id=0,
        )

        req = captured['request']
        assert req.group_id == 'g1'
        topics_by_name = {t.name: t for t in req.topics}
        assert set(topics_by_name.keys()) == {'topic-a', 'topic-b'}
        a_indexes = sorted(p.partition_index for p in topics_by_name['topic-a'].partitions)
        b_indexes = sorted(p.partition_index for p in topics_by_name['topic-b'].partitions)
        assert a_indexes == [0, 1]
        assert b_indexes == [2]

    def test_top_level_error_raises(self, broker, admin):
        broker.respond(
            OffsetDeleteRequest,
            OffsetDeleteResponse(
                error_code=GroupIdNotFoundError.errno,
                throttle_time_ms=0,
                topics=[],
            ),
        )

        with pytest.raises(GroupIdNotFoundError):
            admin.delete_group_offsets(
                'g1',
                [TopicPartition('topic-a', 0)],
                group_coordinator_id=0,
            )

    def test_partition_level_error_returned_not_raised(self, broker, admin):
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

        result = admin.delete_group_offsets(
            'g1',
            [TopicPartition('topic-a', 0)],
            group_coordinator_id=0,
        )

        assert result == {TopicPartition('topic-a', 0): GroupSubscribedToTopicError}

    def test_empty_partitions_is_noop(self, broker, admin):
        result = admin.delete_group_offsets('g1', [], group_coordinator_id=0)
        assert result == {}


# ---------------------------------------------------------------------------
# remove_group_members
# ---------------------------------------------------------------------------


class TestRemoveGroupMembersMockBroker:
    def test_batch_success_returns_member_to_noerror(self, broker, admin):
        # broker_version=(4, 2) -> LeaveGroup v5
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

        result = admin.remove_group_members(
            'g1',
            [
                MemberToRemove(member_id='m1'),
                MemberToRemove(group_instance_id='static-1'),
            ],
            group_coordinator_id=0,
        )

        assert result == {
            'm1': NoError,
            'static-1': NoError,
        }

    def test_batch_request_fields(self, broker, admin):
        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            captured['version'] = api_version
            captured['request'] = LeaveGroupRequest.decode(
                request_bytes, version=api_version, header=True)
            return LeaveGroupResponse(
                throttle_time_ms=0, error_code=0, members=[])

        broker.respond_fn(LeaveGroupRequest, handler)

        admin.remove_group_members(
            'g1',
            [
                MemberToRemove(member_id='m1', reason='rebalance'),
                MemberToRemove(group_instance_id='inst-2', reason='shutdown'),
            ],
            group_coordinator_id=0,
        )

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

    def test_batch_top_level_error_raises(self, broker, admin):
        broker.respond(
            LeaveGroupRequest,
            LeaveGroupResponse(
                throttle_time_ms=0,
                error_code=GroupIdNotFoundError.errno,
                members=[],
            ),
        )

        with pytest.raises(GroupIdNotFoundError):
            admin.remove_group_members(
                'g1',
                [MemberToRemove(member_id='m1')],
                group_coordinator_id=0,
            )

    def test_batch_per_member_error_returned(self, broker, admin):
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

        result = admin.remove_group_members(
            'g1',
            [MemberToRemove(member_id='m1')],
            group_coordinator_id=0,
        )

        assert result == {'m1': UnknownMemberIdError}

    def test_empty_members_is_noop(self, broker, admin):
        result = admin.remove_group_members('g1', [], group_coordinator_id=0)
        assert result == {}

    @pytest.mark.parametrize("broker", [(2, 3)], indirect=True)
    def test_fallback_fans_out_one_request_per_member(self, broker, admin):
        # (2, 3) broker: LeaveGroup v0-v2 only, no batch support
        captured = []

        def handler(api_key, api_version, correlation_id, request_bytes):
            captured.append(LeaveGroupRequest.decode(
                request_bytes, version=api_version, header=True))
            return LeaveGroupResponse(
                version=api_version, throttle_time_ms=0, error_code=0)

        broker.respond_fn(LeaveGroupRequest, handler)
        broker.respond_fn(LeaveGroupRequest, handler)

        result = admin.remove_group_members(
            'g1',
            [
                MemberToRemove(member_id='m1'),
                MemberToRemove(member_id='m2'),
            ],
            group_coordinator_id=0,
        )

        assert len(captured) == 2
        assert captured[0].group_id == 'g1'
        assert captured[0].member_id == 'm1'
        assert captured[1].member_id == 'm2'
        assert result == {
            'm1': NoError,
            'm2': NoError,
        }

    @pytest.mark.parametrize("broker", [(2, 3)], indirect=True)
    def test_fallback_rejects_group_instance_id(self, broker, admin):
        with pytest.raises(UnsupportedVersionError):
            admin.remove_group_members(
                'g1',
                [MemberToRemove(group_instance_id='inst-1')],
                group_coordinator_id=0,
            )

    @pytest.mark.parametrize("broker", [(2, 3)], indirect=True)
    def test_fallback_requires_member_id(self, broker, admin):
        with pytest.raises(ValueError):
            admin.remove_group_members(
                'g1',
                [MemberToRemove()],
                group_coordinator_id=0,
            )


# ---------------------------------------------------------------------------
# list_groups
# ---------------------------------------------------------------------------


def _capture_list_groups(captured, response=None):
    def handler(api_key, api_version, correlation_id, request_bytes):
        captured['request'] = ListGroupsRequest.decode(
            request_bytes, version=api_version, header=True)
        captured['version'] = api_version
        if response is not None:
            return response
        return ListGroupsResponse(throttle_time_ms=0, error_code=0, groups=[])
    return handler


class TestListGroupsMockBroker:

    def test_no_filters_uses_default_version(self, broker, admin):
        captured = {}
        broker.respond_fn(ListGroupsRequest, _capture_list_groups(captured))
        admin.list_groups()
        req = captured['request']
        assert req.states_filter in (None, [])
        assert req.types_filter in (None, [])

    def test_states_filter_propagates(self, broker, admin):
        captured = {}
        broker.respond_fn(ListGroupsRequest, _capture_list_groups(captured))
        admin.list_groups(states_filter=['Stable', 'Empty'])
        assert captured['version'] >= 4
        assert list(captured['request'].states_filter) == ['Stable', 'Empty']

    def test_types_filter_propagates(self, broker, admin):
        captured = {}
        broker.respond_fn(ListGroupsRequest, _capture_list_groups(captured))
        admin.list_groups(types_filter=['consumer', 'share'])
        assert captured['version'] >= 5
        assert list(captured['request'].types_filter) == ['consumer', 'share']

    def test_both_filters_propagate(self, broker, admin):
        captured = {}
        broker.respond_fn(ListGroupsRequest, _capture_list_groups(captured))
        admin.list_groups(states_filter=['Stable'], types_filter=['consumer'])
        assert captured['version'] >= 5
        assert list(captured['request'].states_filter) == ['Stable']
        assert list(captured['request'].types_filter) == ['consumer']

    def test_enum_filters_normalize_to_protocol_strings(self, broker, admin):
        """Enum members, lowercase names, and raw protocol strings all produce
        the canonical wire value."""
        captured = {}
        broker.respond_fn(ListGroupsRequest, _capture_list_groups(captured))
        admin.list_groups(
            states_filter=[GroupState.STABLE, 'empty', 'preparing-rebalance'],
            types_filter=[GroupType.CONSUMER, 'CLASSIC'])
        assert list(captured['request'].states_filter) == [
            'Stable', 'Empty', 'PreparingRebalance']
        assert list(captured['request'].types_filter) == ['consumer', 'classic']

    def test_response_groups_returned(self, broker, admin):
        ListedGroup = ListGroupsResponse.ListedGroup
        response = ListGroupsResponse(
            throttle_time_ms=0, error_code=0,
            groups=[
                ListedGroup(group_id='g1', protocol_type='consumer',
                            group_state='Stable', group_type='consumer'),
                ListedGroup(group_id='g2', protocol_type='consumer',
                            group_state='Empty', group_type='classic'),
            ])
        broker.respond_fn(ListGroupsRequest, _capture_list_groups({}, response))
        result = admin.list_groups(states_filter=['Stable', 'Empty'],
                                   types_filter=['consumer', 'classic'])
        ids = sorted(g['group_id'] for g in result)
        assert ids == ['g1', 'g2']

    @pytest.mark.parametrize("broker", [(2, 3, 0)], indirect=True)
    def test_states_filter_rejected_on_pre_518_broker(self, broker, admin):
        with pytest.raises(Errors.IncompatibleBrokerVersion):
            admin.list_groups(states_filter=['Stable'])

    @pytest.mark.parametrize("broker", [(3, 7, 0)], indirect=True)
    def test_types_filter_rejected_on_pre_848_broker(self, broker, admin):
        with pytest.raises(Errors.IncompatibleBrokerVersion):
            admin.list_groups(types_filter=['consumer'])


# ---------------------------------------------------------------------------
# reset_group_offsets
# ---------------------------------------------------------------------------


def _set_metadata_for_topic(broker, name, num_partitions, leader_id=0):
    Topic = MetadataResponse.MetadataResponseTopic
    Partition = Topic.MetadataResponsePartition
    Broker = MetadataResponse.MetadataResponseBroker
    brokers = [Broker(node_id=0, host=broker.host, port=broker.port, rack=None)]
    broker.set_metadata(
        topics=[Topic(
            version=8, error_code=0, name=name, is_internal=False,
            partitions=[
                Partition(version=8, error_code=0, partition_index=i,
                          leader_id=leader_id, leader_epoch=0,
                          replica_nodes=[0], isr_nodes=[0], offline_replicas=[])
                for i in range(num_partitions)
            ])],
        brokers=brokers,
    )


def _offset_fetch_response(partitions, group_id='g1'):
    """partitions: list of (topic, partition, committed_offset, metadata, leader_epoch)."""
    _Topic = OffsetFetchResponse.OffsetFetchResponseTopic
    _Partition = _Topic.OffsetFetchResponsePartition
    _Group = OffsetFetchResponse.OffsetFetchResponseGroup
    _GroupTopic = _Group.OffsetFetchResponseTopics
    _GroupPartition = _GroupTopic.OffsetFetchResponsePartitions
    by_topic = {}
    for topic, part, offset, meta, le in partitions:
        by_topic.setdefault(topic, []).append((part, offset, meta, le))
    topics = [_Topic(name=t, partitions=[
        _Partition(partition_index=part, committed_offset=offset,
                   committed_leader_epoch=le, metadata=meta, error_code=0)
        for part, offset, meta, le in parts])
        for t, parts in by_topic.items()]
    groups = [_Group(group_id=group_id, error_code=0, topics=[
        _GroupTopic(name=t, partitions=[
            _GroupPartition(partition_index=part, committed_offset=offset,
                            committed_leader_epoch=le, metadata=meta, error_code=0)
            for part, offset, meta, le in parts])
        for t, parts in by_topic.items()])]
    return OffsetFetchResponse(
        throttle_time_ms=0, error_code=0,
        topics=topics, groups=groups,
    )


def _list_offsets_response(partitions):
    """partitions: list of (topic, partition, offset, timestamp, leader_epoch)."""
    Topic = ListOffsetsResponse.ListOffsetsTopicResponse
    Partition = Topic.ListOffsetsPartitionResponse
    by_topic = {}
    for topic, part, offset, ts, le in partitions:
        by_topic.setdefault(topic, []).append(Partition(
            partition_index=part, error_code=0, timestamp=ts,
            offset=offset, leader_epoch=le))
    return ListOffsetsResponse(
        throttle_time_ms=0,
        topics=[Topic(name=t, partitions=parts) for t, parts in by_topic.items()],
    )


def _offset_commit_response(partitions):
    """partitions: list of (topic, partition, error_code)."""
    _Topic = OffsetCommitResponse.OffsetCommitResponseTopic
    _Partition = _Topic.OffsetCommitResponsePartition
    by_topic = {}
    for topic, part, err in partitions:
        by_topic.setdefault(topic, []).append(
            _Partition(partition_index=part, error_code=err))
    return OffsetCommitResponse(
        throttle_time_ms=0,
        topics=[_Topic(name=t, partitions=parts) for t, parts in by_topic.items()],
    )


class TestResetGroupOffsetsMockBroker:
    def test_clamps_explicit_offset_above_latest(self, broker, admin):
        _set_metadata_for_topic(broker, 'topic-a', num_partitions=1)
        broker.respond(FindCoordinatorRequest, _find_coordinator_response('g1')),
        broker.respond(OffsetFetchRequest, _offset_fetch_response(
            [('topic-a', 0, 50, '', 0)]))
        # Bounds: earliest=10, latest=100
        broker.respond(ListOffsetsRequest, _list_offsets_response(
            [('topic-a', 0, 10, -1, 0)]))
        broker.respond(ListOffsetsRequest, _list_offsets_response(
            [('topic-a', 0, 100, -1, 0)]))
        committed = {}

        def commit_handler(api_key, api_version, correlation_id, request_bytes):
            req = OffsetCommitRequest.decode(request_bytes, version=api_version, header=True)
            for t in req.topics:
                for p in t.partitions:
                    committed[TopicPartition(t.name, p.partition_index)] = p.committed_offset
            return _offset_commit_response([('topic-a', 0, 0)])

        broker.respond_fn(OffsetCommitRequest, commit_handler)

        tp = TopicPartition('topic-a', 0)
        result = admin.reset_group_offsets(
            'g1', {tp: 9999})

        assert committed[tp] == 100
        assert result[tp]['offset'] == 100
        assert result[tp]['error'] is NoError

    def test_clamps_explicit_offset_below_earliest(self, broker, admin):
        _set_metadata_for_topic(broker, 'topic-a', num_partitions=1)
        broker.respond(FindCoordinatorRequest, _find_coordinator_response('g1')),
        broker.respond(OffsetFetchRequest, _offset_fetch_response(
            [('topic-a', 0, 50, '', 0)]))
        broker.respond(ListOffsetsRequest, _list_offsets_response(
            [('topic-a', 0, 10, -1, 0)]))
        broker.respond(ListOffsetsRequest, _list_offsets_response(
            [('topic-a', 0, 100, -1, 0)]))
        committed = {}

        def commit_handler(api_key, api_version, correlation_id, request_bytes):
            req = OffsetCommitRequest.decode(request_bytes, version=api_version, header=True)
            for t in req.topics:
                for p in t.partitions:
                    committed[TopicPartition(t.name, p.partition_index)] = p.committed_offset
            return _offset_commit_response([('topic-a', 0, 0)])

        broker.respond_fn(OffsetCommitRequest, commit_handler)

        tp = TopicPartition('topic-a', 0)
        result = admin.reset_group_offsets(
            'g1', {tp: 5})

        assert committed[tp] == 10
        assert result[tp]['offset'] == 10

    def test_clamps_unknown_offset_to_latest(self, broker, admin):
        # Simulate a timestamp beyond the last record: ListOffsets returns -1.
        _set_metadata_for_topic(broker, 'topic-a', num_partitions=1)
        broker.respond(FindCoordinatorRequest, _find_coordinator_response('g1')),
        broker.respond(OffsetFetchRequest, _offset_fetch_response(
            [('topic-a', 0, 50, '', 0)]))
        broker.respond(ListOffsetsRequest, _list_offsets_response(
            [('topic-a', 0, 10, -1, 0)]))  # earliest
        broker.respond(ListOffsetsRequest, _list_offsets_response(
            [('topic-a', 0, 100, -1, 0)]))  # latest
        broker.respond(ListOffsetsRequest, _list_offsets_response(
            [('topic-a', 0, -1, -1, -1)]))  # spec resolution: UNKNOWN
        committed = {}

        def commit_handler(api_key, api_version, correlation_id, request_bytes):
            req = OffsetCommitRequest.decode(request_bytes, version=api_version, header=True)
            for t in req.topics:
                for p in t.partitions:
                    committed[TopicPartition(t.name, p.partition_index)] = p.committed_offset
            return _offset_commit_response([('topic-a', 0, 0)])

        broker.respond_fn(OffsetCommitRequest, commit_handler)

        tp = TopicPartition('topic-a', 0)
        result = admin.reset_group_offsets(
            'g1', {tp: OffsetTimestamp(99999999999)})

        assert committed[tp] == 100
        assert result[tp]['offset'] == 100

    def test_offset_in_range_not_clamped(self, broker, admin):
        _set_metadata_for_topic(broker, 'topic-a', num_partitions=1)
        broker.respond(FindCoordinatorRequest, _find_coordinator_response('g1')),
        broker.respond(OffsetFetchRequest, _offset_fetch_response(
            [('topic-a', 0, 50, 'm', 3)]))
        broker.respond(ListOffsetsRequest, _list_offsets_response(
            [('topic-a', 0, 10, -1, 0)]))
        broker.respond(ListOffsetsRequest, _list_offsets_response(
            [('topic-a', 0, 100, -1, 0)]))
        committed = {}

        def commit_handler(api_key, api_version, correlation_id, request_bytes):
            req = OffsetCommitRequest.decode(request_bytes, version=api_version, header=True)
            for t in req.topics:
                for p in t.partitions:
                    committed[TopicPartition(t.name, p.partition_index)] = (
                        p.committed_offset, p.committed_metadata, p.committed_leader_epoch)
            return _offset_commit_response([('topic-a', 0, 0)])

        broker.respond_fn(OffsetCommitRequest, commit_handler)

        tp = TopicPartition('topic-a', 0)
        result = admin.reset_group_offsets(
            'g1', {tp: 42})

        # Offset not clamped; existing metadata/leader_epoch preserved.
        assert committed[tp] == (42, 'm', 3)
        assert result[tp]['offset'] == 42

    def test_empty_input_noop(self, broker, admin):
        assert admin.reset_group_offsets('g1', {}) == {}

    def test_unsupported_value_type_raises(self, broker, admin):
        tp = TopicPartition('topic-a', 0)
        with pytest.raises(TypeError, match='Unsupported reset target'):
            admin.reset_group_offsets(
                'g1', {tp: 'earliest'})


# ---------------------------------------------------------------------------
# list_group_offsets
# ---------------------------------------------------------------------------


class TestListGroupOffsetsMockBroker:
    def test_single_group_returns_offsets_at_v8(self, broker, admin):
        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            captured['api_version'] = api_version
            return _offset_fetch_response(
                [('topic-a', 0, 123, 'm', 4), ('topic-a', 1, 234, '', -1)])

        broker.respond_fn(OffsetFetchRequest, handler)
        result = admin.list_group_offsets('g1')

        # Negotiated version is v8 against the default (4, 2) MockBroker.
        assert captured['api_version'] == 8
        assert result == {
            TopicPartition('topic-a', 0): OffsetAndMetadata(
                offset=123, metadata='m', leader_epoch=4),
            TopicPartition('topic-a', 1): OffsetAndMetadata(
                offset=234, metadata='', leader_epoch=-1),
        }

    def test_single_group_partition_error_raises(self, broker, admin):
        _Group = OffsetFetchResponse.OffsetFetchResponseGroup
        _GroupTopic = _Group.OffsetFetchResponseTopics
        _GroupPartition = _GroupTopic.OffsetFetchResponsePartitions
        broker.respond(OffsetFetchRequest, OffsetFetchResponse(
            throttle_time_ms=0, error_code=0, topics=[],
            groups=[_Group(group_id='g1', error_code=0, topics=[
                _GroupTopic(name='topic-a', partitions=[
                    _GroupPartition(partition_index=0, committed_offset=-1,
                                    committed_leader_epoch=-1, metadata='',
                                    error_code=Errors.UnstableOffsetCommitError.errno),
                ])])]))
        with pytest.raises(Errors.UnstableOffsetCommitError):
            admin.list_group_offsets('g1')


class TestListGroupOffsetsMockBroker:
    def test_batches_groups_sharing_coordinator(self, broker, admin):
        # Pre-seed the coordinator cache so we don't need FindCoordinator
        # support in the mock broker.
        admin._manager.cluster._coordinators[(CoordinatorType.GROUP, 'g1')] = 0
        admin._manager.cluster._coordinators[(CoordinatorType.GROUP, 'g2')] = 0

        captured = {}
        _Group = OffsetFetchResponse.OffsetFetchResponseGroup
        _GroupTopic = _Group.OffsetFetchResponseTopics
        _GroupPartition = _GroupTopic.OffsetFetchResponsePartitions

        def handler(api_key, api_version, correlation_id, request_bytes):
            req = OffsetFetchRequest.decode(
                request_bytes, version=api_version, header=True)
            captured['api_version'] = api_version
            captured['request_count'] = captured.get('request_count', 0) + 1
            captured['group_ids'] = [g.group_id for g in req.groups]
            return OffsetFetchResponse(
                throttle_time_ms=0, error_code=0, topics=[],
                groups=[
                    _Group(group_id='g1', error_code=0, topics=[
                        _GroupTopic(name='topic-a', partitions=[
                            _GroupPartition(partition_index=0, committed_offset=10,
                                            committed_leader_epoch=-1, metadata='',
                                            error_code=0)])]),
                    _Group(group_id='g2', error_code=0, topics=[
                        _GroupTopic(name='topic-b', partitions=[
                            _GroupPartition(partition_index=0, committed_offset=20,
                                            committed_leader_epoch=-1, metadata='',
                                            error_code=0)])]),
                ])

        broker.respond_fn(OffsetFetchRequest, handler)
        result = admin.list_group_offsets(['g1', 'g2'])

        assert captured['api_version'] == 8
        assert captured['request_count'] == 1  # single RPC for both groups
        assert sorted(captured['group_ids']) == ['g1', 'g2']
        assert result == {
            'g1': {TopicPartition('topic-a', 0):
                   OffsetAndMetadata(offset=10, metadata='', leader_epoch=-1)},
            'g2': {TopicPartition('topic-b', 0):
                   OffsetAndMetadata(offset=20, metadata='', leader_epoch=-1)},
        }

    def test_group_level_error_raises(self, broker, admin):
        admin._manager.cluster._coordinators[(CoordinatorType.GROUP, 'g1')] = 0
        _Group = OffsetFetchResponse.OffsetFetchResponseGroup
        broker.respond(OffsetFetchRequest, OffsetFetchResponse(
            throttle_time_ms=0, error_code=0, topics=[],
            groups=[_Group(
                group_id='g1',
                error_code=Errors.GroupIdNotFoundError.errno,
                topics=[])]))
        with pytest.raises(GroupIdNotFoundError):
            admin.list_group_offsets({'g1': None})

    @pytest.mark.parametrize("broker", [(2, 8, 0)], indirect=True)
    def test_fallback_on_pre_v8_broker_issues_one_rpc_per_group(self, broker, admin):
        # Kafka 2.8 caps OffsetFetch at v7 (KIP-447 only) -> no batch, fan out.
        admin._manager.cluster._coordinators[(CoordinatorType.GROUP, 'g1')] = 0
        admin._manager.cluster._coordinators[(CoordinatorType.GROUP, 'g2')] = 0
        captured = {'count': 0, 'group_ids': []}

        def handler(api_key, api_version, correlation_id, request_bytes):
            req = OffsetFetchRequest.decode(
                request_bytes, version=api_version, header=True)
            captured['count'] += 1
            captured['group_ids'].append(req.group_id)
            captured['api_version'] = api_version
            return _offset_fetch_response(
                [('topic-a', 0, 100, '', -1)], group_id=req.group_id)

        broker.respond_fn(OffsetFetchRequest, handler)
        broker.respond_fn(OffsetFetchRequest, handler)
        result = admin.list_group_offsets({'g1': None, 'g2': None})

        assert captured['count'] == 2
        assert sorted(captured['group_ids']) == ['g1', 'g2']
        assert captured['api_version'] == 7  # negotiated to v7 on 2.8
        assert set(result.keys()) == {'g1', 'g2'}


# ---------------------------------------------------------------------------
# FindCoordinator batching (KIP-699 / v4)
# ---------------------------------------------------------------------------


def _empty_describe_groups_response(group_ids):
    Group = DescribeGroupsResponse.DescribedGroup
    if isinstance(group_ids, str):
        group_ids = [group_ids]
    return DescribeGroupsResponse(
        throttle_time_ms=0,
        groups=[Group(
            error_code=0, error_message=None,
            group_id=gid, group_state='Empty',
            protocol_type='consumer', protocol_data='',
            members=[], authorized_operations=set()) for gid in group_ids])


def _delete_groups_response(group_ids):
    Result = DeleteGroupsResponse.DeletableGroupResult
    return DeleteGroupsResponse(
        throttle_time_ms=0,
        results=[Result(group_id=g, error_code=0) for g in group_ids])


class TestFindCoordinatorBatched:
    def test_v4_batched_single_request_for_many_groups(self, broker, admin):
        captured = {}

        def find_handler(api_key, api_version, correlation_id, request_bytes):
            req = FindCoordinatorRequest.decode(
                request_bytes, version=api_version, header=True)
            captured['api_version'] = api_version
            captured['key_type'] = req.key_type
            captured['coordinator_keys'] = list(req.coordinator_keys)
            captured['count'] = captured.get('count', 0) + 1
            Coordinator = FindCoordinatorResponse.Coordinator
            return FindCoordinatorResponse(
                throttle_time_ms=0,
                error_code=0, error_message=None,
                node_id=0, host='localhost', port=9092,
                coordinators=[Coordinator(
                    key=k, node_id=0, host='localhost', port=9092,
                    error_code=0, error_message=None) for k in req.coordinator_keys])

        broker.respond_fn(FindCoordinatorRequest, find_handler)
        broker.respond(DescribeGroupsRequest,
                       _empty_describe_groups_response(['g1', 'g2', 'g3']))

        admin.describe_groups(['g1', 'g2', 'g3'])

        assert captured['count'] == 1
        assert captured['api_version'] >= 4
        assert captured['key_type'] == 0
        assert sorted(captured['coordinator_keys']) == ['g1', 'g2', 'g3']
        assert admin._manager.cluster._coordinators == {
            (CoordinatorType.GROUP, 'g1'): 0,
            (CoordinatorType.GROUP, 'g2'): 0,
            (CoordinatorType.GROUP, 'g3'): 0,
        }

    def test_v4_batched_per_key_error_raises(self, broker, admin):
        def find_handler(api_key, api_version, correlation_id, request_bytes):
            req = FindCoordinatorRequest.decode(
                request_bytes, version=api_version, header=True)
            Coordinator = FindCoordinatorResponse.Coordinator
            coordinators = []
            for key in req.coordinator_keys:
                if key == 'g2':
                    coordinators.append(Coordinator(
                        key=key, node_id=-1, host='', port=-1,
                        error_code=Errors.GroupAuthorizationFailedError.errno,
                        error_message='nope'))
                else:
                    coordinators.append(Coordinator(
                        key=key, node_id=0, host='localhost', port=9092,
                        error_code=0, error_message=None))
            return FindCoordinatorResponse(
                throttle_time_ms=0,
                error_code=0, error_message=None,
                node_id=0, host='localhost', port=9092,
                coordinators=coordinators)

        broker.respond_fn(FindCoordinatorRequest, find_handler)
        with pytest.raises(Errors.GroupAuthorizationFailedError):
            admin.describe_groups(['g1', 'g2'])

    @pytest.mark.parametrize("broker", [(2, 8, 0)], indirect=True)
    def test_v3_fallback_issues_one_request_per_group(self, broker, admin):
        # Kafka 2.8 caps FindCoordinator at v3 -> no batch, fan out one request
        # per group with the single-key shape.
        captured = {'count': 0, 'keys': [], 'used_coordinator_keys': []}

        def find_handler(api_key, api_version, correlation_id, request_bytes):
            req = FindCoordinatorRequest.decode(
                request_bytes, version=api_version, header=True)
            captured['count'] += 1
            captured['api_version'] = api_version
            captured['keys'].append(req.key)
            captured['used_coordinator_keys'].append(list(req.coordinator_keys))
            return _find_coordinator_response(req.key, node_id=0)

        broker.respond_fn(FindCoordinatorRequest, find_handler)
        broker.respond_fn(FindCoordinatorRequest, find_handler)
        broker.respond_fn(FindCoordinatorRequest, find_handler)
        broker.respond(DescribeGroupsRequest,
                       _empty_describe_groups_response(['g1', 'g2', 'g3']))

        admin.describe_groups(['g1', 'g2', 'g3'])

        assert captured['count'] == 3
        assert captured['api_version'] == 3
        assert sorted(captured['keys']) == ['g1', 'g2', 'g3']
        assert captured['used_coordinator_keys'] == [[], [], []]  # v3 has no batch field

    def test_cache_reuse_skips_already_resolved_groups(self, broker, admin):
        admin._manager.cluster._coordinators[(CoordinatorType.GROUP, 'g1')] = 0
        captured = {}

        def find_handler(api_key, api_version, correlation_id, request_bytes):
            req = FindCoordinatorRequest.decode(
                request_bytes, version=api_version, header=True)
            captured['coordinator_keys'] = list(req.coordinator_keys)
            Coordinator = FindCoordinatorResponse.Coordinator
            return FindCoordinatorResponse(
                throttle_time_ms=0,
                error_code=0, error_message=None,
                node_id=0, host='localhost', port=9092,
                coordinators=[Coordinator(
                    key=k, node_id=0, host='localhost', port=9092,
                    error_code=0, error_message=None) for k in req.coordinator_keys])

        broker.respond_fn(FindCoordinatorRequest, find_handler)
        broker.respond(DescribeGroupsRequest,
                       _empty_describe_groups_response(['g1', 'g2']))

        admin.describe_groups(['g1', 'g2'])

        # Only g2 should have hit the network; g1 came from the cache.
        assert captured['coordinator_keys'] == ['g2']
        assert admin._manager.cluster._coordinators == {
            (CoordinatorType.GROUP, 'g1'): 0,
            (CoordinatorType.GROUP, 'g2'): 0,
        }

    def test_find_coordinator_id_respects_key_type(self, broker, admin):
        # Group and transaction coordinators with the same id must not
        # collide in cluster._coordinators.
        captured = []

        def find_handler(api_key, api_version, correlation_id, request_bytes):
            req = FindCoordinatorRequest.decode(
                request_bytes, version=api_version, header=True)
            captured.append((req.key_type, list(req.coordinator_keys)))
            Coordinator = FindCoordinatorResponse.Coordinator
            # group coordinator -> node 1; transaction coordinator -> node 2
            node_id = 1 if req.key_type == 0 else 2
            return FindCoordinatorResponse(
                throttle_time_ms=0,
                error_code=0, error_message=None,
                node_id=node_id, host='localhost', port=9092,
                coordinators=[Coordinator(
                    key=k, node_id=node_id, host='localhost', port=9092,
                    error_code=0, error_message=None) for k in req.coordinator_keys])

        broker.respond_fn(FindCoordinatorRequest, find_handler)
        broker.respond_fn(FindCoordinatorRequest, find_handler)

        group_node = admin._manager.run(admin._find_coordinator_id, 'x', 0)
        txn_node = admin._manager.run(admin._find_coordinator_id, 'x', 1)

        assert group_node == 1
        assert txn_node == 2
        assert admin._manager.cluster._coordinators == {
            (CoordinatorType.GROUP, 'x'): 1,
            (CoordinatorType.TRANSACTION, 'x'): 2,
        }
        assert captured == [(0, ['x']), (1, ['x'])]

    def test_delete_groups_uses_batched_lookup(self, broker, admin):
        captured = {}

        def find_handler(api_key, api_version, correlation_id, request_bytes):
            req = FindCoordinatorRequest.decode(
                request_bytes, version=api_version, header=True)
            captured['coordinator_keys'] = list(req.coordinator_keys)
            captured['count'] = captured.get('count', 0) + 1
            Coordinator = FindCoordinatorResponse.Coordinator
            return FindCoordinatorResponse(
                throttle_time_ms=0,
                error_code=0, error_message=None,
                node_id=0, host='localhost', port=9092,
                coordinators=[Coordinator(
                    key=k, node_id=0, host='localhost', port=9092,
                    error_code=0, error_message=None) for k in req.coordinator_keys])

        broker.respond_fn(FindCoordinatorRequest, find_handler)
        broker.respond(DeleteGroupsRequest, _delete_groups_response(['g1', 'g2']))

        result = admin.delete_groups(['g1', 'g2'])

        assert captured['count'] == 1
        assert sorted(captured['coordinator_keys']) == ['g1', 'g2']
        assert result == {'g1': 'OK', 'g2': 'OK'}

    def test_list_group_offsets_uses_batched_lookup(self, broker, admin):
        captured = {}

        def find_handler(api_key, api_version, correlation_id, request_bytes):
            req = FindCoordinatorRequest.decode(
                request_bytes, version=api_version, header=True)
            captured['coordinator_keys'] = list(req.coordinator_keys)
            captured['count'] = captured.get('count', 0) + 1
            Coordinator = FindCoordinatorResponse.Coordinator
            return FindCoordinatorResponse(
                throttle_time_ms=0,
                error_code=0, error_message=None,
                node_id=0, host='localhost', port=9092,
                coordinators=[Coordinator(
                    key=k, node_id=0, host='localhost', port=9092,
                    error_code=0, error_message=None) for k in req.coordinator_keys])

        _Group = OffsetFetchResponse.OffsetFetchResponseGroup
        broker.respond_fn(FindCoordinatorRequest, find_handler)
        broker.respond(OffsetFetchRequest, OffsetFetchResponse(
            throttle_time_ms=0, error_code=0, topics=[],
            groups=[
                _Group(group_id='g1', error_code=0, topics=[]),
                _Group(group_id='g2', error_code=0, topics=[]),
            ]))

        admin.list_group_offsets(['g1', 'g2'])

        assert captured['count'] == 1
        assert sorted(captured['coordinator_keys']) == ['g1', 'g2']

    def test_describe_groups_batches_request_per_coordinator(self, broker, admin):
        # All three groups share coordinator 0, so describe_groups should send
        # exactly one DescribeGroups containing all three group_ids.
        admin._manager.cluster._coordinators[(CoordinatorType.GROUP, 'g1')] = 0
        admin._manager.cluster._coordinators[(CoordinatorType.GROUP, 'g2')] = 0
        admin._manager.cluster._coordinators[(CoordinatorType.GROUP, 'g3')] = 0
        captured = {'count': 0, 'groups_per_request': []}

        def describe_handler(api_key, api_version, correlation_id, request_bytes):
            req = DescribeGroupsRequest.decode(
                request_bytes, version=api_version, header=True)
            captured['count'] += 1
            captured['groups_per_request'].append(list(req.groups))
            return _empty_describe_groups_response(list(req.groups))

        broker.respond_fn(DescribeGroupsRequest, describe_handler)

        result = admin.describe_groups(['g1', 'g2', 'g3'])

        assert captured['count'] == 1
        assert sorted(captured['groups_per_request'][0]) == ['g1', 'g2', 'g3']
        assert set(result.keys()) == {'g1', 'g2', 'g3'}
