import pytest

from kafka.protocol.new.messages.metadata import FindCoordinatorRequest, FindCoordinatorResponse
from kafka.protocol.new.messages.consumer import (
    JoinGroupRequest, JoinGroupResponse,
    SyncGroupRequest, SyncGroupResponse,
    LeaveGroupRequest, LeaveGroupResponse,
    HeartbeatRequest, HeartbeatResponse,
    OffsetFetchRequest, OffsetFetchResponse,
    OffsetCommitRequest, OffsetCommitResponse,
)


@pytest.mark.parametrize("version", range(FindCoordinatorRequest.min_version, FindCoordinatorRequest.max_version + 1))
def test_find_coordinator_request_roundtrip(version):
    data = FindCoordinatorRequest(
        key="test-group" if version < 4 else "",
        key_type=0,
        coordinator_keys=["test-group"] if version >= 4 else []
    )
    encoded = FindCoordinatorRequest.encode(data, version=version)
    decoded = FindCoordinatorRequest.decode(encoded, version=version)
    assert decoded == data


@pytest.mark.parametrize("version", range(FindCoordinatorResponse.min_version, FindCoordinatorResponse.max_version + 1))
def test_find_coordinator_response_roundtrip(version):
    coordinators = [
        FindCoordinatorResponse.Coordinator(
            key="test-group",
            node_id=1,
            host="localhost",
            port=9092,
            error_code=0,
            error_message=None
        )
    ]
    data = FindCoordinatorResponse(
        throttle_time_ms=100 if version >= 2 else 0,
        error_code=0,
        error_message="", 
        node_id=1 if version < 4 else 0,
        host="localhost" if version < 4 else "",
        port=9092 if version < 4 else 0,
        coordinators=coordinators if version >= 4 else []
    )
    encoded = FindCoordinatorResponse.encode(data, version=version)
    decoded = FindCoordinatorResponse.decode(encoded, version=version)
    assert decoded == data


@pytest.mark.parametrize("version", range(JoinGroupRequest.min_version, JoinGroupRequest.max_version + 1))
def test_join_group_request_roundtrip(version):
    protocols = [
        JoinGroupRequest.JoinGroupRequestProtocol(
            name="range",
            metadata=b"protocol-metadata"
        )
    ]
    data = JoinGroupRequest(
        group_id="test-group",
        session_timeout_ms=30000,
        rebalance_timeout_ms=60000 if version >= 1 else -1,
        member_id="test-member",
        group_instance_id=None,
        protocol_type="consumer",
        protocols=protocols,
        reason="joining" if version >= 8 else None
    )
    encoded = JoinGroupRequest.encode(data, version=version)
    decoded = JoinGroupRequest.decode(encoded, version=version)
    assert decoded == data


@pytest.mark.parametrize("version", range(JoinGroupResponse.min_version, JoinGroupResponse.max_version + 1))
def test_join_group_response_roundtrip(version):
    members = [
        JoinGroupResponse.JoinGroupResponseMember(
            member_id="test-member",
            group_instance_id=None,
            metadata=b"member-metadata"
        )
    ]
    data = JoinGroupResponse(
        throttle_time_ms=100 if version >= 3 else 0,
        error_code=0,
        generation_id=1,
        protocol_type="consumer" if version >= 7 else None,
        protocol_name="range",
        leader="test-member",
        member_id="test-member",
        members=members
    )
    encoded = JoinGroupResponse.encode(data, version=version)
    decoded = JoinGroupResponse.decode(encoded, version=version)
    assert decoded == data


@pytest.mark.parametrize("version", range(SyncGroupRequest.min_version, SyncGroupRequest.max_version + 1))
def test_sync_group_request_roundtrip(version):
    assignments = [
        SyncGroupRequest.SyncGroupRequestAssignment(
            member_id="test-member",
            assignment=b"test-assignment"
        )
    ]
    data = SyncGroupRequest(
        group_id="test-group",
        generation_id=1,
        member_id="test-member",
        group_instance_id=None,
        assignments=assignments if version < 5 else [],
        protocol_type="consumer" if version >= 5 else None,
        protocol_name="range" if version >= 5 else None
    )
    encoded = SyncGroupRequest.encode(data, version=version)
    decoded = SyncGroupRequest.decode(encoded, version=version)
    assert decoded == data


@pytest.mark.parametrize("version", range(SyncGroupResponse.min_version, SyncGroupResponse.max_version + 1))
def test_sync_group_response_roundtrip(version):
    data = SyncGroupResponse(
        throttle_time_ms=100 if version >= 1 else 0,
        error_code=0,
        protocol_type="consumer" if version >= 5 else None,
        protocol_name="range" if version >= 5 else None,
        assignment=b"test-assignment"
    )
    encoded = SyncGroupResponse.encode(data, version=version)
    decoded = SyncGroupResponse.decode(encoded, version=version)
    assert decoded == data


@pytest.mark.parametrize("version", range(HeartbeatRequest.min_version, HeartbeatRequest.max_version + 1))
def test_heartbeat_request_roundtrip(version):
    data = HeartbeatRequest(
        group_id="test-group",
        generation_id=1,
        member_id="test-member",
        group_instance_id=None
    )
    encoded = HeartbeatRequest.encode(data, version=version)
    decoded = HeartbeatRequest.decode(encoded, version=version)
    assert decoded == data


@pytest.mark.parametrize("version", range(HeartbeatResponse.min_version, HeartbeatResponse.max_version + 1))
def test_heartbeat_response_roundtrip(version):
    data = HeartbeatResponse(
        throttle_time_ms=100 if version >= 1 else 0,
        error_code=0
    )
    encoded = HeartbeatResponse.encode(data, version=version)
    decoded = HeartbeatResponse.decode(encoded, version=version)
    assert decoded == data


@pytest.mark.parametrize("version", range(LeaveGroupRequest.min_version, LeaveGroupRequest.max_version + 1))
def test_leave_group_request_roundtrip(version):
    members = [
        LeaveGroupRequest.MemberIdentity(
            member_id="test-member",
            group_instance_id=None,
            reason="leaving" if version >= 5 else None
        )
    ]
    data = LeaveGroupRequest(
        group_id="test-group",
        member_id="test-member" if version < 3 else "",
        members=members if version >= 3 else []
    )
    encoded = LeaveGroupRequest.encode(data, version=version)
    decoded = LeaveGroupRequest.decode(encoded, version=version)
    assert decoded == data


@pytest.mark.parametrize("version", range(LeaveGroupResponse.min_version, LeaveGroupResponse.max_version + 1))
def test_leave_group_response_roundtrip(version):
    members = [
        LeaveGroupResponse.MemberResponse(
            member_id="test-member",
            group_instance_id=None,
            error_code=0
        )
    ]
    data = LeaveGroupResponse(
        throttle_time_ms=100 if version >= 1 else 0,
        error_code=0,
        members=members if version >= 3 else []
    )
    encoded = LeaveGroupResponse.encode(data, version=version)
    decoded = LeaveGroupResponse.decode(encoded, version=version)
    assert decoded == data


@pytest.mark.parametrize("version", range(OffsetFetchRequest.min_version, OffsetFetchRequest.max_version + 1))
def test_offset_fetch_request_roundtrip(version):
    if version < 8:
        topics = [
            OffsetFetchRequest.OffsetFetchRequestTopic(
                name="topic-1",
                partition_indexes=[0, 1]
            )
        ]
        data = OffsetFetchRequest(
            group_id="test-group",
            topics=topics,
            require_stable=False if version >= 7 else False
        )
    else:
        groups = [
            OffsetFetchRequest.OffsetFetchRequestGroup(
                group_id="test-group",
                topics=[
                    OffsetFetchRequest.OffsetFetchRequestGroup.OffsetFetchRequestTopics(
                        name="topic-1",
                        partition_indexes=[0, 1]
                    )
                ]
            )
        ]
        data = OffsetFetchRequest(
            groups=groups,
            require_stable=False
        )

    encoded = OffsetFetchRequest.encode(data, version=version)
    decoded = OffsetFetchRequest.decode(encoded, version=version)
    assert decoded == data


@pytest.mark.parametrize("version", range(OffsetFetchResponse.min_version, OffsetFetchResponse.max_version + 1))
def test_offset_fetch_response_roundtrip(version):
    if version < 8:
        topics = [
            OffsetFetchResponse.OffsetFetchResponseTopic(
                name="topic-1",
                partitions=[
                    OffsetFetchResponse.OffsetFetchResponseTopic.OffsetFetchResponsePartition(
                        partition_index=0,
                        committed_offset=100,
                        committed_leader_epoch=1 if version >= 5 else -1,
                        metadata="meta",
                        error_code=0
                    )
                ]
            )
        ]
        data = OffsetFetchResponse(
            throttle_time_ms=100 if version >= 3 else 0,
            topics=topics,
            error_code=0 if version >= 2 else 0
        )
    else:
        groups = [
            OffsetFetchResponse.OffsetFetchResponseGroup(
                group_id="test-group",
                topics=[
                    OffsetFetchResponse.OffsetFetchResponseGroup.OffsetFetchResponseTopics(
                        name="topic-1",
                        partitions=[
                            OffsetFetchResponse.OffsetFetchResponseGroup.OffsetFetchResponseTopics.OffsetFetchResponsePartitions(
                                partition_index=0,
                                committed_offset=100,
                                committed_leader_epoch=1,
                                metadata="meta",
                                error_code=0
                            )
                        ]
                    )
                ],
                error_code=0
            )
        ]
        data = OffsetFetchResponse(
            throttle_time_ms=100,
            groups=groups
        )

    encoded = OffsetFetchResponse.encode(data, version=version)
    decoded = OffsetFetchResponse.decode(encoded, version=version)
    assert decoded == data


@pytest.mark.parametrize("version", range(OffsetCommitRequest.min_version, OffsetCommitRequest.max_version + 1))
def test_offset_commit_request_roundtrip(version):
    topics = [
        OffsetCommitRequest.OffsetCommitRequestTopic(
            name="topic-1",
            partitions=[
                OffsetCommitRequest.OffsetCommitRequestTopic.OffsetCommitRequestPartition(
                    partition_index=0,
                    committed_offset=100,
                    committed_leader_epoch=1 if version >= 6 else -1,
                    committed_metadata="meta"
                )
            ]
        )
    ]
    data = OffsetCommitRequest(
        group_id="test-group",
        generation_id_or_member_epoch=1 if version >= 1 else -1,
        member_id="test-member" if version >= 1 else "",
        group_instance_id=None,
        retention_time_ms=5000 if 2 <= version <= 4 else -1,
        topics=topics
    )
    encoded = OffsetCommitRequest.encode(data, version=version)
    decoded = OffsetCommitRequest.decode(encoded, version=version)
    assert decoded == data


@pytest.mark.parametrize("version", range(OffsetCommitResponse.min_version, OffsetCommitResponse.max_version + 1))
def test_offset_commit_response_roundtrip(version):
    topics = [
        OffsetCommitResponse.OffsetCommitResponseTopic(
            name="topic-1",
            partitions=[
                OffsetCommitResponse.OffsetCommitResponseTopic.OffsetCommitResponsePartition(
                    partition_index=0,
                    error_code=0
                )
            ]
        )
    ]
    data = OffsetCommitResponse(
        throttle_time_ms=100 if version >= 3 else 0,
        topics=topics
    )
    encoded = OffsetCommitResponse.encode(data, version=version)
    decoded = OffsetCommitResponse.decode(encoded, version=version)
    assert decoded == data
