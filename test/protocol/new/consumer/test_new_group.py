import pytest

from kafka.protocol.new.metadata import FindCoordinatorRequest, FindCoordinatorResponse
from kafka.protocol.new.consumer import (
    DEFAULT_GENERATION_ID, UNKNOWN_MEMBER_ID,
    JoinGroupRequest, JoinGroupResponse,
    SyncGroupRequest, SyncGroupResponse,
    LeaveGroupRequest, LeaveGroupResponse,
    HeartbeatRequest, HeartbeatResponse,
    OffsetFetchRequest, OffsetFetchResponse,
    OffsetCommitRequest, OffsetCommitResponse,
)


@pytest.mark.parametrize("version", range(FindCoordinatorRequest.min_version, FindCoordinatorRequest.max_version + 1))
def test_find_coordinator_request_roundtrip(version):
    request = FindCoordinatorRequest(
        key="test-group" if version < 4 else "",
        key_type=1 if version >= 1 else 0,
        coordinator_keys=["test-group"] if version >= 4 else []
    )
    encoded = request.encode(version=version)
    decoded = FindCoordinatorRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(FindCoordinatorResponse.min_version, FindCoordinatorResponse.max_version + 1))
def test_find_coordinator_response_roundtrip(version):
    Coordinator = FindCoordinatorResponse.Coordinator
    response = FindCoordinatorResponse(
        throttle_time_ms=100 if version >= 2 else 0,
        error_code=12 if version <= 3 else 0,
        error_message="error" if 1 <= version <= 3 else "", 
        node_id=1 if version <= 3 else 0,
        host="localhost" if version <= 3 else "",
        port=9092 if version <= 3 else 0,
        coordinators=[
            Coordinator(
                key="test-group",
                node_id=1,
                host="localhost",
                port=9092,
                error_code=9,
                error_message='error',
            )
        ] if version >= 4 else [],
    )
    encoded = response.encode(version=version)
    decoded = FindCoordinatorResponse.decode(encoded, version=version)
    assert decoded == response


@pytest.mark.parametrize("version", range(JoinGroupRequest.min_version, JoinGroupRequest.max_version + 1))
def test_join_group_request_roundtrip(version):
    Protocol = JoinGroupRequest.JoinGroupRequestProtocol
    request = JoinGroupRequest(
        group_id="test-group",
        session_timeout_ms=30000,
        rebalance_timeout_ms=60000 if version >= 1 else -1,
        member_id="test-member",
        group_instance_id="group-instance-id" if version >= 5 else None,
        protocol_type="consumer",
        protocols=[
            Protocol(
                name="range",
                metadata=b"protocol-metadata"
            ),
        ],
        reason="joining" if version >= 8 else None
    )
    encoded = request.encode(version=version)
    decoded = JoinGroupRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(JoinGroupResponse.min_version, JoinGroupResponse.max_version + 1))
def test_join_group_response_roundtrip(version):
    Member = JoinGroupResponse.JoinGroupResponseMember
    response = JoinGroupResponse(
        throttle_time_ms=100 if version >= 3 else 0,
        error_code=12,
        generation_id=1,
        protocol_type="consumer" if version >= 7 else None,
        protocol_name="range",
        leader="test-member",
        skip_assignment=True if version >= 9 else False,
        member_id="test-member",
        members=[
            Member(
                member_id="test-member",
                group_instance_id="group-instance-id" if version >= 5 else None,
                metadata=b"member-metadata",
            )
        ],
    )
    encoded = response.encode(version=version)
    decoded = JoinGroupResponse.decode(encoded, version=version)
    assert decoded == response


@pytest.mark.parametrize("version", range(SyncGroupRequest.min_version, SyncGroupRequest.max_version + 1))
def test_sync_group_request_roundtrip(version):
    Assignment = SyncGroupRequest.SyncGroupRequestAssignment
    request = SyncGroupRequest(
        group_id="test-group",
        generation_id=1,
        member_id="test-member",
        group_instance_id="group-instance-id" if version >= 3 else None,
        protocol_type='consumer' if version >= 5 else None,
        protocol_name='range' if version >= 5 else None,
        assignments=[
            Assignment(
                member_id="test-member",
                assignment=b"test-assignment"
            ),
        ] if version <= 4 else [],
    )
    encoded = request.encode(version=version)
    decoded = SyncGroupRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(SyncGroupResponse.min_version, SyncGroupResponse.max_version + 1))
def test_sync_group_response_roundtrip(version):
    response = SyncGroupResponse(
        throttle_time_ms=100 if version >= 1 else 0,
        error_code=1,
        protocol_type="consumer" if version >= 5 else None,
        protocol_name="range" if version >= 5 else None,
        assignment=b"test-assignment"
    )
    encoded = response.encode(version=version)
    decoded = SyncGroupResponse.decode(encoded, version=version)
    assert decoded == response


@pytest.mark.parametrize("version", range(HeartbeatRequest.min_version, HeartbeatRequest.max_version + 1))
def test_heartbeat_request_roundtrip(version):
    request = HeartbeatRequest(
        group_id="test-group",
        generation_id=1,
        member_id="test-member",
        group_instance_id="group-instance-id" if version >= 3 else None,
    )
    encoded = request.encode(version=version)
    decoded = HeartbeatRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(HeartbeatResponse.min_version, HeartbeatResponse.max_version + 1))
def test_heartbeat_response_roundtrip(version):
    response = HeartbeatResponse(
        throttle_time_ms=100 if version >= 1 else 0,
        error_code=33,
    )
    encoded = response.encode(version=version)
    decoded = HeartbeatResponse.decode(encoded, version=version)
    assert decoded == response


@pytest.mark.parametrize("version", range(LeaveGroupRequest.min_version, LeaveGroupRequest.max_version + 1))
def test_leave_group_request_roundtrip(version):
    Member = LeaveGroupRequest.MemberIdentity
    request = LeaveGroupRequest(
        group_id="test-group",
        member_id="test-member" if version <= 2 else "",
        members=[
            Member(
                member_id="test-member",
                group_instance_id="group-instance-id",
                reason="leaving" if version >= 5 else None
            )
        ] if version >= 3 else []
    )
    encoded = request.encode(version=version)
    decoded = LeaveGroupRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(LeaveGroupResponse.min_version, LeaveGroupResponse.max_version + 1))
def test_leave_group_response_roundtrip(version):
    Member = LeaveGroupResponse.MemberResponse
    response = LeaveGroupResponse(
        throttle_time_ms=100 if version >= 1 else 0,
        error_code=1,
        members=[
            Member(
                member_id="test-member",
                group_instance_id="group-instance-id",
                error_code=0
            )
        ] if version >= 3 else []
    )
    encoded = response.encode(version=version)
    decoded = LeaveGroupResponse.decode(encoded, version=version)
    assert decoded == response


@pytest.mark.parametrize("version", range(OffsetFetchRequest.min_version, OffsetFetchRequest.max_version + 1))
def test_offset_fetch_request_roundtrip(version):
    Topic = OffsetFetchRequest.OffsetFetchRequestTopic
    Group = OffsetFetchRequest.OffsetFetchRequestGroup
    GroupTopic = Group.OffsetFetchRequestTopics
    request = OffsetFetchRequest(
        group_id="test-group" if version <= 7 else "",
        topics=[
            Topic(
                name="topic-1",
                partition_indexes=[0, 1],
            )
        ] if version <= 7 else [],
        groups=[
            Group(
                group_id="test-group",
                member_id="foo-member" if version >= 9 else None,
                member_epoch=12 if version >= 9 else -1,
                topics=[
                    GroupTopic(
                        name="topic-1",
                        partition_indexes=[0, 1]
                    )
                ]
            )
        ] if version >= 8 else [],
        require_stable=True if version >= 7 else False,
    )
    encoded = request.encode(version=version)
    decoded = OffsetFetchRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(OffsetFetchResponse.min_version, OffsetFetchResponse.max_version + 1))
def test_offset_fetch_response_roundtrip(version):
    Topic = OffsetFetchResponse.OffsetFetchResponseTopic
    Partition = Topic.OffsetFetchResponsePartition
    Group = OffsetFetchResponse.OffsetFetchResponseGroup
    GroupTopic = Group.OffsetFetchResponseTopics
    GroupPartition = GroupTopic.OffsetFetchResponsePartitions
    response = OffsetFetchResponse(
        throttle_time_ms=100 if version >= 3 else 0,
        topics=[
            Topic(
                name="topic-1",
                partitions=[
                    Partition(
                        partition_index=1,
                        committed_offset=100,
                        committed_leader_epoch=1 if version >= 5 else -1,
                        metadata="meta",
                        error_code=9,
                    ),
                ]
            )
        ] if version <= 7 else [],
        error_code=9 if 2 <= version <= 7 else 0,
        groups=[
            Group(
                group_id="test-group",
                topics=[
                    GroupTopic(
                        name="topic-1",
                        partitions=[
                            GroupPartition(
                                partition_index=1,
                                committed_offset=100,
                                committed_leader_epoch=1,
                                metadata="meta",
                                error_code=9,
                            ),
                        ],
                    ),
                ],
                error_code=1,
            )
        ] if version >= 8 else [],
    )
    encoded = response.encode(version=version)
    decoded = OffsetFetchResponse.decode(encoded, version=version)
    assert decoded == response


@pytest.mark.parametrize("version", range(OffsetCommitRequest.min_version, OffsetCommitRequest.max_version + 1))
def test_offset_commit_request_roundtrip(version):
    Topic = OffsetCommitRequest.OffsetCommitRequestTopic
    Partition = Topic.OffsetCommitRequestPartition
    request = OffsetCommitRequest(
        group_id="test-group",
        generation_id_or_member_epoch=1 if version >= 1 else -1,
        member_id="test-member" if version >= 1 else "",
        group_instance_id="group-instance-id" if version >= 7 else None,
        retention_time_ms=5000 if 2 <= version <= 4 else -1,
        topics=[
            Topic(
                name="topic-1",
                partitions=[
                    Partition(
                        partition_index=0,
                        committed_offset=100,
                        committed_leader_epoch=1 if version >= 6 else -1,
                        commit_timestamp=1 if version == 1 else -1,
                        committed_metadata="meta"
                    )
                ]
            )
        ],
    )
    encoded = request.encode(version=version)
    decoded = OffsetCommitRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(OffsetCommitResponse.min_version, OffsetCommitResponse.max_version + 1))
def test_offset_commit_response_roundtrip(version):
    Topic = OffsetCommitResponse.OffsetCommitResponseTopic
    Partition = Topic.OffsetCommitResponsePartition
    response = OffsetCommitResponse(
        throttle_time_ms=100 if version >= 3 else 0,
        topics=[
            Topic(
                name="topic-1",
                partitions=[
                    Partition(
                        partition_index=10,
                        error_code=3,
                    ),
                ],
            ),
        ],
    )
    encoded = response.encode(version=version)
    decoded = OffsetCommitResponse.decode(encoded, version=version)
    assert decoded == response


def test_default_generation_id():
    assert DEFAULT_GENERATION_ID == -1


def test_unknown_member_id():
    assert UNKNOWN_MEMBER_ID == ''
