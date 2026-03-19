import binascii
import pytest
import uuid

from kafka.protocol.new.consumer import FetchRequest, FetchResponse


@pytest.mark.parametrize("version", range(FetchRequest.min_version, FetchRequest.max_version + 1))
def test_fetch_request_roundtrip(version):
    ReplicaState = FetchRequest.ReplicaState
    Topic = FetchRequest.FetchTopic
    Partition = Topic.FetchPartition
    ForgottenTopic = FetchRequest.ForgottenTopic
    request = FetchRequest(
        cluster_id='cluster' if version >= 12 else None,
        replica_id=12 if version <= 14 else -1,
        replica_state=ReplicaState(
            replica_id=12,
            replica_epoch=345,
        ) if version >= 15 else None,
        max_wait_ms=500,
        min_bytes=1,
        isolation_level=2 if version >= 4 else 0,
        session_id=12 if version >= 7 else 0,
        session_epoch=34 if version >= 7 else -1,
        topics=[
            Topic(
                topic="test-topic" if version <= 12 else '',
                topic_id=uuid.uuid4() if version >= 13 else None,
                partitions=[
                    Partition(
                        partition=0,
                        current_leader_epoch=6 if version >= 9 else -1,
                        fetch_offset=100,
                        last_fetched_epoch=8 if version >= 12 else -1,
                        log_start_offset=3 if version >= 5 else -1,
                        partition_max_bytes=1024,
                        replica_directory_id=uuid.uuid4() if version >= 17 else None,
                    )
                ],
            ),
        ],
        forgotten_topics_data=[
            ForgottenTopic(
                topic='foo' if version <= 12 else '',
                topic_id=uuid.uuid4() if version >= 13 else None,
                partitions=[1, 2, 3],
            ),
        ] if version >= 7 else [],
        rack_id='Z' if version >= 11 else '',
    )
    encoded = request.encode(version=version)
    decoded = FetchRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(FetchResponse.min_version, FetchResponse.max_version + 1))
def test_fetch_response_roundtrip(version):
    Response = FetchResponse.FetchableTopicResponse
    PartitionData = Response.PartitionData
    EpochEndOffset = PartitionData.EpochEndOffset
    LeaderIdAndEpoch = PartitionData.LeaderIdAndEpoch
    SnapshotId = PartitionData.SnapshotId
    AbortedTransaction = PartitionData.AbortedTransaction
    NodeEndpoint = FetchResponse.NodeEndpoint
    response = FetchResponse(
        throttle_time_ms=100 if version >= 1 else 0,
        error_code=13 if version >= 7 else 0,
        session_id=12345 if version >= 7 else 0,
        responses=[
            Response(
                topic="test-topic" if version <= 12 else '',
                topic_id=uuid.uuid4() if version >= 13 else None,
                partitions=[
                    PartitionData(
                        partition_index=0,
                        error_code=0,
                        high_watermark=1000,
                        last_stable_offset=3 if version >= 4 else -1,
                        log_start_offset=25 if version >= 5 else -1,
                        diverging_epoch=EpochEndOffset(
                            epoch=1000,
                            end_offset=3,
                        ) if version >= 12 else None,
                        current_leader=LeaderIdAndEpoch(
                            leader_id=5,
                            leader_epoch=99,
                        ) if version >= 12 else None,
                        snapshot_id=SnapshotId(
                            end_offset=44,
                            epoch=88,
                        ) if version >= 12 else None,
                        aborted_transactions=[
                            AbortedTransaction(
                                producer_id=3,
                                first_offset=9,
                            ),
                        ] if version >= 4 else [],
                        preferred_read_replica=12 if version >= 11 else -1,
                        records=b"some records"
                    )
                ]
            )
        ],
        node_endpoints=[
            NodeEndpoint(
                node_id=12,
                host='foo',
                port=1000,
                rack='ZZ',
            ),
        ] if version >= 16 else [],
    )
    encoded = response.encode(version=version)
    decoded = FetchResponse.decode(encoded, version=version)
    assert decoded == response


def test_fetch_request_v15_hex():
    expected_hex = "0001000f0000007b00096d792d636c69656e7400000001f400000001000003e800123456780000007f01010100"
    expected_bytes = binascii.unhexlify(expected_hex)

    req = FetchRequest(
        max_wait_ms=500,
        min_bytes=1,
        max_bytes=1000,
        isolation_level=0,
        session_id=0x12345678,
        session_epoch=0x7f,
        topics=[],
        forgotten_topics_data=[],
        rack_id=""
    )

    req.with_header(correlation_id=123, client_id="my-client")
    encoded = req.encode(version=15, header=True)

    assert encoded == expected_bytes

    # Decoding check
    decoded = FetchRequest.decode(encoded, version=15, header=True)
    assert decoded.header.correlation_id == 123
    assert decoded.header.client_id == "my-client"
    assert decoded.session_id == 0x12345678
    assert decoded.session_epoch == 0x7f
    assert decoded.max_wait_ms == 500
