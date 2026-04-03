import pytest

from kafka.protocol.consumer import (
    ListOffsetsRequest, ListOffsetsResponse,
    UNKNOWN_OFFSET, OffsetResetStrategy,
)


@pytest.mark.parametrize("version", range(ListOffsetsRequest.min_version, ListOffsetsRequest.max_version + 1))
def test_list_offsets_request_roundtrip(version):
    Topic = ListOffsetsRequest.ListOffsetsTopic
    Partition = Topic.ListOffsetsPartition
    request = ListOffsetsRequest(
        replica_id=-1,
        isolation_level=1 if version >= 2 else 0,
        topics=[
            Topic(
                name="topic-1",
                partitions=[
                    Partition(
                        partition_index=1,
                        current_leader_epoch=1 if version >= 4 else -1,
                        timestamp=1000,
                        max_num_offsets=2 if version == 0 else 1
                    )
                ],
            )
        ],
        timeout_ms=1000 if version >= 10 else 0,
    )
    encoded = request.encode(version=version)
    decoded = ListOffsetsRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(ListOffsetsResponse.min_version, ListOffsetsResponse.max_version + 1))
def test_list_offsets_response_roundtrip(version):
    Topic = ListOffsetsResponse.ListOffsetsTopicResponse
    Partition = Topic.ListOffsetsPartitionResponse
    response = ListOffsetsResponse(
        throttle_time_ms=10 if version >= 2 else 0,
        topics=[
            Topic(
                name="topic-1",
                partitions=[
                    Partition(
                        partition_index=1,
                        error_code=2,
                        old_style_offsets=[0] if version == 0 else [],
                        timestamp=1000 if version >= 1 else -1,
                        offset=1000 if version >= 1 else -1,
                        leader_epoch=1 if version >= 4 else -1
                    ),
                ],
            ),
        ],
    )
    encoded = response.encode(version=version)
    decoded = ListOffsetsResponse.decode(encoded, version=version)
    assert decoded == response


def test_unknown_offset():
    assert UNKNOWN_OFFSET == -1


def test_offset_reset_strategies():
    assert OffsetResetStrategy.LATEST == -1
    assert OffsetResetStrategy.EARLIEST == -2
    assert OffsetResetStrategy.NONE == 0

