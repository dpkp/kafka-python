import pytest

from kafka.protocol.new.consumer import (
    ListOffsetsRequest, ListOffsetsResponse,
    UNKNOWN_OFFSET, OffsetResetStrategy,
)


@pytest.mark.parametrize("version", range(ListOffsetsRequest.min_version, ListOffsetsRequest.max_version + 1))
def test_list_offsets_request_roundtrip(version):
    topics = [
        ListOffsetsRequest.ListOffsetsTopic(
            name="topic-1",
            partitions=[
                ListOffsetsRequest.ListOffsetsTopic.ListOffsetsPartition(
                    partition_index=0,
                    current_leader_epoch=1 if version >= 9 else -1,
                    timestamp=1000,
                    max_num_offsets=2 if version == 0 else 1
                )
            ],
        )
    ]
    data = ListOffsetsRequest(
        replica_id=-1,
        isolation_level=0, 
        topics=topics
    )

    encoded = ListOffsetsRequest.encode(data, version=version)
    decoded = ListOffsetsRequest.decode(encoded, version=version)
    assert decoded == data


@pytest.mark.parametrize("version", range(ListOffsetsResponse.min_version, ListOffsetsResponse.max_version + 1))
def test_list_offsets_response_roundtrip(version):
    topics = [
        ListOffsetsResponse.ListOffsetsTopicResponse(
            name="topic-1",
            partitions=[
                ListOffsetsResponse.ListOffsetsTopicResponse.ListOffsetsPartitionResponse(
                    partition_index=0,
                    error_code=0,
                    old_style_offsets=[0] if version == 0 else [],
                    timestamp=1000 if version >= 1 else -1,
                    offset=1000 if version >= 1 else -1,
                    leader_epoch=1 if version >= 4 else -1
                )
            ],
        )
    ]
    data = ListOffsetsResponse(
        throttle_time_ms=10 if version >= 2 else 0,
        topics=topics
    )

    encoded = ListOffsetsResponse.encode(data, version=version)
    decoded = ListOffsetsResponse.decode(encoded, version=version)
    assert decoded == data


def test_unknown_offset():
    assert UNKNOWN_OFFSET == -1


def test_offset_reset_strategies():
    assert OffsetResetStrategy.LATEST == -1
    assert OffsetResetStrategy.EARLIEST == -2
    assert OffsetResetStrategy.NONE == 0

