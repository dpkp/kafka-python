import binascii
import pytest
import uuid

from kafka.protocol.new.consumer import FetchRequest, FetchResponse


def test_fetch_request_v15_hex():
    # Hex dump provided by user
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


@pytest.mark.parametrize("version", range(FetchRequest.min_version, FetchRequest.max_version + 1))
def test_fetch_request_roundtrip(version):
    # Topic data needs to match the version's requirements (Topic vs TopicId)
    topic_data = []
    if version < 13:
        topic_data = [
            FetchRequest.FetchTopic(
                topic="test-topic",
                partitions=[
                    FetchRequest.FetchTopic.FetchPartition(
                        partition=0,
                        fetch_offset=100,
                        partition_max_bytes=1024
                    )
                ]
            )
        ]
    else:
        topic_id = uuid.uuid4()
        topic_data = [
            FetchRequest.FetchTopic(
                topic_id=topic_id,
                partitions=[
                    FetchRequest.FetchTopic.FetchPartition(
                        partition=0,
                        fetch_offset=100,
                        partition_max_bytes=1024
                    )
                ]
            )
        ]

    data = FetchRequest(
        replica_id=-1,
        max_wait_ms=500,
        min_bytes=1,
        topics=topic_data
    )

    encoded = data.encode(version=version)
    decoded = FetchRequest.decode(encoded, version=version)
    
    assert decoded == data


@pytest.mark.parametrize("version", range(FetchResponse.min_version, FetchResponse.max_version + 1))
def test_fetch_response_roundtrip(version):
    # Mocking some response data
    resp_topic_data = []
    if version < 13:
        resp_topic_data = [
            FetchResponse.FetchableTopicResponse(
                topic="test-topic",
                partitions=[
                    FetchResponse.FetchableTopicResponse.PartitionData(
                        partition_index=0,
                        error_code=0,
                        high_watermark=1000,
                        records=b"some records"
                    )
                ]
            )
        ]
    else:
        topic_id = uuid.uuid4()
        resp_topic_data = [
            FetchResponse.FetchableTopicResponse(
                topic_id=topic_id,
                partitions=[
                    FetchResponse.FetchableTopicResponse.PartitionData(
                        partition_index=0,
                        error_code=0,
                        high_watermark=1000,
                        records=b"some records"
                    )
                ]
            )
        ]

    data = FetchResponse(
        throttle_time_ms=100 if version >= 1 else 0,
        error_code=0,
        session_id=12345 if version >= 7 else 0,
        responses=resp_topic_data
    )

    encoded = data.encode(version=version)
    decoded = FetchResponse.decode(encoded, version=version)
    
    assert decoded == data
