import pytest

from kafka.protocol.new.messages.produce import ProduceRequest, ProduceResponse


@pytest.mark.parametrize("version", range(ProduceRequest.min_version, ProduceRequest.max_version + 1))
def test_produce_request_roundtrip(version):
    topic_data = [
        ProduceRequest.TopicProduceData(
            name="topic-1",
            partition_data=[
                ProduceRequest.TopicProduceData.PartitionProduceData(
                    index=0,
                    records=b"some binary records data"
                )
            ]
        )
    ]
    data = ProduceRequest(
        transactional_id="trans-id" if version >= 3 else None,
        acks=-1,
        timeout_ms=500,
        topic_data=topic_data
    )

    encoded = ProduceRequest.encode(data, version=version)
    decoded = ProduceRequest.decode(encoded, version=version)
    assert decoded == data


@pytest.mark.parametrize("version", range(ProduceResponse.min_version, ProduceResponse.max_version + 1))
def test_produce_response_roundtrip(version):
    responses = [
        ProduceResponse.TopicProduceResponse(
            name="topic-1",
            partition_responses=[
                ProduceResponse.TopicProduceResponse.PartitionProduceResponse(
                    index=0,
                    error_code=0,
                    base_offset=12345,
                    log_append_time_ms=1000 if version >= 2 else -1,
                    log_start_offset=100 if version >= 5 else -1
                )
            ]
        )
    ]
    data = ProduceResponse(
        responses=responses,
        throttle_time_ms=10 if version >= 1 else 0
    )

    encoded = ProduceResponse.encode(data, version=version)
    decoded = ProduceResponse.decode(encoded, version=version)
    assert decoded == data
