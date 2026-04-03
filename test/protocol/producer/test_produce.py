import pytest

from kafka.protocol.producer import ProduceRequest, ProduceResponse


@pytest.mark.parametrize("version", range(ProduceRequest.min_version, ProduceRequest.max_version + 1))
def test_produce_request_roundtrip(version):
    Topic = ProduceRequest.TopicProduceData
    Partition = Topic.PartitionProduceData
    request = ProduceRequest(
        transactional_id="txn-id" if version >= 3 else None,
        acks=-1,
        timeout_ms=500,
        topic_data=[
            Topic(
                name="topic-1",
                partition_data=[
                    Partition(
                        index=1,
                        records=b"some binary records data"
                    ),
                ],
            ),
        ],
    )
    encoded = request.encode(version=version)
    decoded = ProduceRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(ProduceResponse.min_version, ProduceResponse.max_version + 1))
def test_produce_response_roundtrip(version):
    Topic = ProduceResponse.TopicProduceResponse
    Partition = Topic.PartitionProduceResponse
    BatchError = Partition.BatchIndexAndErrorMessage
    LeaderIdAndEpoch = Partition.LeaderIdAndEpoch
    response = ProduceResponse(
        responses=[
            Topic(
                name="topic-1",
                partition_responses=[
                    Partition(
                        index=1,
                        error_code=3,
                        base_offset=12345,
                        log_append_time_ms=1000 if version >= 2 else -1,
                        log_start_offset=100 if version >= 5 else -1,
                        record_errors=[
                            BatchError(
                                batch_index=1,
                                batch_index_error_message='foo',
                            ),
                        ] if version >= 8 else [],
                        error_message='error' if version >= 8 else None,
                        current_leader=LeaderIdAndEpoch(
                            leader_id=12,
                            leader_epoch=333,
                        ) if version >= 10 else None,
                    ),
                ],
            ),
        ],
        throttle_time_ms=10 if version >= 1 else 0
    )
    encoded = response.encode(version=version)
    decoded = ProduceResponse.decode(encoded, version=version)
    assert decoded == response
