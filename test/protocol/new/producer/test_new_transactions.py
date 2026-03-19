import pytest

from kafka.protocol.new.producer import (
    InitProducerIdRequest, InitProducerIdResponse,
    AddPartitionsToTxnRequest, AddPartitionsToTxnResponse,
    AddOffsetsToTxnRequest, AddOffsetsToTxnResponse,
    EndTxnRequest, EndTxnResponse,
    TxnOffsetCommitRequest, TxnOffsetCommitResponse,
)


@pytest.mark.parametrize("version", range(InitProducerIdRequest.min_version, InitProducerIdRequest.max_version + 1))
def test_init_producer_id_request_roundtrip(version):
    request = InitProducerIdRequest(
        transactional_id="test-txn",
        transaction_timeout_ms=10000,
        producer_id=12345 if version >= 3 else -1,
        producer_epoch=1 if version >= 3 else -1,
    )
    encoded = request.encode(version=version)
    decoded = InitProducerIdRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(InitProducerIdResponse.min_version, InitProducerIdResponse.max_version + 1))
def test_init_producer_id_response_roundtrip(version):
    response = InitProducerIdResponse(
        throttle_time_ms=100 if version >= 1 else 0,
        error_code=12,
        producer_id=12345,
        producer_epoch=1
    )
    encoded = response.encode(version=version)
    decoded = InitProducerIdResponse.decode(encoded, version=version)
    assert decoded == response


@pytest.mark.parametrize("version", range(AddPartitionsToTxnRequest.min_version, AddPartitionsToTxnRequest.max_version + 1))
def test_add_partitions_to_txn_request_roundtrip(version):
    Topic = AddPartitionsToTxnRequest.AddPartitionsToTxnTopic
    Transaction = AddPartitionsToTxnRequest.AddPartitionsToTxnTransaction
    TransactionTopic = Transaction.AddPartitionsToTxnTopic
    request = AddPartitionsToTxnRequest(
        transactions=[
            Transaction(
                transactional_id="test-txn",
                producer_id=12345,
                producer_epoch=1,
                verify_only=True,
                topics=[
                    TransactionTopic(
                        name="topic-1",
                        partitions=[0, 1]
                    ),
                ],
            ),
        ] if version >= 4 else [],
        v3_and_below_transactional_id="test-txn" if version <= 3 else "",
        v3_and_below_producer_id=12345 if version <= 3 else 0,
        v3_and_below_producer_epoch=1 if version <= 3 else 0,
        v3_and_below_topics=[
            Topic(
                name="topic-1",
                partitions=[0, 1]
            ),
        ] if version <= 3 else [],
    )
    encoded = request.encode(version=version)
    decoded = AddPartitionsToTxnRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(AddPartitionsToTxnResponse.min_version, AddPartitionsToTxnResponse.max_version + 1))
def test_add_partitions_to_txn_response_roundtrip(version):
    TopicResult = AddPartitionsToTxnResponse.AddPartitionsToTxnTopicResult
    TopicPartitionResult = TopicResult.AddPartitionsToTxnPartitionResult
    Result = AddPartitionsToTxnResponse.AddPartitionsToTxnResult
    ResultTopicResult = Result.AddPartitionsToTxnTopicResult
    ResultTopicPartitionResult = ResultTopicResult.AddPartitionsToTxnPartitionResult
    response = AddPartitionsToTxnResponse(
        throttle_time_ms=100 if version >= 1 else 0,
        error_code=13 if version >= 4 else 0,
        results_by_transaction=[
            Result(
                transactional_id="test-txn",
                topic_results=[
                    ResultTopicResult(
                        name="topic-1",
                        results_by_partition=[
                            ResultTopicPartitionResult(
                                partition_index=1,
                                partition_error_code=3
                            ),
                        ],
                    ),
                ],
            ),
        ] if version >= 4 else [],
        results_by_topic_v3_and_below=[
            TopicResult(
                name="topic-1",
                results_by_partition=[
                    TopicPartitionResult(
                        partition_index=1,
                        partition_error_code=3
                    ),
                ],
            ),
        ] if version <= 3 else [],
    )
    encoded = response.encode(version=version)
    decoded = AddPartitionsToTxnResponse.decode(encoded, version=version)
    assert decoded == response


@pytest.mark.parametrize("version", range(AddOffsetsToTxnRequest.min_version, AddOffsetsToTxnRequest.max_version + 1))
def test_add_offsets_to_txn_request_roundtrip(version):
    request = AddOffsetsToTxnRequest(
        transactional_id="test-txn",
        producer_id=12345,
        producer_epoch=1,
        group_id="test-group"
    )
    encoded = request.encode(version=version)
    decoded = AddOffsetsToTxnRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(AddOffsetsToTxnResponse.min_version, AddOffsetsToTxnResponse.max_version + 1))
def test_add_offsets_to_txn_response_roundtrip(version):
    response = AddOffsetsToTxnResponse(
        throttle_time_ms=100 if version >= 1 else 0,
        error_code=9,
    )
    encoded = response.encode(version=version)
    decoded = AddOffsetsToTxnResponse.decode(encoded, version=version)
    assert decoded == response


@pytest.mark.parametrize("version", range(EndTxnRequest.min_version, EndTxnRequest.max_version + 1))
def test_end_txn_request_roundtrip(version):
    request = EndTxnRequest(
        transactional_id="test-txn",
        producer_id=12345,
        producer_epoch=1,
        committed=True
    )
    encoded = request.encode(version=version)
    decoded = EndTxnRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(EndTxnResponse.min_version, EndTxnResponse.max_version + 1))
def test_end_txn_response_roundtrip(version):
    response = EndTxnResponse(
        throttle_time_ms=100 if version >= 1 else 0,
        error_code=9,
        producer_id=12345 if version >= 5 else -1,
        producer_epoch=1 if version >= 5 else -1,
    )
    encoded = response.encode(version=version)
    decoded = EndTxnResponse.decode(encoded, version=version)
    assert decoded == response


@pytest.mark.parametrize("version", range(TxnOffsetCommitRequest.min_version, TxnOffsetCommitRequest.max_version + 1))
def test_txn_offset_commit_request_roundtrip(version):
    Topic = TxnOffsetCommitRequest.TxnOffsetCommitRequestTopic
    Partition = Topic.TxnOffsetCommitRequestPartition
    request = TxnOffsetCommitRequest(
        transactional_id="test-txn",
        group_id="test-group",
        producer_id=12345,
        producer_epoch=1,
        generation_id=1 if version >= 3 else -1,
        member_id="test-member" if version >= 3 else "",
        group_instance_id="group-instance-id" if version >= 3 else None,
        topics=[
            Topic(
                name="topic-1",
                partitions=[
                    Partition(
                        partition_index=1,
                        committed_offset=100,
                        committed_leader_epoch=1 if version >= 2 else -1,
                        committed_metadata="meta"
                    ),
                ],
            ),
        ],
    )
    encoded = request.encode(version=version)
    decoded = TxnOffsetCommitRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(TxnOffsetCommitResponse.min_version, TxnOffsetCommitResponse.max_version + 1))
def test_txn_offset_commit_response_roundtrip(version):
    Topic = TxnOffsetCommitResponse.TxnOffsetCommitResponseTopic
    Partition = Topic.TxnOffsetCommitResponsePartition
    response = TxnOffsetCommitResponse(
        throttle_time_ms=100 if version >= 1 else 0,
        topics=[
            Topic(
                name="topic-1",
                partitions=[
                    Partition(
                        partition_index=1,
                        error_code=9,
                    ),
                ],
            ),
        ],
    )
    encoded = response.encode(version=version)
    decoded = TxnOffsetCommitResponse.decode(encoded, version=version)
    assert decoded == response
