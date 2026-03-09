
import pytest
from kafka.protocol.new.messages.transactions import (
    InitProducerIdRequest, InitProducerIdResponse,
    AddPartitionsToTxnRequest, AddPartitionsToTxnResponse,
    AddOffsetsToTxnRequest, AddOffsetsToTxnResponse,
    EndTxnRequest, EndTxnResponse,
    TxnOffsetCommitRequest, TxnOffsetCommitResponse,
)

@pytest.mark.parametrize("version", range(InitProducerIdRequest.min_version, InitProducerIdRequest.max_version + 1))
def test_init_producer_id_request_roundtrip(version):
    data = InitProducerIdRequest(
        transactional_id="test-txn",
        transaction_timeout_ms=10000,
        producer_id=-1,
        producer_epoch=-1
    )
    encoded = InitProducerIdRequest.encode(data, version=version)
    decoded = InitProducerIdRequest.decode(encoded, version=version)
    assert decoded == data

@pytest.mark.parametrize("version", range(InitProducerIdResponse.min_version, InitProducerIdResponse.max_version + 1))
def test_init_producer_id_response_roundtrip(version):
    data = InitProducerIdResponse(
        throttle_time_ms=100 if version >= 1 else 0,
        error_code=0,
        producer_id=12345,
        producer_epoch=1
    )
    encoded = InitProducerIdResponse.encode(data, version=version)
    decoded = InitProducerIdResponse.decode(encoded, version=version)
    assert decoded == data

@pytest.mark.parametrize("version", range(AddPartitionsToTxnRequest.min_version, AddPartitionsToTxnRequest.max_version + 1))
def test_add_partitions_to_txn_request_roundtrip(version):
    if version < 4:
        topics = [
            AddPartitionsToTxnRequest.AddPartitionsToTxnTopic(
                name="topic-1",
                partitions=[0, 1]
            )
        ]
        data = AddPartitionsToTxnRequest(
            v3_and_below_transactional_id="test-txn",
            v3_and_below_producer_id=12345,
            v3_and_below_producer_epoch=1,
            v3_and_below_topics=topics,
            transactions=[]
        )
    else:
        # Use the specific data class for the Topics array within Transaction
        Transaction = AddPartitionsToTxnRequest.AddPartitionsToTxnTransaction
        Topic = Transaction.AddPartitionsToTxnTopic
        transactions = [
            Transaction(
                transactional_id="test-txn",
                producer_id=12345,
                producer_epoch=1,
                verify_only=False,
                topics=[
                    Topic(
                        name="topic-1",
                        partitions=[0, 1]
                    )
                ]
            )
        ]
        data = AddPartitionsToTxnRequest(
            transactions=transactions,
            v3_and_below_transactional_id="",
            v3_and_below_producer_id=0,
            v3_and_below_producer_epoch=0,
            v3_and_below_topics=[]
        )
    encoded = AddPartitionsToTxnRequest.encode(data, version=version)
    decoded = AddPartitionsToTxnRequest.decode(encoded, version=version)
    assert decoded == data

@pytest.mark.parametrize("version", range(AddPartitionsToTxnResponse.min_version, AddPartitionsToTxnResponse.max_version + 1))
def test_add_partitions_to_txn_response_roundtrip(version):
    if version < 4:
        TopicResult = AddPartitionsToTxnResponse.AddPartitionsToTxnTopicResult
        PartitionResult = TopicResult.AddPartitionsToTxnPartitionResult
        topic_results = [
            TopicResult(
                name="topic-1",
                results_by_partition=[
                    PartitionResult(
                        partition_index=0,
                        partition_error_code=0
                    )
                ]
            )
        ]
        data = AddPartitionsToTxnResponse(
            throttle_time_ms=100 if version >= 1 else 0,
            results_by_topic_v3_and_below=topic_results,
            results_by_transaction=[]
        )
    else:
        Result = AddPartitionsToTxnResponse.AddPartitionsToTxnResult
        TopicResult = Result.AddPartitionsToTxnTopicResult
        PartitionResult = TopicResult.AddPartitionsToTxnPartitionResult
        topic_results = [
            TopicResult(
                name="topic-1",
                results_by_partition=[
                    PartitionResult(
                        partition_index=0,
                        partition_error_code=0
                    )
                ]
            )
        ]
        results_by_transaction = [
            Result(
                transactional_id="test-txn",
                topic_results=topic_results
            )
        ]
        data = AddPartitionsToTxnResponse(
            throttle_time_ms=100,
            error_code=0,
            results_by_transaction=results_by_transaction,
            results_by_topic_v3_and_below=[]
        )
    encoded = AddPartitionsToTxnResponse.encode(data, version=version)
    decoded = AddPartitionsToTxnResponse.decode(encoded, version=version)
    assert decoded == data

@pytest.mark.parametrize("version", range(AddOffsetsToTxnRequest.min_version, AddOffsetsToTxnRequest.max_version + 1))
def test_add_offsets_to_txn_request_roundtrip(version):
    data = AddOffsetsToTxnRequest(
        transactional_id="test-txn",
        producer_id=12345,
        producer_epoch=1,
        group_id="test-group"
    )
    encoded = AddOffsetsToTxnRequest.encode(data, version=version)
    decoded = AddOffsetsToTxnRequest.decode(encoded, version=version)
    assert decoded == data

@pytest.mark.parametrize("version", range(AddOffsetsToTxnResponse.min_version, AddOffsetsToTxnResponse.max_version + 1))
def test_add_offsets_to_txn_response_roundtrip(version):
    data = AddOffsetsToTxnResponse(
        throttle_time_ms=100 if version >= 1 else 0,
        error_code=0
    )
    encoded = AddOffsetsToTxnResponse.encode(data, version=version)
    decoded = AddOffsetsToTxnResponse.decode(encoded, version=version)
    assert decoded == data

@pytest.mark.parametrize("version", range(EndTxnRequest.min_version, EndTxnRequest.max_version + 1))
def test_end_txn_request_roundtrip(version):
    data = EndTxnRequest(
        transactional_id="test-txn",
        producer_id=12345,
        producer_epoch=1,
        committed=True
    )
    encoded = EndTxnRequest.encode(data, version=version)
    decoded = EndTxnRequest.decode(encoded, version=version)
    assert decoded == data

@pytest.mark.parametrize("version", range(EndTxnResponse.min_version, EndTxnResponse.max_version + 1))
def test_end_txn_response_roundtrip(version):
    data = EndTxnResponse(
        throttle_time_ms=100 if version >= 1 else 0,
        error_code=0
    )
    encoded = EndTxnResponse.encode(data, version=version)
    decoded = EndTxnResponse.decode(encoded, version=version)
    assert decoded == data

@pytest.mark.parametrize("version", range(TxnOffsetCommitRequest.min_version, TxnOffsetCommitRequest.max_version + 1))
def test_txn_offset_commit_request_roundtrip(version):
    Topic = TxnOffsetCommitRequest.TxnOffsetCommitRequestTopic
    Partition = Topic.TxnOffsetCommitRequestPartition
    topics = [
        Topic(
            name="topic-1",
            partitions=[
                Partition(
                    partition_index=0,
                    committed_offset=100,
                    committed_leader_epoch=1 if version >= 2 else -1,
                    committed_metadata="meta"
                )
            ]
        )
    ]
    data = TxnOffsetCommitRequest(
        transactional_id="test-txn",
        group_id="test-group",
        producer_id=12345,
        producer_epoch=1,
        generation_id=1 if version >= 3 else -1,
        member_id="test-member" if version >= 3 else "",
        group_instance_id=None,
        topics=topics
    )
    encoded = TxnOffsetCommitRequest.encode(data, version=version)
    decoded = TxnOffsetCommitRequest.decode(encoded, version=version)
    assert decoded == data

@pytest.mark.parametrize("version", range(TxnOffsetCommitResponse.min_version, TxnOffsetCommitResponse.max_version + 1))
def test_txn_offset_commit_response_roundtrip(version):
    Topic = TxnOffsetCommitResponse.TxnOffsetCommitResponseTopic
    Partition = Topic.TxnOffsetCommitResponsePartition
    topics = [
        Topic(
            name="topic-1",
            partitions=[
                Partition(
                    partition_index=0,
                    error_code=0
                )
            ]
        )
    ]
    data = TxnOffsetCommitResponse(
        throttle_time_ms=100 if version >= 1 else 0,
        topics=topics
    )
    encoded = TxnOffsetCommitResponse.encode(data, version=version)
    decoded = TxnOffsetCommitResponse.decode(encoded, version=version)
    assert decoded == data
