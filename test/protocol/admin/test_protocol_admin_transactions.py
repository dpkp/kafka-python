"""Round-trip encode/decode tests for the KIP-664 protocol messages."""
import pytest

from kafka.protocol.admin.transactions import (
    DescribeProducersRequest,
    DescribeProducersResponse,
    DescribeTransactionsRequest,
    DescribeTransactionsResponse,
    ListTransactionsRequest,
    ListTransactionsResponse,
)
from kafka.protocol.producer.transaction import (
    WriteTxnMarkersRequest,
    WriteTxnMarkersResponse,
)


@pytest.mark.parametrize(
    'version', range(ListTransactionsRequest.min_version,
                     ListTransactionsRequest.max_version + 1))
def test_list_transactions_request_roundtrip(version):
    request = ListTransactionsRequest(
        state_filters=['Ongoing'],
        producer_id_filters=[1000, 2000],
        duration_filter=60000 if version >= 1 else -1,
        transactional_id_pattern='.*' if version >= 2 else None,
    )
    encoded = request.encode(version=version)
    decoded = ListTransactionsRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize(
    'version', range(ListTransactionsResponse.min_version,
                     ListTransactionsResponse.max_version + 1))
def test_list_transactions_response_roundtrip(version):
    Row = ListTransactionsResponse.TransactionState
    response = ListTransactionsResponse(
        throttle_time_ms=100,
        error_code=0,
        unknown_state_filters=[],
        transaction_states=[
            Row(transactional_id='txn-1', producer_id=1000,
                transaction_state='Ongoing')],
    )
    encoded = response.encode(version=version)
    decoded = ListTransactionsResponse.decode(encoded, version=version)
    assert decoded == response


def test_describe_transactions_request_roundtrip():
    request = DescribeTransactionsRequest(transactional_ids=['txn-1', 'txn-2'])
    encoded = request.encode(version=0)
    decoded = DescribeTransactionsRequest.decode(encoded, version=0)
    assert decoded == request


def test_describe_transactions_response_roundtrip():
    Row = DescribeTransactionsResponse.TransactionState
    Topic = Row.TopicData
    response = DescribeTransactionsResponse(
        throttle_time_ms=0,
        transaction_states=[
            Row(error_code=0, transactional_id='txn-1',
                transaction_state='Ongoing',
                transaction_timeout_ms=60000,
                transaction_start_time_ms=1_700_000_000_000,
                producer_id=1000, producer_epoch=5,
                topics=[Topic(topic='topic-a', partitions=[0, 1])])],
    )
    encoded = response.encode(version=0)
    decoded = DescribeTransactionsResponse.decode(encoded, version=0)
    assert decoded == response


def test_describe_producers_request_roundtrip():
    Topic = DescribeProducersRequest.TopicRequest
    request = DescribeProducersRequest(
        topics=[Topic(name='topic-a', partition_indexes=[0, 1])])
    encoded = request.encode(version=0)
    decoded = DescribeProducersRequest.decode(encoded, version=0)
    assert decoded == request


def test_describe_producers_response_roundtrip():
    Topic = DescribeProducersResponse.TopicResponse
    Partition = Topic.PartitionResponse
    Producer = Partition.ProducerState
    response = DescribeProducersResponse(
        throttle_time_ms=0,
        topics=[Topic(name='topic-a', partitions=[
            Partition(partition_index=0, error_code=0, error_message=None,
                      active_producers=[Producer(
                          producer_id=1000, producer_epoch=5,
                          last_sequence=42, last_timestamp=1_700_000_000_000,
                          coordinator_epoch=3,
                          current_txn_start_offset=100)])])])
    encoded = response.encode(version=0)
    decoded = DescribeProducersResponse.decode(encoded, version=0)
    assert decoded == response


@pytest.mark.parametrize(
    'version', range(WriteTxnMarkersRequest._valid_versions[0],
                     WriteTxnMarkersRequest._valid_versions[1] + 1))
def test_write_txn_markers_request_roundtrip(version):
    Marker = WriteTxnMarkersRequest.WritableTxnMarker
    Topic = Marker.WritableTxnMarkerTopic
    request = WriteTxnMarkersRequest(markers=[Marker(
        producer_id=1000, producer_epoch=5, transaction_result=False,
        topics=[Topic(name='topic-a', partition_indexes=[0])],
        coordinator_epoch=-1,
        transaction_version=0 if version >= 2 else 0,
    )])
    encoded = request.encode(version=version)
    decoded = WriteTxnMarkersRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize(
    'version', range(WriteTxnMarkersResponse._valid_versions[0],
                     WriteTxnMarkersResponse._valid_versions[1] + 1))
def test_write_txn_markers_response_roundtrip(version):
    Result = WriteTxnMarkersResponse.WritableTxnMarkerResult
    Topic = Result.WritableTxnMarkerTopicResult
    Partition = Topic.WritableTxnMarkerPartitionResult
    response = WriteTxnMarkersResponse(markers=[Result(
        producer_id=1000,
        topics=[Topic(name='topic-a', partitions=[
            Partition(partition_index=0, error_code=0)])])])
    encoded = response.encode(version=version)
    decoded = WriteTxnMarkersResponse.decode(encoded, version=version)
    assert decoded == response
