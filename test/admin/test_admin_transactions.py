import pytest

from kafka.admin import (
    AbortTransactionSpec,
    PartitionProducerState,
    ProducerState,
    TransactionDescription,
    TransactionListing,
    TransactionState,
)
import kafka.errors as Errors
from kafka.protocol.admin.transactions import (
    DescribeProducersRequest,
    DescribeProducersResponse,
    DescribeTransactionsRequest,
    DescribeTransactionsResponse,
    ListTransactionsRequest,
    ListTransactionsResponse,
)
from kafka.protocol.metadata import (
    CoordinatorType,
    FindCoordinatorRequest,
    FindCoordinatorResponse,
    MetadataResponse,
)
from kafka.protocol.producer.transaction import (
    WriteTxnMarkersRequest,
    WriteTxnMarkersResponse,
)
from kafka.structs import TopicPartition


def _find_coordinator_response(key, node_id=0):
    Coordinator = FindCoordinatorResponse.Coordinator
    return FindCoordinatorResponse(
        throttle_time_ms=0,
        error_code=0, error_message=None,
        node_id=node_id, host='localhost', port=9092,
        coordinators=[Coordinator(
            key=key, node_id=node_id, host='localhost', port=9092,
            error_code=0, error_message=None)])


def _list_transactions_response(rows, error_code=0, unknown_state_filters=None):
    Row = ListTransactionsResponse.TransactionState
    return ListTransactionsResponse(
        throttle_time_ms=0,
        error_code=error_code,
        unknown_state_filters=unknown_state_filters or [],
        transaction_states=[Row(
            transactional_id=tid, producer_id=pid, transaction_state=state)
            for tid, pid, state in rows])


def _describe_transactions_response(rows):
    Row = DescribeTransactionsResponse.TransactionState
    Topic = Row.TopicData
    txn_rows = []
    for row in rows:
        topics = [
            Topic(topic=name, partitions=list(parts))
            for name, parts in row.get('topics', {}).items()
        ]
        txn_rows.append(Row(
            error_code=row.get('error_code', 0),
            transactional_id=row['transactional_id'],
            transaction_state=row.get('state', 'Ongoing'),
            transaction_timeout_ms=row.get('timeout_ms', 60000),
            transaction_start_time_ms=row.get('start_time_ms', 1_700_000_000_000),
            producer_id=row.get('producer_id', 1000),
            producer_epoch=row.get('producer_epoch', 1),
            topics=topics,
        ))
    return DescribeTransactionsResponse(
        throttle_time_ms=0,
        transaction_states=txn_rows)


def _describe_producers_response(per_partition):
    """per_partition: list of dicts: {topic, partition, error_code?, error_message?, producers: [{...}]}"""
    Topic = DescribeProducersResponse.TopicResponse
    Partition = Topic.PartitionResponse
    Producer = Partition.ProducerState
    by_topic = {}
    for p in per_partition:
        by_topic.setdefault(p['topic'], []).append(Partition(
            partition_index=p['partition'],
            error_code=p.get('error_code', 0),
            error_message=p.get('error_message'),
            active_producers=[Producer(
                producer_id=ap['producer_id'],
                producer_epoch=ap.get('producer_epoch', 1),
                last_sequence=ap.get('last_sequence', -1),
                last_timestamp=ap.get('last_timestamp', -1),
                coordinator_epoch=ap.get('coordinator_epoch', 0),
                current_txn_start_offset=ap.get('current_txn_start_offset', -1),
            ) for ap in p.get('producers', [])]))
    return DescribeProducersResponse(
        throttle_time_ms=0,
        topics=[Topic(name=name, partitions=parts) for name, parts in by_topic.items()])


def _write_txn_markers_response(per_partition):
    """per_partition: list of (producer_id, topic, partition, error_code)."""
    Result = WriteTxnMarkersResponse.WritableTxnMarkerResult
    Topic = Result.WritableTxnMarkerTopicResult
    Partition = Topic.WritableTxnMarkerPartitionResult
    by_producer = {}
    for pid, topic, partition, error_code in per_partition:
        by_producer.setdefault(pid, {}).setdefault(topic, []).append(
            Partition(partition_index=partition, error_code=error_code))
    return WriteTxnMarkersResponse(markers=[
        Result(producer_id=pid, topics=[
            Topic(name=name, partitions=parts) for name, parts in topics.items()
        ]) for pid, topics in by_producer.items()
    ])


def _set_metadata_for_topic(broker, name, num_partitions, leader_id=0, brokers=None):
    Topic = MetadataResponse.MetadataResponseTopic
    Partition = Topic.MetadataResponsePartition
    if brokers is None:
        Broker = MetadataResponse.MetadataResponseBroker
        brokers = [Broker(node_id=0, host=broker.host, port=broker.port, rack=None)]
    broker.set_metadata(
        topics=[Topic(
            version=8, error_code=0, name=name, is_internal=False,
            partitions=[
                Partition(version=8, error_code=0, partition_index=i,
                          leader_id=leader_id if not isinstance(leader_id, dict) else leader_id[i],
                          leader_epoch=0,
                          replica_nodes=[0], isr_nodes=[0], offline_replicas=[])
                for i in range(num_partitions)])],
        brokers=brokers,
    )


# ---------------------------------------------------------------------------
# list_transactions
# ---------------------------------------------------------------------------


class TestListTransactionsMockBroker:
    def test_default_fans_out_to_all_brokers(self, broker, admin):
        broker.respond(ListTransactionsRequest,
                       _list_transactions_response([('txn-1', 100, 'Ongoing')]))

        result = admin.list_transactions()

        assert result == {broker.node_id: [
            TransactionListing(transactional_id='txn-1', producer_id=100,
                               state=TransactionState.ONGOING)]}

    def test_state_filter_passes_through_to_request(self, broker, admin):
        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            req = ListTransactionsRequest.decode(
                request_bytes, version=api_version, header=True)
            captured['state_filters'] = list(req.state_filters)
            captured['producer_id_filters'] = list(req.producer_id_filters)
            return _list_transactions_response([])

        broker.respond_fn(ListTransactionsRequest, handler)

        admin.list_transactions(state_filters=['Ongoing'], producer_id_filters=[1, 2])

        assert captured['state_filters'] == ['Ongoing']
        assert captured['producer_id_filters'] == [1, 2]

    def test_duration_filter_requires_v1(self, broker, admin):
        # 4.2 supports ListTransactions v2 (default fixture). To force the
        # version pre-check failure, drop the broker version.
        # The fixture defaults to (4, 2), which supports v0-v2.
        # We need a 3.0 broker (which only supports v0).
        # Use a separate test parametrize. Here just verify success path on 4.2.
        broker.respond(ListTransactionsRequest, _list_transactions_response([]))
        admin.list_transactions(duration_filter_ms=60000)  # should not raise

    @pytest.mark.parametrize('broker', [(3, 0)], indirect=True)
    def test_duration_filter_unsupported_on_old_broker(self, broker, admin):
        with pytest.raises(Errors.UnsupportedVersionError):
            admin.list_transactions(duration_filter_ms=60000)

    @pytest.mark.parametrize('broker', [(4, 0)], indirect=True)
    def test_id_pattern_unsupported_on_pre_4_1_broker(self, broker, admin):
        with pytest.raises(Errors.UnsupportedVersionError):
            admin.list_transactions(transactional_id_pattern='.*')

    def test_response_error_raises(self, broker, admin):
        broker.respond(
            ListTransactionsRequest,
            _list_transactions_response(
                [], error_code=Errors.ClusterAuthorizationFailedError.errno))
        with pytest.raises(Errors.ClusterAuthorizationFailedError):
            admin.list_transactions()


# ---------------------------------------------------------------------------
# describe_transactions
# ---------------------------------------------------------------------------


class TestDescribeTransactionsMockBroker:
    def test_routes_to_transaction_coordinator(self, broker, admin):
        captured = {'find_key_types': []}

        def find_handler(api_key, api_version, correlation_id, request_bytes):
            req = FindCoordinatorRequest.decode(
                request_bytes, version=api_version, header=True)
            captured['find_key_types'].append(req.key_type)
            keys = list(req.coordinator_keys) or [req.key]
            return _find_coordinator_response(keys[0])

        broker.respond_fn(FindCoordinatorRequest, find_handler)
        broker.respond(
            DescribeTransactionsRequest,
            _describe_transactions_response([{
                'transactional_id': 'txn-1',
                'state': 'Ongoing',
                'producer_id': 1000,
                'producer_epoch': 5,
                'timeout_ms': 60000,
                'start_time_ms': 1_700_000_000_000,
                'topics': {'topic-a': [0, 1]},
            }]))

        result = admin.describe_transactions(['txn-1'])

        assert captured['find_key_types'] == [CoordinatorType.TRANSACTION.value]
        desc = result['txn-1']
        assert isinstance(desc, TransactionDescription)
        assert desc.coordinator_id == 0
        assert desc.state == TransactionState.ONGOING
        assert desc.producer_id == 1000
        assert desc.producer_epoch == 5
        assert desc.transaction_timeout_ms == 60000
        assert desc.transaction_start_time_ms == 1_700_000_000_000
        assert desc.topic_partitions == {
            TopicPartition('topic-a', 0), TopicPartition('topic-a', 1)}

    def test_per_id_error_raises(self, broker, admin):
        broker.respond(FindCoordinatorRequest, _find_coordinator_response('txn-1'))
        broker.respond(
            DescribeTransactionsRequest,
            _describe_transactions_response([{
                'transactional_id': 'txn-1',
                'error_code': Errors.TransactionalIdNotFoundError.errno,
            }]))

        with pytest.raises(Errors.TransactionalIdNotFoundError):
            admin.describe_transactions(['txn-1'])

    def test_empty_input_short_circuits(self, broker, admin):
        # No request should be issued at all.
        assert admin.describe_transactions([]) == {}


# ---------------------------------------------------------------------------
# describe_producers
# ---------------------------------------------------------------------------


class TestDescribeProducersMockBroker:
    def test_routes_to_partition_leader(self, broker, admin):
        _set_metadata_for_topic(broker, 'topic-a', num_partitions=1, leader_id=0)
        broker.respond(DescribeProducersRequest, _describe_producers_response([
            {'topic': 'topic-a', 'partition': 0, 'producers': [
                {'producer_id': 1000, 'producer_epoch': 7,
                 'last_sequence': 42, 'last_timestamp': 1_700_000_000_000,
                 'coordinator_epoch': 3, 'current_txn_start_offset': 100},
            ]},
        ]))

        result = admin.describe_producers([TopicPartition('topic-a', 0)])

        assert result == {
            TopicPartition('topic-a', 0): PartitionProducerState(
                active_producers=[ProducerState(
                    producer_id=1000, producer_epoch=7,
                    last_sequence=42, last_timestamp=1_700_000_000_000,
                    coordinator_epoch=3,
                    current_transaction_start_offset=100)]),
        }

    def test_broker_id_override_sends_to_specified_broker(self, broker, admin):
        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            req = DescribeProducersRequest.decode(
                request_bytes, version=api_version, header=True)
            captured['topics'] = [(t.name, list(t.partition_indexes)) for t in req.topics]
            return _describe_producers_response([
                {'topic': 'topic-a', 'partition': 0, 'producers': []},
            ])

        broker.respond_fn(DescribeProducersRequest, handler)
        # No metadata set up -> if we hit leader_for_partition we'd fail.
        admin.describe_producers([TopicPartition('topic-a', 0)], broker_id=0)

        assert captured['topics'] == [('topic-a', [0])]

    def test_per_partition_error_raises(self, broker, admin):
        _set_metadata_for_topic(broker, 'topic-a', num_partitions=1, leader_id=0)
        broker.respond(DescribeProducersRequest, _describe_producers_response([
            {'topic': 'topic-a', 'partition': 0,
             'error_code': Errors.NotLeaderForPartitionError.errno,
             'error_message': 'not the leader'},
        ]))

        with pytest.raises(Errors.NotLeaderForPartitionError):
            admin.describe_producers([TopicPartition('topic-a', 0)])

    def test_empty_input_short_circuits(self, broker, admin):
        assert admin.describe_producers([]) == {}


# ---------------------------------------------------------------------------
# abort_transaction (WriteTxnMarkers)
# ---------------------------------------------------------------------------


class TestAbortTransactionMockBroker:
    def test_sends_abort_marker_to_partition_leader(self, broker, admin):
        _set_metadata_for_topic(broker, 'topic-a', num_partitions=1, leader_id=0)
        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            req = WriteTxnMarkersRequest.decode(
                request_bytes, version=api_version, header=True)
            marker = req.markers[0]
            captured['producer_id'] = marker.producer_id
            captured['producer_epoch'] = marker.producer_epoch
            captured['transaction_result'] = marker.transaction_result
            captured['coordinator_epoch'] = marker.coordinator_epoch
            captured['topics'] = [(t.name, list(t.partition_indexes)) for t in marker.topics]
            return _write_txn_markers_response([
                (marker.producer_id, 'topic-a', 0, 0),
            ])

        broker.respond_fn(WriteTxnMarkersRequest, handler)

        admin.abort_transaction(AbortTransactionSpec(
            topic_partition=TopicPartition('topic-a', 0),
            producer_id=1000,
            producer_epoch=5,
        ))

        assert captured['producer_id'] == 1000
        assert captured['producer_epoch'] == 5
        assert captured['transaction_result'] is False
        assert captured['coordinator_epoch'] == -1
        assert captured['topics'] == [('topic-a', [0])]

    def test_per_partition_error_raises(self, broker, admin):
        _set_metadata_for_topic(broker, 'topic-a', num_partitions=1, leader_id=0)
        broker.respond(WriteTxnMarkersRequest, _write_txn_markers_response([
            (1000, 'topic-a', 0, Errors.NotLeaderForPartitionError.errno),
        ]))

        with pytest.raises(Errors.NotLeaderForPartitionError):
            admin.abort_transaction(AbortTransactionSpec(
                topic_partition=TopicPartition('topic-a', 0),
                producer_id=1000, producer_epoch=5))


# ---------------------------------------------------------------------------
# find_hanging_transactions
# ---------------------------------------------------------------------------


class TestFindHangingTransactionsMockBroker:
    def test_returns_hanging_only(self, broker, admin):
        import time
        now_ms = int(time.time() * 1000)

        # One hanging (started 1 hour ago), one fresh.
        listings = [
            ('hanging-txn', 1000, 'Ongoing'),
            ('fresh-txn', 1001, 'Ongoing'),
        ]
        broker.respond(ListTransactionsRequest, _list_transactions_response(listings))
        broker.respond(FindCoordinatorRequest, _find_coordinator_response('hanging-txn'))
        broker.respond(DescribeTransactionsRequest, _describe_transactions_response([
            {'transactional_id': 'hanging-txn', 'state': 'Ongoing',
             'producer_id': 1000, 'producer_epoch': 5,
             'start_time_ms': now_ms - 60 * 60 * 1000,  # 1 hour ago
             'topics': {'topic-a': [0]}},
            {'transactional_id': 'fresh-txn', 'state': 'Ongoing',
             'producer_id': 1001, 'producer_epoch': 1,
             'start_time_ms': now_ms - 1000,  # 1 second ago
             'topics': {'topic-a': [1]}},
        ]))

        # threshold = 5min default in test (small), so 1h is hanging, 1s is fresh.
        result = admin.find_hanging_transactions(max_transaction_timeout_ms=60_000)

        assert len(result) == 1
        assert result[0]['transactional_id'] == 'hanging-txn'
        assert result[0]['producer_id'] == 1000
        assert result[0]['producer_epoch'] == 5
        assert result[0]['state'] == 'Ongoing'
        assert result[0]['topic_partitions'] == [TopicPartition('topic-a', 0)]
        assert result[0]['age_ms'] >= 60 * 60 * 1000

    def test_no_transactions_returns_empty(self, broker, admin):
        broker.respond(ListTransactionsRequest, _list_transactions_response([]))
        assert admin.find_hanging_transactions() == []
