import uuid

import pytest

from kafka.admin import KafkaAdminClient, OffsetSpec
from kafka.errors import (
    NotLeaderForPartitionError,
    UnknownTopicOrPartitionError,
    IncompatibleBrokerVersion,
)
from kafka.protocol.admin import (
    AlterPartitionReassignmentsRequest, AlterPartitionReassignmentsResponse,
    ListPartitionReassignmentsRequest, ListPartitionReassignmentsResponse,
    DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse,
)
from kafka.protocol.consumer import ListOffsetsRequest, ListOffsetsResponse
from kafka.protocol.metadata import MetadataResponse
from kafka.structs import TopicPartition, OffsetAndTimestamp

from test.mock_broker import MockBroker


def _make_admin(broker):
    return KafkaAdminClient(
        kafka_client=broker.client_factory(),
        bootstrap_servers='%s:%d' % (broker.host, broker.port),
        api_version=broker.broker_version,
        request_timeout_ms=5000,
    )


# ---------------------------------------------------------------------------
# alter_partition_reassignments
# ---------------------------------------------------------------------------


class TestAlterPartitionReassignmentsMockBroker:

    def test_success_returns_dict(self):
        broker = MockBroker()
        Topic = AlterPartitionReassignmentsResponse.ReassignableTopicResponse
        Partition = Topic.ReassignablePartitionResponse
        broker.respond(
            AlterPartitionReassignmentsRequest,
            AlterPartitionReassignmentsResponse(
                throttle_time_ms=0,
                error_code=0,
                error_message=None,
                responses=[
                    Topic(
                        name='topic-a',
                        partitions=[
                            Partition(partition_index=0, error_code=0, error_message=None),
                        ],
                    ),
                ],
            ),
        )

        admin = _make_admin(broker)
        try:
            result = admin.alter_partition_reassignments({
                TopicPartition('topic-a', 0): [1, 2, 3],
            })
        finally:
            admin.close()

        assert result['error_code'] == 0
        assert result['responses'][0]['name'] == 'topic-a'
        assert result['responses'][0]['partitions'][0]['error_code'] == 0

    def test_cancel_reassignment_sends_null_replicas(self):
        broker = MockBroker()
        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            decoded = AlterPartitionReassignmentsRequest.decode(
                request_bytes, version=api_version, header=True)
            captured['request'] = decoded
            return AlterPartitionReassignmentsResponse(
                throttle_time_ms=0, error_code=0, error_message=None, responses=[])

        broker.respond_fn(AlterPartitionReassignmentsRequest, handler)

        admin = _make_admin(broker)
        try:
            admin.alter_partition_reassignments({
                TopicPartition('topic-a', 0): None,  # cancel
                TopicPartition('topic-a', 1): [4, 5],
            })
        finally:
            admin.close()

        req = captured['request']
        assert len(req.topics) == 1
        assert req.topics[0].name == 'topic-a'
        by_index = {p.partition_index: p for p in req.topics[0].partitions}
        assert by_index[0].replicas is None
        assert list(by_index[1].replicas) == [4, 5]

    def test_partition_level_error_raises(self):
        broker = MockBroker()
        Topic = AlterPartitionReassignmentsResponse.ReassignableTopicResponse
        Partition = Topic.ReassignablePartitionResponse
        broker.respond(
            AlterPartitionReassignmentsRequest,
            AlterPartitionReassignmentsResponse(
                throttle_time_ms=0,
                error_code=0,
                error_message=None,
                responses=[
                    Topic(
                        name='topic-a',
                        partitions=[
                            Partition(partition_index=0, error_code=37,  # InvalidPartitionsError
                                      error_message='bad partition'),
                        ],
                    ),
                ],
            ),
        )

        admin = _make_admin(broker)
        try:
            with pytest.raises(Exception):
                admin.alter_partition_reassignments({
                    TopicPartition('topic-a', 0): [1, 2, 3],
                })
        finally:
            admin.close()

    def test_partition_error_suppressed_with_raise_errors_false(self):
        broker = MockBroker()
        Topic = AlterPartitionReassignmentsResponse.ReassignableTopicResponse
        Partition = Topic.ReassignablePartitionResponse
        broker.respond(
            AlterPartitionReassignmentsRequest,
            AlterPartitionReassignmentsResponse(
                throttle_time_ms=0,
                error_code=0,
                error_message=None,
                responses=[
                    Topic(
                        name='topic-a',
                        partitions=[
                            Partition(partition_index=0, error_code=37, error_message='bad'),
                        ],
                    ),
                ],
            ),
        )

        admin = _make_admin(broker)
        try:
            result = admin.alter_partition_reassignments(
                {TopicPartition('topic-a', 0): [1, 2, 3]},
                raise_errors=False,
            )
        finally:
            admin.close()

        assert result['responses'][0]['partitions'][0]['error_code'] == 37


# ---------------------------------------------------------------------------
# list_partition_reassignments
# ---------------------------------------------------------------------------


class TestListPartitionReassignmentsMockBroker:

    def test_returns_tp_to_reassignment_dict(self):
        broker = MockBroker()
        Topic = ListPartitionReassignmentsResponse.OngoingTopicReassignment
        Partition = Topic.OngoingPartitionReassignment
        broker.respond(
            ListPartitionReassignmentsRequest,
            ListPartitionReassignmentsResponse(
                throttle_time_ms=0,
                error_code=0,
                error_message=None,
                topics=[
                    Topic(
                        name='topic-a',
                        partitions=[
                            Partition(
                                partition_index=0,
                                replicas=[1, 2, 3],
                                adding_replicas=[4],
                                removing_replicas=[1],
                            ),
                            Partition(
                                partition_index=1,
                                replicas=[2, 3, 4],
                                adding_replicas=[],
                                removing_replicas=[],
                            ),
                        ],
                    ),
                ],
            ),
        )

        admin = _make_admin(broker)
        try:
            result = admin.list_partition_reassignments()
        finally:
            admin.close()

        assert result == {
            TopicPartition('topic-a', 0): {
                'replicas': [1, 2, 3],
                'adding_replicas': [4],
                'removing_replicas': [1],
            },
            TopicPartition('topic-a', 1): {
                'replicas': [2, 3, 4],
                'adding_replicas': [],
                'removing_replicas': [],
            },
        }

    def test_none_topic_partitions_sends_null_topics(self):
        broker = MockBroker()
        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            decoded = ListPartitionReassignmentsRequest.decode(
                request_bytes, version=api_version, header=True)
            captured['request'] = decoded
            return ListPartitionReassignmentsResponse(
                throttle_time_ms=0, error_code=0, error_message=None, topics=[])

        broker.respond_fn(ListPartitionReassignmentsRequest, handler)

        admin = _make_admin(broker)
        try:
            admin.list_partition_reassignments()
        finally:
            admin.close()

        assert captured['request'].topics is None

    def test_dict_input_encodes_topic_partitions(self):
        broker = MockBroker()
        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            decoded = ListPartitionReassignmentsRequest.decode(
                request_bytes, version=api_version, header=True)
            captured['request'] = decoded
            return ListPartitionReassignmentsResponse(
                throttle_time_ms=0, error_code=0, error_message=None, topics=[])

        broker.respond_fn(ListPartitionReassignmentsRequest, handler)

        admin = _make_admin(broker)
        try:
            admin.list_partition_reassignments({'topic-a': [0, 1], 'topic-b': [2]})
        finally:
            admin.close()

        topics = {t.name: list(t.partition_indexes) for t in captured['request'].topics}
        assert topics == {'topic-a': [0, 1], 'topic-b': [2]}

    def test_tp_list_input_groups_by_topic(self):
        broker = MockBroker()
        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            decoded = ListPartitionReassignmentsRequest.decode(
                request_bytes, version=api_version, header=True)
            captured['request'] = decoded
            return ListPartitionReassignmentsResponse(
                throttle_time_ms=0, error_code=0, error_message=None, topics=[])

        broker.respond_fn(ListPartitionReassignmentsRequest, handler)

        admin = _make_admin(broker)
        try:
            admin.list_partition_reassignments([
                TopicPartition('topic-a', 0),
                TopicPartition('topic-a', 1),
                TopicPartition('topic-b', 5),
            ])
        finally:
            admin.close()

        topics = {t.name: sorted(t.partition_indexes) for t in captured['request'].topics}
        assert topics == {'topic-a': [0, 1], 'topic-b': [5]}

    def test_top_level_error_raises(self):
        broker = MockBroker()
        broker.respond(
            ListPartitionReassignmentsRequest,
            ListPartitionReassignmentsResponse(
                throttle_time_ms=0,
                error_code=41,  # NotControllerError
                error_message='not controller',
                topics=[],
            ),
        )

        admin = _make_admin(broker)
        try:
            with pytest.raises(Exception) as exc_info:
                admin.list_partition_reassignments()
            assert 'not controller' in str(exc_info.value)
        finally:
            admin.close()


# ---------------------------------------------------------------------------
# describe_topic_partitions
# ---------------------------------------------------------------------------


class TestDescribeTopicPartitionsMockBroker:

    def test_returns_topic_partition_details(self):
        broker = MockBroker()
        topic_id = uuid.uuid4()
        Topic = DescribeTopicPartitionsResponse.DescribeTopicPartitionsResponseTopic
        Partition = Topic.DescribeTopicPartitionsResponsePartition
        broker.respond(
            DescribeTopicPartitionsRequest,
            DescribeTopicPartitionsResponse(
                throttle_time_ms=0,
                topics=[
                    Topic(
                        error_code=0,
                        name='topic-a',
                        topic_id=topic_id,
                        is_internal=False,
                        partitions=[
                            Partition(
                                error_code=0,
                                partition_index=0,
                                leader_id=1,
                                leader_epoch=5,
                                replica_nodes=[1, 2, 3],
                                isr_nodes=[1, 2],
                                eligible_leader_replicas=[3],
                                last_known_elr=[2],
                                offline_replicas=[],
                            ),
                        ],
                        topic_authorized_operations=-2147483648,
                    ),
                ],
                next_cursor=None,
            ),
        )

        admin = _make_admin(broker)
        try:
            result = admin.describe_topic_partitions(['topic-a'])
        finally:
            admin.close()

        assert result['next_cursor'] is None
        assert len(result['topics']) == 1
        t = result['topics'][0]
        assert t['name'] == 'topic-a'
        assert t['topic_id'] == topic_id
        assert t['is_internal'] is False
        assert t['partitions'][0]['partition_index'] == 0
        assert t['partitions'][0]['leader_id'] == 1
        assert t['partitions'][0]['eligible_leader_replicas'] == [3]
        assert t['partitions'][0]['last_known_elr'] == [2]

    def test_pagination_cursor_returned(self):
        broker = MockBroker()
        Cursor = DescribeTopicPartitionsResponse.Cursor
        broker.respond(
            DescribeTopicPartitionsRequest,
            DescribeTopicPartitionsResponse(
                throttle_time_ms=0,
                topics=[],
                next_cursor=Cursor(topic_name='topic-next', partition_index=5),
            ),
        )

        admin = _make_admin(broker)
        try:
            result = admin.describe_topic_partitions(['topic-a'])
        finally:
            admin.close()

        assert result['next_cursor'] == {'topic_name': 'topic-next', 'partition_index': 5}

    def test_request_encodes_topics_and_limit(self):
        broker = MockBroker()
        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            decoded = DescribeTopicPartitionsRequest.decode(
                request_bytes, version=api_version, header=True)
            captured['request'] = decoded
            return DescribeTopicPartitionsResponse(
                throttle_time_ms=0, topics=[], next_cursor=None)

        broker.respond_fn(DescribeTopicPartitionsRequest, handler)

        admin = _make_admin(broker)
        try:
            admin.describe_topic_partitions(
                ['topic-a', 'topic-b'], response_partition_limit=100)
        finally:
            admin.close()

        req = captured['request']
        assert [t.name for t in req.topics] == ['topic-a', 'topic-b']
        assert req.response_partition_limit == 100
        assert req.cursor is None

    def test_request_encodes_cursor(self):
        broker = MockBroker()
        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            decoded = DescribeTopicPartitionsRequest.decode(
                request_bytes, version=api_version, header=True)
            captured['request'] = decoded
            return DescribeTopicPartitionsResponse(
                throttle_time_ms=0, topics=[], next_cursor=None)

        broker.respond_fn(DescribeTopicPartitionsRequest, handler)

        admin = _make_admin(broker)
        try:
            admin.describe_topic_partitions(
                ['topic-a'],
                cursor={'topic_name': 'topic-a', 'partition_index': 3})
        finally:
            admin.close()

        cursor = captured['request'].cursor
        assert cursor is not None
        assert cursor.topic_name == 'topic-a'
        assert cursor.partition_index == 3


# ---------------------------------------------------------------------------
# list_partition_offsets
# ---------------------------------------------------------------------------


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
                for i in range(num_partitions)
            ])],
        brokers=brokers,
    )


def _list_offsets_response(per_partition):
    """per_partition: list of (topic, partition, offset, timestamp, leader_epoch, error_code)."""
    Topic = ListOffsetsResponse.ListOffsetsTopicResponse
    Partition = Topic.ListOffsetsPartitionResponse
    by_topic = {}
    for topic, partition, offset, ts, le, err in per_partition:
        by_topic.setdefault(topic, []).append(Partition(
            partition_index=partition, error_code=err, timestamp=ts,
            offset=offset, leader_epoch=le))
    return ListOffsetsResponse(
        throttle_time_ms=0,
        topics=[Topic(name=t, partitions=parts) for t, parts in by_topic.items()],
    )


class TestListPartitionOffsetsMockBroker:

    def test_returns_result_info(self):
        broker = MockBroker()
        _set_metadata_for_topic(broker, 'topic-a', num_partitions=2)
        broker.respond(
            ListOffsetsRequest,
            _list_offsets_response([
                ('topic-a', 0, 100, 1234, 5, 0),
                ('topic-a', 1, 200, 5678, 7, 0),
            ]),
        )

        admin = _make_admin(broker)
        try:
            result = admin.list_partition_offsets({
                TopicPartition('topic-a', 0): OffsetSpec.EARLIEST,
                TopicPartition('topic-a', 1): OffsetSpec.LATEST,
            })
        finally:
            admin.close()

        assert result == {
            TopicPartition('topic-a', 0): OffsetAndTimestamp(
                offset=100, timestamp=1234, leader_epoch=5),
            TopicPartition('topic-a', 1): OffsetAndTimestamp(
                offset=200, timestamp=5678, leader_epoch=7),
        }

    def test_request_uses_spec_timestamp_sentinels(self):
        broker = MockBroker()
        _set_metadata_for_topic(broker, 'topic-a', num_partitions=2)
        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            captured['version'] = api_version
            captured['request'] = ListOffsetsRequest.decode(
                request_bytes, version=api_version, header=True)
            return _list_offsets_response([
                ('topic-a', 0, 0, -1, -1, 0),
                ('topic-a', 1, 0, -1, -1, 0),
            ])

        broker.respond_fn(ListOffsetsRequest, handler)

        admin = _make_admin(broker)
        try:
            admin.list_partition_offsets({
                TopicPartition('topic-a', 0): OffsetSpec.EARLIEST,
                TopicPartition('topic-a', 1): OffsetSpec.LATEST,
            })
        finally:
            admin.close()

        req = captured['request']
        assert req.replica_id == -1
        assert req.isolation_level == 0
        topic = req.topics[0]
        timestamps = {p.partition_index: p.timestamp for p in topic.partitions}
        assert timestamps == {0: -2, 1: -1}

    def test_offset_timestamp_passed_through(self):
        broker = MockBroker()
        _set_metadata_for_topic(broker, 'topic-a', num_partitions=1)
        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            captured['request'] = ListOffsetsRequest.decode(
                request_bytes, version=api_version, header=True)
            return _list_offsets_response([('topic-a', 0, 42, 1700000000, 0, 0)])

        broker.respond_fn(ListOffsetsRequest, handler)

        admin = _make_admin(broker)
        try:
            result = admin.list_partition_offsets({
                TopicPartition('topic-a', 0): 1700000000,
            })
        finally:
            admin.close()

        assert captured['request'].topics[0].partitions[0].timestamp == 1700000000
        assert result[TopicPartition('topic-a', 0)].offset == 42

    def test_groups_partitions_by_leader(self):
        # Two partitions, two different leaders => two requests.
        broker = MockBroker()
        Broker = MetadataResponse.MetadataResponseBroker
        Topic = MetadataResponse.MetadataResponseTopic
        Partition = Topic.MetadataResponsePartition
        broker.set_metadata(
            topics=[Topic(
                version=8, error_code=0, name='topic-a', is_internal=False,
                partitions=[
                    Partition(version=8, error_code=0, partition_index=0,
                              leader_id=0, leader_epoch=0,
                              replica_nodes=[0], isr_nodes=[0], offline_replicas=[]),
                    Partition(version=8, error_code=0, partition_index=1,
                              leader_id=1, leader_epoch=0,
                              replica_nodes=[1], isr_nodes=[1], offline_replicas=[]),
                ])],
            brokers=[
                Broker(node_id=0, host=broker.host, port=broker.port, rack=None),
                Broker(node_id=1, host=broker.host, port=broker.port, rack=None),
            ],
        )

        captured = []

        def handler(api_key, api_version, correlation_id, request_bytes):
            req = ListOffsetsRequest.decode(
                request_bytes, version=api_version, header=True)
            captured.append(req)
            partitions = [(t.name, p.partition_index, 0, -1, -1, 0)
                          for t in req.topics for p in t.partitions]
            return _list_offsets_response(partitions)

        broker.respond_fn(ListOffsetsRequest, handler)
        broker.respond_fn(ListOffsetsRequest, handler)

        admin = _make_admin(broker)
        try:
            admin.list_partition_offsets({
                TopicPartition('topic-a', 0): OffsetSpec.LATEST,
                TopicPartition('topic-a', 1): OffsetSpec.LATEST,
            })
        finally:
            admin.close()

        # One request per leader, each carrying exactly one partition.
        assert len(captured) == 2
        for req in captured:
            assert sum(len(t.partitions) for t in req.topics) == 1

    def test_partition_error_raises(self):
        broker = MockBroker()
        _set_metadata_for_topic(broker, 'topic-a', num_partitions=1)
        broker.respond(
            ListOffsetsRequest,
            _list_offsets_response([
                ('topic-a', 0, -1, -1, -1, NotLeaderForPartitionError.errno),
            ]),
        )

        admin = _make_admin(broker)
        try:
            with pytest.raises(NotLeaderForPartitionError):
                admin.list_partition_offsets({
                    TopicPartition('topic-a', 0): OffsetSpec.LATEST,
                })
        finally:
            admin.close()

    def test_unknown_partition_raises(self):
        broker = MockBroker()
        _set_metadata_for_topic(broker, 'topic-a', num_partitions=1)
        admin = _make_admin(broker)
        try:
            with pytest.raises(UnknownTopicOrPartitionError):
                admin.list_partition_offsets({
                    TopicPartition('topic-a', 99): OffsetSpec.LATEST,
                })
        finally:
            admin.close()

    def test_max_timestamp_requires_v7(self):
        # (2, 7) broker -> ListOffsets max v6 -> MAX_TIMESTAMP unsupported
        broker = MockBroker(broker_version=(2, 7))
        _set_metadata_for_topic(broker, 'topic-a', num_partitions=1)
        admin = _make_admin(broker)
        try:
            with pytest.raises(IncompatibleBrokerVersion):
                admin.list_partition_offsets({
                    TopicPartition('topic-a', 0): OffsetSpec.MAX_TIMESTAMP,
                })
        finally:
            admin.close()

    def test_invalid_timestamp_raises(self):
        broker = MockBroker()
        _set_metadata_for_topic(broker, 'topic-a', num_partitions=1)
        admin = _make_admin(broker)
        try:
            with pytest.raises(ValueError):
                admin.list_partition_offsets({TopicPartition('topic-a', 0): -100}, 0)
        finally:
            admin.close()

    def test_invalid_isolation_level_raises(self):
        broker = MockBroker()
        _set_metadata_for_topic(broker, 'topic-a', num_partitions=1)
        admin = _make_admin(broker)
        try:
            with pytest.raises(ValueError):
                admin.list_partition_offsets(
                    {TopicPartition('topic-a', 0): OffsetSpec.LATEST},
                    isolation_level='wat',
                )
        finally:
            admin.close()

    def test_empty_input_is_noop(self):
        broker = MockBroker()
        admin = _make_admin(broker)
        try:
            assert admin.list_partition_offsets({}) == {}
        finally:
            admin.close()

    def test_int_timestamp_accepted_directly(self):
        broker = MockBroker()
        _set_metadata_for_topic(broker, 'topic-a', num_partitions=1)
        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            captured['request'] = ListOffsetsRequest.decode(
                request_bytes, version=api_version, header=True)
            return _list_offsets_response([('topic-a', 0, 0, -1, -1, 0)])

        broker.respond_fn(ListOffsetsRequest, handler)

        admin = _make_admin(broker)
        try:
            admin.list_partition_offsets({TopicPartition('topic-a', 0): -2})
        finally:
            admin.close()

        assert captured['request'].topics[0].partitions[0].timestamp == -2

    def test_read_committed_uses_isolation_level_1(self):
        broker = MockBroker()
        _set_metadata_for_topic(broker, 'topic-a', num_partitions=1)
        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            captured['request'] = ListOffsetsRequest.decode(
                request_bytes, version=api_version, header=True)
            return _list_offsets_response([('topic-a', 0, 0, -1, -1, 0)])

        broker.respond_fn(ListOffsetsRequest, handler)

        admin = _make_admin(broker)
        try:
            admin.list_partition_offsets(
                {TopicPartition('topic-a', 0): OffsetSpec.LATEST},
                isolation_level='read_committed',
            )
        finally:
            admin.close()

        assert captured['request'].isolation_level == 1
