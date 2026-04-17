import uuid

import pytest

from kafka.protocol.admin import (
    AlterPartitionReassignmentsRequest, AlterPartitionReassignmentsResponse,
    ListPartitionReassignmentsRequest, ListPartitionReassignmentsResponse,
    DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse,
)


def _versions(cls):
    lo, hi = cls._valid_versions
    return range(lo, hi + 1)


@pytest.mark.parametrize("version", _versions(AlterPartitionReassignmentsRequest))
def test_alter_partition_reassignments_request_roundtrip(version):
    Topic = AlterPartitionReassignmentsRequest.ReassignableTopic
    Partition = Topic.ReassignablePartition
    request = AlterPartitionReassignmentsRequest(
        timeout_ms=30000,
        topics=[
            Topic(
                name='topic-a',
                partitions=[
                    Partition(partition_index=0, replicas=[1, 2, 3]),
                    Partition(partition_index=1, replicas=None),
                ],
            ),
        ],
    )
    encoded = request.encode(version=version)
    decoded = AlterPartitionReassignmentsRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", _versions(AlterPartitionReassignmentsResponse))
def test_alter_partition_reassignments_response_roundtrip(version):
    Topic = AlterPartitionReassignmentsResponse.ReassignableTopicResponse
    Partition = Topic.ReassignablePartitionResponse
    response = AlterPartitionReassignmentsResponse(
        throttle_time_ms=5,
        error_code=0,
        error_message=None,
        responses=[
            Topic(
                name='topic-a',
                partitions=[
                    Partition(partition_index=0, error_code=0, error_message=None),
                    Partition(partition_index=1, error_code=37, error_message='invalid'),
                ],
            ),
        ],
    )
    encoded = response.encode(version=version)
    decoded = AlterPartitionReassignmentsResponse.decode(encoded, version=version)
    assert decoded == response


@pytest.mark.parametrize("version", _versions(ListPartitionReassignmentsRequest))
def test_list_partition_reassignments_request_roundtrip(version):
    Topic = ListPartitionReassignmentsRequest.ListPartitionReassignmentsTopics
    request = ListPartitionReassignmentsRequest(
        timeout_ms=30000,
        topics=[
            Topic(name='topic-a', partition_indexes=[0, 1]),
            Topic(name='topic-b', partition_indexes=[2]),
        ],
    )
    encoded = request.encode(version=version)
    decoded = ListPartitionReassignmentsRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", _versions(ListPartitionReassignmentsRequest))
def test_list_partition_reassignments_request_null_topics(version):
    request = ListPartitionReassignmentsRequest(timeout_ms=30000, topics=None)
    encoded = request.encode(version=version)
    decoded = ListPartitionReassignmentsRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", _versions(ListPartitionReassignmentsResponse))
def test_list_partition_reassignments_response_roundtrip(version):
    Topic = ListPartitionReassignmentsResponse.OngoingTopicReassignment
    Partition = Topic.OngoingPartitionReassignment
    response = ListPartitionReassignmentsResponse(
        throttle_time_ms=5,
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
                        removing_replicas=[2],
                    ),
                ],
            ),
        ],
    )
    encoded = response.encode(version=version)
    decoded = ListPartitionReassignmentsResponse.decode(encoded, version=version)
    assert decoded == response


def test_describe_topic_partitions_raw_bytes():
    # bytes encoding from java client
    data = bytes.fromhex('00 4b 00 00 00 00 00 7b 00 09 6d 79 2d 63 6c 69 65 6e 74 00 02 04 66 6f 6f 00 00 00 07 d0 ff 00')
    request = DescribeTopicPartitionsRequest.decode(data, version=0, header=True, framed=False)
    _TopicRequest = DescribeTopicPartitionsRequest.TopicRequest
    assert request == DescribeTopicPartitionsRequest(version=0, topics=[_TopicRequest(version=0, name='foo')], response_partition_limit=2000, cursor=None)


@pytest.mark.parametrize("version", _versions(DescribeTopicPartitionsRequest))
def test_describe_topic_partitions_request_roundtrip(version):
    Topic = DescribeTopicPartitionsRequest.TopicRequest
    Cursor = DescribeTopicPartitionsRequest.Cursor
    request = DescribeTopicPartitionsRequest(
        topics=[Topic(name='topic-a'), Topic(name='topic-b')],
        response_partition_limit=1000,
        cursor=Cursor(topic_name='topic-a', partition_index=5),
    )
    encoded = request.encode(version=version)
    decoded = DescribeTopicPartitionsRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", _versions(DescribeTopicPartitionsRequest))
def test_describe_topic_partitions_request_null_cursor(version):
    Topic = DescribeTopicPartitionsRequest.TopicRequest
    request = DescribeTopicPartitionsRequest(
        topics=[Topic(name='topic-a')],
        response_partition_limit=2000,
        cursor=None,
    )
    encoded = request.encode(version=version)
    decoded = DescribeTopicPartitionsRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", _versions(DescribeTopicPartitionsResponse))
def test_describe_topic_partitions_response_roundtrip(version):
    Topic = DescribeTopicPartitionsResponse.DescribeTopicPartitionsResponseTopic
    Partition = Topic.DescribeTopicPartitionsResponsePartition
    Cursor = DescribeTopicPartitionsResponse.Cursor
    response = DescribeTopicPartitionsResponse(
        throttle_time_ms=0,
        topics=[
            Topic(
                error_code=0,
                name='topic-a',
                topic_id=uuid.uuid4(),
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
        next_cursor=Cursor(topic_name='topic-a', partition_index=1),
    )
    encoded = response.encode(version=version)
    decoded = DescribeTopicPartitionsResponse.decode(encoded, version=version)
    assert decoded == response


@pytest.mark.parametrize("version", _versions(DescribeTopicPartitionsResponse))
def test_describe_topic_partitions_response_null_cursor(version):
    response = DescribeTopicPartitionsResponse(
        throttle_time_ms=0,
        topics=[],
        next_cursor=None,
    )
    encoded = response.encode(version=version)
    decoded = DescribeTopicPartitionsResponse.decode(encoded, version=version)
    assert decoded == response
