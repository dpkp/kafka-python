import uuid

import pytest

from kafka.protocol.admin import (
    DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse,
)


def _versions(cls):
    lo, hi = cls._valid_versions
    return range(lo, hi + 1)


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
