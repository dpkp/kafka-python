import uuid

import pytest

from kafka.protocol.new.admin import (
    CreateTopicsRequest, CreateTopicsResponse,
    DeleteTopicsRequest, DeleteTopicsResponse,
    CreatePartitionsRequest, CreatePartitionsResponse,
    DescribeGroupsRequest, DescribeGroupsResponse,
    ListGroupsRequest, ListGroupsResponse,
    DeleteGroupsRequest, DeleteGroupsResponse,
    DescribeClusterRequest, DescribeClusterResponse,
    DescribeConfigsRequest, DescribeConfigsResponse,
    AlterConfigsRequest, AlterConfigsResponse,
    CreateAclsRequest, CreateAclsResponse,
    DeleteAclsRequest, DeleteAclsResponse,
    DescribeAclsRequest, DescribeAclsResponse,
)


@pytest.mark.parametrize("version", range(CreateTopicsRequest.min_version, CreateTopicsRequest.max_version + 1))
def test_create_topics_request_roundtrip(version):
    Topic = CreateTopicsRequest.CreatableTopic
    topics = [
        Topic(
            name="test-topic",
            num_partitions=1,
            replication_factor=1,
            assignments=[],
            configs=[]
        )
    ]
    request = CreateTopicsRequest(
        topics=topics,
        timeout_ms=10000,
        validate_only=False
    )
    encoded = request.encode(version=version)
    decoded = CreateTopicsRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(CreateTopicsResponse.min_version, CreateTopicsResponse.max_version + 1))
def test_create_topics_response_roundtrip(version):
    Topic = CreateTopicsResponse.CreatableTopicResult
    topics = [
        Topic(
            name="test-topic",
            error_code=13,
            error_message='foo' if version >= 1 else '',
            topic_config_error_code=2 if version >= 5 else 0,
            num_partitions=1 if version >= 5 else -1,
            replication_factor=1 if version >= 5 else -1,
            configs=[]
        )
    ]
    response = CreateTopicsResponse(
        throttle_time_ms=123 if version >= 2 else 0,
        topics=topics,
    )
    encoded = response.encode(version=version)
    decoded = CreateTopicsResponse.decode(encoded, version=version)
    assert decoded == response


@pytest.mark.parametrize("version", range(DeleteTopicsRequest.min_version, DeleteTopicsRequest.max_version + 1))
def test_delete_topics_request_roundtrip(version):
    topic_names = ["topic-1", "topic-2"]
    
    Topic = DeleteTopicsRequest.DeleteTopicState
    topics = []
    if version >= 6:
        for t_name in topic_names:
            topics.append(Topic(name=t_name, topic_id=uuid.uuid4()))
    
    request = DeleteTopicsRequest(
        topic_names=topic_names if version < 6 else [],
        timeout_ms=10000,
        topics=topics
    )
    encoded = request.encode(version=version)
    decoded = DeleteTopicsRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(DescribeGroupsRequest.min_version, DescribeGroupsRequest.max_version + 1))
def test_describe_groups_request_roundtrip(version):
    request = DescribeGroupsRequest(
        groups=["group-1"],
        include_authorized_operations=True if version >= 3 else False
    )
    encoded = request.encode(version=version)
    decoded = DescribeGroupsRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(ListGroupsRequest.min_version, ListGroupsRequest.max_version + 1))
def test_list_groups_request_roundtrip(version):
    request = ListGroupsRequest(
        states_filter=["Stable"] if version >= 4 else []
    )
    encoded = request.encode(version=version)
    decoded = ListGroupsRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(DescribeClusterRequest.min_version, DescribeClusterRequest.max_version + 1))
def test_describe_cluster_request_roundtrip(version):
    request = DescribeClusterRequest(
        include_cluster_authorized_operations=True,
        endpoint_type=1 if version >= 1 else 1,
        include_fenced_brokers=False if version >= 2 else False
    )
    encoded = request.encode(version=version)
    decoded = DescribeClusterRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(DescribeConfigsRequest.min_version, DescribeConfigsRequest.max_version + 1))
def test_describe_configs_request_roundtrip(version):
    Resource = DescribeConfigsRequest.DescribeConfigsResource
    resources = [
        Resource(
            resource_type=2, # TOPIC
            resource_name="test-topic",
            configuration_keys=["cleanup.policy"]
        )
    ]
    request = DescribeConfigsRequest(
        resources=resources,
        include_synonyms=True if version >= 1 else False,
        include_documentation=True if version >= 3 else False
    )
    encoded = request.encode(version=version)
    decoded = DescribeConfigsRequest.decode(encoded, version=version)
    assert decoded == request

    Result = DescribeConfigsResponse.DescribeConfigsResult
    Config = Result.DescribeConfigsResourceResult
    Synonym = Config.DescribeConfigsSynonym
    response = DescribeConfigsResponse(
        throttle_time_ms=0,
        results=[
            DescribeConfigsResponse.DescribeConfigsResult(
                error_code=0,
                error_message=None,
                resource_type=2,
                resource_name="test-topic",
                configs=[
                    Config(
                        name='foo',
                        value='bar',
                        read_only=False,
                        config_source=2 if version >= 1 else -1,
                        is_default=True if version == 0 else False,
                        is_sensitive=False,
                        synonyms=[
                            Synonym(name='fizz', value='buzz', source=2)
                        ] if version >= 1 else [],
                        config_type=1 if version >=3 else 0,
                        documentation='whats up doc' if version >=3 else '',
                    )
                ]
            )
        ],
    )

    encoded = DescribeConfigsResponse.encode(response, version=version)
    decoded = DescribeConfigsResponse.decode(encoded, version=version)
    assert decoded == response


@pytest.mark.parametrize("version", range(CreateAclsRequest.min_version, CreateAclsRequest.max_version + 1))
def test_create_acls_request_roundtrip(version):
    Creation = CreateAclsRequest.AclCreation
    creations = [
        Creation(
            resource_type=2,
            resource_name="test-topic",
            resource_pattern_type=3,
            principal="User:alice",
            host="*",
            operation=3,
            permission_type=3
        )
    ]
    request = CreateAclsRequest(
        creations=creations
    )
    encoded = request.encode(version=version)
    decoded = CreateAclsRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(CreatePartitionsRequest.min_version, CreatePartitionsRequest.max_version + 1))
def test_create_partitions_request_roundtrip(version):
    TopicPartition = CreatePartitionsRequest.CreatePartitionsTopic
    Assignment = TopicPartition.CreatePartitionsAssignment
    topic_partitions = [
        TopicPartition(
            name="test-topic",
            count=2,
            assignments=[
                Assignment(broker_ids=[1, 2])
            ]
        )
    ]
    request = CreatePartitionsRequest(
        topics=topic_partitions,
        timeout_ms=10000,
        validate_only=False
    )
    encoded = request.encode(version=version)
    decoded = CreatePartitionsRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(DescribeAclsRequest.min_version, DescribeAclsRequest.max_version + 1))
def test_describe_acls_request_roundtrip(version):
    request = DescribeAclsRequest(
        resource_type_filter=2,
        resource_name_filter="test-topic",
        pattern_type_filter=3 if version >= 1 else 3,
        principal_filter="User:alice",
        host_filter="*",
        operation=3,
        permission_type=3
    )
    encoded = request.encode(version=version)
    decoded = DescribeAclsRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(DeleteAclsRequest.min_version, DeleteAclsRequest.max_version + 1))
def test_delete_acls_request_roundtrip(version):
    Filter = DeleteAclsRequest.DeleteAclsFilter
    filters = [
        Filter(
            resource_type_filter=2,
            resource_name_filter="test-topic",
            pattern_type_filter=3 if version >= 1 else 3,
            principal_filter="User:alice",
            host_filter="*",
            operation=3,
            permission_type=3
        )
    ]
    request = DeleteAclsRequest(
        filters=filters
    )
    encoded = request.encode(version=version)
    decoded = DeleteAclsRequest.decode(encoded, version=version)
    assert decoded == request
