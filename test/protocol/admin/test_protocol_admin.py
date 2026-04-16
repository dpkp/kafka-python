import uuid

import pytest

from kafka.protocol.admin import (
    CreateTopicsRequest, CreateTopicsResponse,
    DeleteTopicsRequest, DeleteTopicsResponse,
    CreatePartitionsRequest, CreatePartitionsResponse,
    DescribeGroupsRequest, DescribeGroupsResponse,
    ListGroupsRequest, ListGroupsResponse,
    DeleteGroupsRequest, DeleteGroupsResponse,
    DescribeClusterRequest, DescribeClusterResponse,
    DescribeConfigsRequest, DescribeConfigsResponse,
    AlterConfigsRequest, AlterConfigsResponse,
    AlterUserScramCredentialsRequest, AlterUserScramCredentialsResponse,
    CreateAclsRequest, CreateAclsResponse,
    DeleteAclsRequest, DeleteAclsResponse,
    DescribeAclsRequest, DescribeAclsResponse,
)


@pytest.mark.parametrize("version", range(CreateTopicsRequest.min_version, CreateTopicsRequest.max_version + 1))
def test_create_topics_request_roundtrip(version):
    Topic = CreateTopicsRequest.CreatableTopic
    Assignment = Topic.CreatableReplicaAssignment
    Config = Topic.CreatableTopicConfig
    request = CreateTopicsRequest(
        topics=[
            Topic(
                name="test-topic",
                num_partitions=1,
                replication_factor=1,
                assignments=[
                    Assignment(
                        partition_index=1,
                        broker_ids=[1, 2, 3],
                    ),
                ],
                configs=[
                    Config(
                        name='foo',
                        value='bar',
                    ),
                ]
            )
        ],
        timeout_ms=10000,
        validate_only=False
    )
    encoded = request.encode(version=version)
    decoded = CreateTopicsRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(CreateTopicsResponse.min_version, CreateTopicsResponse.max_version + 1))
def test_create_topics_response_roundtrip(version):
    Topic = CreateTopicsResponse.CreatableTopicResult
    Config = Topic.CreatableTopicConfigs
    response = CreateTopicsResponse(
        throttle_time_ms=123 if version >= 2 else 0,
        topics=[
            Topic(
                name="test-topic",
                topic_id=uuid.uuid4() if version >= 7 else None,
                error_code=13,
                error_message='foo' if version >= 1 else '',
                topic_config_error_code=2 if version >= 5 else 0,
                num_partitions=1 if version >= 5 else -1,
                replication_factor=1 if version >= 5 else -1,
                configs=[
                    Config(
                        name='foo',
                        value='bar',
                        read_only=True,
                        config_source=3,
                        is_sensitive=True,
                    ),
                ] if version >= 5 else [],
            )
        ],
    )
    encoded = response.encode(version=version)
    decoded = CreateTopicsResponse.decode(encoded, version=version)
    assert decoded == response


@pytest.mark.parametrize("version", range(DeleteTopicsRequest.min_version, DeleteTopicsRequest.max_version + 1))
def test_delete_topics_request_roundtrip(version):
    Topic = DeleteTopicsRequest.DeleteTopicState
    request = DeleteTopicsRequest(
        topics=[
            Topic(name="topic-1", topic_id=uuid.uuid4()),
            Topic(name="topic-2", topic_id=uuid.uuid4()),
        ] if version >= 6 else [],
        topic_names=["topic-1", "topic-2"] if version < 6 else [],
        timeout_ms=10000,
    )
    encoded = request.encode(version=version)
    decoded = DeleteTopicsRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(DeleteTopicsResponse.min_version, DeleteTopicsResponse.max_version + 1))
def test_delete_topics_response_roundtrip(version):
    Topic = DeleteTopicsResponse.DeletableTopicResult
    response = DeleteTopicsResponse(
        throttle_time_ms=123 if version >= 1 else 0,
        responses=[
            Topic(
                name="topic-1",
                error_code=123,
                error_message='foo' if version >= 5 else None,
            )
        ],
    )
    encoded = response.encode(version=version)
    decoded = DeleteTopicsResponse.decode(encoded, version=version)
    assert decoded == response


@pytest.mark.parametrize("version", range(CreatePartitionsRequest.min_version, CreatePartitionsRequest.max_version + 1))
def test_create_partitions_request_roundtrip(version):
    TopicPartition = CreatePartitionsRequest.CreatePartitionsTopic
    Assignment = TopicPartition.CreatePartitionsAssignment
    request = CreatePartitionsRequest(
        topics=[
            TopicPartition(
                name="test-topic",
                count=2,
                assignments=[
                    Assignment(broker_ids=[1, 2])
                ]
            )
        ],
        timeout_ms=10000,
        validate_only=False
    )
    encoded = request.encode(version=version)
    decoded = CreatePartitionsRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(CreatePartitionsResponse.min_version, CreatePartitionsResponse.max_version + 1))
def test_create_partitions_response_roundtrip(version):
    Result = CreatePartitionsResponse.CreatePartitionsTopicResult
    response = CreatePartitionsResponse(
        throttle_time_ms=123,
        results=[
            Result(
                name='topic-foo',
                error_code=123,
                error_message='error',
            ),
        ],
    )
    encoded = response.encode(version=version)
    decoded = CreatePartitionsResponse.decode(encoded, version=version)
    assert decoded == response


@pytest.mark.parametrize("version", range(DescribeGroupsRequest.min_version, DescribeGroupsRequest.max_version + 1))
def test_describe_groups_request_roundtrip(version):
    request = DescribeGroupsRequest(
        groups=["group-1"],
        include_authorized_operations=True if version >= 3 else False
    )
    encoded = request.encode(version=version)
    decoded = DescribeGroupsRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(DescribeGroupsResponse.min_version, DescribeGroupsResponse.max_version + 1))
def test_describe_groups_response_roundtrip(version):
    Group = DescribeGroupsResponse.DescribedGroup
    Member = Group.DescribedGroupMember
    response = DescribeGroupsResponse(
        throttle_time_ms=123 if version >= 1 else 0,
        groups=[
            Group(
                error_code=3,
                error_message='foo' if version >= 6 else None,
                group_id='group-1',
                group_state='good',
                protocol_type='fizz',
                protocol_data='buzz',
                members=[
                    Member(
                        member_id='abcd',
                        group_instance_id='efgh' if version >= 4 else None,
                        client_id='client',
                        client_host='localhost',
                        member_metadata=b'\xab\x12',
                        member_assignment=b'\x72\xff',
                    ),
                ],
                authorized_operations={1} if version >= 3 else None,
            )
        ],
    )
    encoded = DescribeGroupsResponse.encode(response, version=version)
    decoded = DescribeGroupsResponse.decode(encoded, version=version)
    assert decoded == response


@pytest.mark.parametrize("version", range(ListGroupsRequest.min_version, ListGroupsRequest.max_version + 1))
def test_list_groups_request_roundtrip(version):
    request = ListGroupsRequest(
        states_filter=["Stable"] if version >= 4 else []
    )
    encoded = request.encode(version=version)
    decoded = ListGroupsRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(ListGroupsResponse.min_version, ListGroupsResponse.max_version + 1))
def test_list_groups_response_roundtrip(version):
    Group = ListGroupsResponse.ListedGroup
    response = ListGroupsResponse(
        throttle_time_ms=123 if version >= 1 else 0,
        error_code=2,
        groups=[
            Group(
                group_id='group-1',
                protocol_type='foobar',
                group_state='stable' if version >= 4 else '',
                group_type='type' if version >= 5 else '',
            )
        ],
    )
    encoded = response.encode(version=version)
    decoded = ListGroupsResponse.decode(encoded, version=version)
    assert decoded == response


@pytest.mark.parametrize("version", range(DeleteGroupsRequest.min_version, DeleteGroupsRequest.max_version + 1))
def test_delete_groups_request_roundtrip(version):
    request = DeleteGroupsRequest(
        groups_names=['foo', 'bar'],
    )
    encoded = request.encode(version=version)
    decoded = DeleteGroupsRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(DeleteGroupsResponse.min_version, DeleteGroupsResponse.max_version + 1))
def test_delete_groups_response_roundtrip(version):
    Result = DeleteGroupsResponse.DeletableGroupResult
    response = DeleteGroupsResponse(
        throttle_time_ms=123,
        results=[
            Result(
                group_id='group-1',
                error_code=23,
            )
        ],
    )
    encoded = response.encode(version=version)
    decoded = DeleteGroupsResponse.decode(encoded, version=version)
    assert decoded == response


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


@pytest.mark.parametrize("version", range(DescribeClusterResponse.min_version, DescribeClusterResponse.max_version + 1))
def test_describe_cluster_response_roundtrip(version):
    Broker = DescribeClusterResponse.DescribeClusterBroker
    response = DescribeClusterResponse(
        throttle_time_ms=123,
        error_code=3,
        error_message='error',
        endpoint_type=12 if version >= 1 else 1,
        cluster_id='cluster',
        controller_id=2,
        brokers=[
            Broker(
                broker_id=2,
                host='localhost',
                port=9000,
                rack='AA',
                is_fenced=True if version >= 2 else False,
            )
        ],
        cluster_authorized_operations={2},
    )
    encoded = response.encode(version=version)
    decoded = DescribeClusterResponse.decode(encoded, version=version)
    assert decoded == response


@pytest.mark.parametrize("version", range(DescribeConfigsRequest.min_version, DescribeConfigsRequest.max_version + 1))
def test_describe_configs_request_roundtrip(version):
    Resource = DescribeConfigsRequest.DescribeConfigsResource
    request = DescribeConfigsRequest(
        resources=[
            Resource(
                resource_type=2,
                resource_name="test-topic",
                configuration_keys=["cleanup.policy"]
            )
        ],
        include_synonyms=True if version >= 1 else False,
        include_documentation=True if version >= 3 else False
    )
    encoded = request.encode(version=version)
    decoded = DescribeConfigsRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(DescribeConfigsResponse.min_version, DescribeConfigsResponse.max_version + 1))
def test_describe_configs_response_roundtrip(version):
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


@pytest.mark.parametrize("version", range(AlterConfigsRequest.min_version, AlterConfigsRequest.max_version + 1))
def test_alter_configs_request_roundtrip(version):
    Resource = AlterConfigsRequest.AlterConfigsResource
    Config = Resource.AlterableConfig
    request = AlterConfigsRequest(
        resources=[
            Resource(
                resource_type=2,
                resource_name="test-topic",
                configs=[
                    Config(
                        name='foo',
                        value='bar',
                    ),
                ],
            )
        ],
        validate_only=True,
    )
    encoded = request.encode(version=version)
    decoded = AlterConfigsRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(AlterConfigsResponse.min_version, AlterConfigsResponse.max_version + 1))
def test_alter_configs_response_roundtrip(version):
    Response = AlterConfigsResponse.AlterConfigsResourceResponse
    response = AlterConfigsResponse(
        throttle_time_ms=123,
        responses=[
            Response(
                error_code=22,
                error_message='error',
                resource_type=2,
                resource_name="test-topic",
            )
        ],
    )
    encoded = response.encode(version=version)
    decoded = AlterConfigsResponse.decode(encoded, version=version)
    assert decoded == response


@pytest.mark.parametrize("version", range(AlterUserScramCredentialsRequest.min_version, AlterUserScramCredentialsRequest.max_version + 1))
def test_alter_user_scram_credentials_request_roundtrip(version):
    Deletion = AlterUserScramCredentialsRequest.ScramCredentialDeletion
    Upsertion = AlterUserScramCredentialsRequest.ScramCredentialUpsertion
    request = AlterUserScramCredentialsRequest(
        deletions=[
            Deletion(name='alice', mechanism=1),
        ],
        upsertions=[
            Upsertion(
                name='bob',
                mechanism=2,
                iterations=8192,
                salt=b'\x00\x01\x02\x03',
                salted_password=b'\xaa\xbb\xcc\xdd',
            ),
        ],
    )
    encoded = request.encode(version=version)
    decoded = AlterUserScramCredentialsRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(AlterUserScramCredentialsResponse.min_version, AlterUserScramCredentialsResponse.max_version + 1))
def test_alter_user_scram_credentials_response_roundtrip(version):
    Result = AlterUserScramCredentialsResponse.AlterUserScramCredentialsResult
    response = AlterUserScramCredentialsResponse(
        throttle_time_ms=123,
        results=[
            Result(user='alice', error_code=0, error_message=None),
            Result(user='bob', error_code=58, error_message='bad mechanism'),
        ],
    )
    encoded = response.encode(version=version)
    decoded = AlterUserScramCredentialsResponse.decode(encoded, version=version)
    assert decoded == response


@pytest.mark.parametrize("version", range(CreateAclsRequest.min_version, CreateAclsRequest.max_version + 1))
def test_create_acls_request_roundtrip(version):
    Creation = CreateAclsRequest.AclCreation
    request = CreateAclsRequest(
        creations=[
            Creation(
                resource_type=2,
                resource_name="test-topic",
                resource_pattern_type=3,
                principal="User:alice",
                host="*",
                operation=3,
                permission_type=3
            )
        ],
    )
    encoded = request.encode(version=version)
    decoded = CreateAclsRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(CreateAclsResponse.min_version, CreateAclsResponse.max_version + 1))
def test_create_acls_response_roundtrip(version):
    Result = CreateAclsResponse.AclCreationResult
    response = CreateAclsResponse(
        throttle_time_ms=123,
        results=[
            Result(
                error_code=2,
                error_message='foo',
            ),
        ],
    )
    encoded = response.encode(version=version)
    decoded = CreateAclsResponse.decode(encoded, version=version)
    assert decoded == response


@pytest.mark.parametrize("version", range(DeleteAclsRequest.min_version, DeleteAclsRequest.max_version + 1))
def test_delete_acls_request_roundtrip(version):
    Filter = DeleteAclsRequest.DeleteAclsFilter
    request = DeleteAclsRequest(
        filters=[
            Filter(
                resource_type_filter=2,
                resource_name_filter="test-topic",
                pattern_type_filter=3 if version >= 1 else 3,
                principal_filter="User:alice",
                host_filter="*",
                operation=3,
                permission_type=3
            )
        ],
    )
    encoded = request.encode(version=version)
    decoded = DeleteAclsRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(DeleteAclsResponse.min_version, DeleteAclsResponse.max_version + 1))
def test_delete_acls_response_roundtrip(version):
    Result = DeleteAclsResponse.DeleteAclsFilterResult
    Acl = Result.DeleteAclsMatchingAcl
    response = DeleteAclsResponse(
        throttle_time_ms=123,
        filter_results=[
            Result(
                error_code=12,
                error_message='error',
                matching_acls=[
                    Acl(
                        error_code=2,
                        error_message='fizz',
                        resource_type=2,
                        resource_name="test-topic",
                        pattern_type=3 if version >= 1 else 3,
                        principal="User:alice",
                        host="localhost",
                        operation=3,
                        permission_type=3
                    ),
                ]
            ),
        ]
    )
    encoded = response.encode(version=version)
    decoded = DeleteAclsResponse.decode(encoded, version=version)
    assert decoded == response


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


@pytest.mark.parametrize("version", range(DescribeAclsResponse.min_version, DescribeAclsResponse.max_version + 1))
def test_describe_acls_response_roundtrip(version):
    Resource = DescribeAclsResponse.DescribeAclsResource
    Acl = Resource.AclDescription
    response = DescribeAclsResponse(
        throttle_time_ms=123,
        error_code=1,
        error_message='error',
        resources=[
            Resource(
                resource_type=2,
                resource_name="test-topic",
                pattern_type=3 if version >= 1 else 3,
                acls=[
                    Acl(
                        principal="User:alice",
                        host="localhost",
                        operation=3,
                        permission_type=3
                    ),
                ],
            ),
        ],
    )
    encoded = response.encode(version=version)
    decoded = DescribeAclsResponse.decode(encoded, version=version)
    assert decoded == response
