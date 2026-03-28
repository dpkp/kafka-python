import uuid

import pytest

from kafka.protocol.new.metadata import MetadataRequest, MetadataResponse


@pytest.mark.parametrize("version", range(MetadataRequest.min_version, MetadataRequest.max_version + 1))
def test_metadata_request_roundtrip(version):
    Topic = MetadataRequest.MetadataRequestTopic
    request = MetadataRequest(
        topics=[
            Topic(
                name="topic-1" if version < 10 else None,
                topic_id=uuid.uuid4() if version >= 10 else None,
            ),
            Topic(
                name="topic-2" if version < 10 else None,
                topic_id=uuid.uuid4() if version >= 10 else None,
            ),
        ],
        allow_auto_topic_creation=False if version >= 4 else True,
        include_cluster_authorized_operations=True if 8 <= version <= 10 else False,
        include_topic_authorized_operations=True if version >= 8 else False
    )
    encoded = request.encode(version=version)
    decoded = MetadataRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(MetadataRequest.min_version, MetadataRequest.max_version + 1))
def test_metadata_request_all_topics(version):
    Topic = MetadataRequest.MetadataRequestTopic
    request = MetadataRequest(
        topics=None if version > 0 else [],
        allow_auto_topic_creation=False if version >= 4 else True,
        include_cluster_authorized_operations=True if 8 <= version <= 10 else False,
        include_topic_authorized_operations=True if version >= 8 else False
    )
    encoded = request.encode(version=version)
    decoded = MetadataRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(MetadataResponse.min_version, MetadataResponse.max_version + 1))
def test_metadata_response_roundtrip(version):
    Broker = MetadataResponse.MetadataResponseBroker
    Topic = MetadataResponse.MetadataResponseTopic
    Partition = Topic.MetadataResponsePartition
    response = MetadataResponse(
        throttle_time_ms=0,
        brokers=[
            Broker(
                node_id=1,
                host="localhost",
                port=9092,
                rack='A' if version >= 1 else None,
            ),
            Broker(
                node_id=2,
                host="localhost",
                port=9093,
                rack='B' if version >= 1 else None,
            )
        ],
        cluster_id='foo' if version >= 2 else None,
        controller_id=1 if version >= 1 else -1,
        topics=[
            Topic(
                error_code=0,
                name="topic-1",
                topic_id=uuid.uuid4() if version >= 10 else None,
                is_internal=True if version >= 1 else False,
                partitions=[
                    Partition(
                        error_code=0,
                        partition_index=0,
                        leader_id=1,
                        leader_epoch=321 if version >= 7 else -1,
                        replica_nodes=[1, 2],
                        isr_nodes=[1, 2],
                        offline_replicas=[3, 4] if version >= 5 else [],
                    )
                ],
                authorized_operations={1, 2, 3} if version >= 8 else None,
            )
        ],
        authorized_operations={4, 5, 6} if 8 <= version <= 10 else None,
    )
    encoded = response.encode(version=version)
    decoded = MetadataResponse.decode(encoded, version=version)
    assert decoded == response
