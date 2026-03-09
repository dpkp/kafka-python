import pytest

from kafka.protocol.new.messages.metadata import MetadataRequest, MetadataResponse


@pytest.mark.parametrize("version", range(MetadataRequest.min_version, MetadataRequest.max_version + 1))
def test_metadata_request_roundtrip(version):
    # MetadataRequest v0 is basic, v12 is flexible
    topics = [
        MetadataRequest.MetadataRequestTopic(name="topic-1"),
        MetadataRequest.MetadataRequestTopic(name="topic-2")
    ]
    # In v0-v3, allow_auto_topic_creation is not in schema, so it won't be encoded/decoded.
    # But when we decode, the resulting data object will have the default value (True).
    data = MetadataRequest(
        topics=topics,
        allow_auto_topic_creation=True, # Always True to match decoded default
        include_cluster_authorized_operations=False,
        include_topic_authorized_operations=True if version >= 8 else False
    )

    encoded = MetadataRequest.encode(data, version=version)
    decoded = MetadataRequest.decode(encoded, version=version)
    assert decoded == data


@pytest.mark.parametrize("version", range(MetadataResponse.min_version, MetadataResponse.max_version + 1))
def test_metadata_response_roundtrip(version):
    # Mock some data for response
    brokers = [
        MetadataResponse.MetadataResponseBroker(node_id=1, host="localhost", port=9092, rack=None),
        MetadataResponse.MetadataResponseBroker(node_id=2, host="localhost", port=9093, rack=None)
    ]
    topics = [
        MetadataResponse.MetadataResponseTopic(
            error_code=0,
            name="topic-1",
            is_internal=False,
            partitions=[
                MetadataResponse.MetadataResponseTopic.MetadataResponsePartition(
                    error_code=0,
                    partition_index=0,
                    leader_id=1,
                    replica_nodes=[1, 2],
                    isr_nodes=[1, 2],
                    offline_replicas=[]
                )
            ],
            topic_authorized_operations=-2147483648
        )
    ]
    data = MetadataResponse(
        throttle_time_ms=0,
        brokers=brokers,
        cluster_id=None,
        controller_id=1 if version >= 1 else -1,
        topics=topics,
        cluster_authorized_operations=-2147483648
    )

    encoded = MetadataResponse.encode(data, version=version)
    decoded = MetadataResponse.decode(encoded, version=version)
    assert decoded == data
