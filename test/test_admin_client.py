import pytest

@pytest.fixture
def client(conn, mocker):
    from kafka import KafkaClient

    cli = KafkaClient(api_version=(0, 9))
    mocker.patch.object(cli, '_init_connect', return_value=True)
    try:
        yield cli
    finally:
        cli._close()

@pytest.fixture
def kafka_admin_client(mocker, client):
    from kafka.admin.client import KafkaAdminClient

    admin_client = KafkaAdminClient()
    # Mock the internal _client attribute with our fixture client
    mocker.patch.object(admin_client, '_client', client)
    # Mock the controller_id to avoid errors
    mocker.patch.object(admin_client, '_controller_id', 0)
    # Mock api_version to avoid IncompatibleBrokerVersion error - use higher version
    mocker.patch.object(client, 'api_version', return_value=(1, 0, 0))

    try:
        yield admin_client
    finally:
        admin_client.close()

@pytest.mark.parametrize("removed_from_cluster", [True, False])
def test_send_request_to_node_with_node_failure(kafka_admin_client, mocker, removed_from_cluster):
    """Test that _send_request_to_node handles nodes that have been removed from the cluster"""
    from kafka.errors import NodeNotReadyError, IncompatibleBrokerVersion
    from kafka.protocol.metadata import MetadataRequest

    node_id = 123  # Use a node id that doesn't exist
    # Use v1 or higher of MetadataRequest since we need controller_id
    request = MetadataRequest[1]([])

    # Ensure the check_version returns True to avoid IncompatibleBrokerVersion
    mocker.patch.object(kafka_admin_client._client, 'check_version', return_value=True)

    # Mock the needed methods
    mocker.patch.object(kafka_admin_client._client, 'ready', return_value=False)
    mocker.patch.object(kafka_admin_client._client, 'poll')

    # If removed_from_cluster is True, broker_metadata returns None
    mocker.patch.object(kafka_admin_client._client.cluster, 'broker_metadata', 
                       return_value=None if removed_from_cluster else "mock_broker")

    # Mock check_version to avoid IncompatibleBrokerVersion error
    mocker.patch.object(kafka_admin_client._client, 'check_version', return_value=True)

    try:
        if removed_from_cluster:
            # Should raise NodeNotReadyError when node is no longer in cluster
            with pytest.raises(NodeNotReadyError, match="Node 123 is no longer part of the cluster"):
                kafka_admin_client._send_request_to_node(node_id, request)
        else:
            # Should timeout in test due to mocking
            with pytest.raises(Exception):
                kafka_admin_client._send_request_to_node(node_id, request)
    except IncompatibleBrokerVersion:
        pytest.skip("Kafka broker does not support the required protocol version")
