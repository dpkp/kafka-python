import pytest
import time
from kafka.errors import BrokerNotAvailableError

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
    # Mock api_version to avoid IncompatibleBrokerVersion error
    mocker.patch.object(client, 'api_version', return_value=(1, 0, 0))

    try:
        yield admin_client
    finally:
        admin_client.close()

def test_send_request_to_node_broker_not_in_cluster(kafka_admin_client, mocker):
    """Test that _send_request_to_node raises BrokerNotAvailableError when broker is no longer in cluster"""
    from kafka.protocol.metadata import MetadataRequest

    # Mock the needed methods
    mocker.patch.object(kafka_admin_client._client, 'ready', return_value=False)

    # Return empty broker list to simulate broker no longer in cluster
    mocker.patch.object(kafka_admin_client._client.cluster, 'brokers', return_value=[])

    # Use a node_id that doesn't exist in the cluster
    node_id = 999
    request = MetadataRequest[1]([])  # Use v1 or higher to have controller_id

    # Should raise BrokerNotAvailableError when node is not in cluster
    with pytest.raises(BrokerNotAvailableError, match=f"Broker {node_id} no longer in cluster"):
        kafka_admin_client._send_request_to_node(node_id, request)

def test_send_request_to_node_timeout(kafka_admin_client, mocker):
    """Test that _send_request_to_node raises BrokerNotAvailableError when timeout is reached"""
    from kafka.protocol.metadata import MetadataRequest
    import time

    # Mock the needed methods
    mocker.patch.object(kafka_admin_client._client, 'ready', return_value=False)

    # Return a broker list with our node_id to prevent the "no longer in cluster" error
    mock_broker = mocker.MagicMock()
    mock_broker.nodeId = 999
    mocker.patch.object(kafka_admin_client._client.cluster, 'brokers', return_value=[mock_broker])

    # Mock time.time to simulate timeout
    real_time = time.time
    time_values = [100.0, 100.0, 131.0]  # Start time, check time, final time (over 30s default timeout)
    mocker.patch('time.time', side_effect=time_values)

    # Use a node_id that exists but is not ready
    node_id = 999
    request = MetadataRequest[1]([])  # Use v1 or higher to have controller_id

    # Should raise BrokerNotAvailableError when timeout is reached
    with pytest.raises(BrokerNotAvailableError, match=f"Broker {node_id} is not available after 30s"):
        kafka_admin_client._send_request_to_node(node_id, request)

def test_retry_get_cluster_metadata(kafka_admin_client, mocker):
    """Test that _get_cluster_metadata retries when BrokerNotAvailableError occurs"""
    from kafka.protocol.metadata import MetadataRequest, MetadataResponse
    from kafka.errors import BrokerNotAvailableError
    import time

    # Mock time.sleep to avoid actual delays
    mocker.patch('time.sleep')

    # Mock _send_request_to_node to fail twice and then succeed
    mock_response = mocker.MagicMock()
    mock_future = mocker.MagicMock()
    mock_future.value = mock_response

    send_request_mock = mocker.patch.object(
        kafka_admin_client, 
        '_send_request_to_node', 
        side_effect=[
            BrokerNotAvailableError("First attempt fails"),
            BrokerNotAvailableError("Second attempt fails"),
            mock_future
        ]
    )

    # Mock _wait_for_futures to do nothing since we're returning a mock future
    mocker.patch.object(kafka_admin_client, '_wait_for_futures')

    # Call the method
    result = kafka_admin_client._get_cluster_metadata()

    # Assert we got the expected result
    assert result == mock_response

    # Assert we called _send_request_to_node 3 times
    assert send_request_mock.call_count == 3
