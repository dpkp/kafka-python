from __future__ import absolute_import

import pytest


@pytest.fixture
def metrics():
    from kafka.metrics import Metrics

    metrics = Metrics()
    try:
        yield metrics
    finally:
        metrics.close()


@pytest.fixture
def conn(mocker):
    """Return a connection mocker fixture"""
    from kafka.conn import ConnectionStates
    from kafka.future import Future
    from kafka.protocol.metadata import MetadataResponse
    conn = mocker.patch('kafka.client_async.BrokerConnection')
    conn.return_value = conn
    conn.state = ConnectionStates.CONNECTED
    conn.send.return_value = Future().success(
        MetadataResponse[1](
            [(0, 'foo', 12, None), (1, 'bar', 34, None)],  # brokers with rack=None
            0,  # controller_id
            []))  # topics
    conn.connection_delay.return_value = 0
    conn.blacked_out.return_value = False
    conn.next_ifr_request_timeout_ms.return_value = float('inf')
    def _set_conn_state(state):
        conn.state = state
        return state
    conn._set_conn_state = _set_conn_state
    conn.connect.side_effect = lambda: conn.state
    conn.connect_blocking.return_value = True
    conn.connecting = lambda: conn.state in (ConnectionStates.CONNECTING,
                                             ConnectionStates.HANDSHAKE)
    conn.connected = lambda: conn.state is ConnectionStates.CONNECTED
    conn.disconnected = lambda: conn.state is ConnectionStates.DISCONNECTED
    return conn


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
def kafka_broker(mocker, client):
    """Mock kafka_broker fixture for tests that depend on it"""
    class MockKafkaBroker:
        def __init__(self):
            self.bootstrap_server = 'localhost:9092'
            # Patch client.api_version to return a proper tuple and make it consistent
            # with tests to avoid version compatibility issues
            mocker.patch.object(client, 'api_version', return_value=(1, 0, 0))
            mocker.patch.object(client, 'check_version', return_value=True)

        def create_topic(self, topic_name, replication_factor=1):
            return topic_name

        def get_producers(self, cnt=1, **configs):
            """Helper method to create producers for testing"""
            from kafka import KafkaProducer
            for _ in range(cnt):
                producer_config = {
                    'bootstrap_servers': self.bootstrap_server,
                    'api_version': (1, 0, 0),  # Use a consistent version
                }
                producer_config.update(configs)
                yield KafkaProducer(**producer_config)

    return MockKafkaBroker()
