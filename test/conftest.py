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
        MetadataResponse[0](
            [(0, 'foo', 12), (1, 'bar', 34)],  # brokers
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
