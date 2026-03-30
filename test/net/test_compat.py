import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from kafka.future import Future
from kafka.net.compat import KafkaNetClient
import kafka.errors as Errors


@pytest.fixture
def client():
    return KafkaNetClient(
        bootstrap_servers=['localhost:1'],
        socket_connection_timeout_ms=1000,
        reconnect_backoff_ms=10,
        reconnect_backoff_max_ms=100,
    )


@pytest.fixture
def manager(client):
    return client._manager


class TestKafkaNetClientInit:
    def test_has_lock(self, client):
        assert hasattr(client, '_lock')
        assert isinstance(client._lock, type(threading.RLock()))

    def test_cluster_property(self, client, manager):
        assert client.cluster is manager.cluster


class TestKafkaNetClientConnectionState:
    def test_connected_no_connection(self, client):
        assert not client.connected('nonexistent')

    def test_is_disconnected_no_connection(self, client):
        assert client.is_disconnected('nonexistent')

    def test_is_ready_no_connection(self, client):
        assert not client.is_ready('nonexistent')

    def test_connected_with_protocol(self, client, manager):
        conn = MagicMock()
        conn.connected = True
        conn.paused = set()
        manager._conns['node-1'] = conn
        assert client.connected('node-1')

    def test_is_ready_connected_not_paused(self, client, manager):
        conn = MagicMock()
        conn.connected = True
        conn.paused = set()
        manager._conns['node-1'] = conn
        assert client.is_ready('node-1')

    def test_is_ready_paused(self, client, manager):
        conn = MagicMock()
        conn.connected = True
        conn.paused = {'throttle'}
        manager._conns['node-1'] = conn
        assert not client.is_ready('node-1')


class TestKafkaNetClientReady:
    def test_ready_initiates_connection(self, client, manager):
        result = client.ready('bootstrap-0')
        assert 'bootstrap-0' in manager._conns

    def test_ready_during_backoff(self, client, manager):
        manager.update_backoff('bootstrap-0')
        result = client.ready('bootstrap-0')
        assert result is False


class TestKafkaNetClientMaybeConnect:
    def test_maybe_connect(self, client, manager):
        client.maybe_connect('bootstrap-0')
        assert 'bootstrap-0' in manager._conns

    def test_maybe_connect_during_backoff(self, client, manager):
        manager.update_backoff('bootstrap-0')
        # Should not raise
        client.maybe_connect('bootstrap-0')


class TestKafkaNetClientInFlight:
    def test_in_flight_no_connection(self, client):
        assert client.in_flight_request_count('nonexistent') == 0

    def test_in_flight_with_requests(self, client, manager):
        conn = MagicMock()
        conn.in_flight_requests = [1, 2, 3]
        manager._conns['node-1'] = conn
        assert client.in_flight_request_count('node-1') == 3


class TestKafkaNetClientBootstrap:
    def test_bootstrap_connected(self, client, manager):
        assert not client.bootstrap_connected()
        manager._bootstrap_future = Future()
        manager._bootstrap_future.success(True)
        assert client.bootstrap_connected()

    def test_get_broker_version(self, client, manager):
        assert client.get_broker_version() is None
        from kafka.protocol.broker_version_data import BrokerVersionData
        manager.broker_version_data = BrokerVersionData(api_versions={0: (0, 9), 1: (0, 9)})
        result = client.get_broker_version()
        assert isinstance(result, tuple)


class TestKafkaNetClientDelegation:
    def test_send(self, client, manager):
        request = MagicMock()
        f = client.send('bootstrap-0', request)
        assert isinstance(f, Future)

    def test_poll(self, client, manager):
        # Should not raise
        client.poll(timeout_ms=0)

    def test_least_loaded_node_before_bootstrap(self, client, manager):
        assert client.least_loaded_node() == 'bootstrap-0'

    def test_least_loaded_node_with_brokers(self, client, manager):
        from kafka.protocol.metadata import MetadataResponse
        broker = MetadataResponse.MetadataResponseBroker(node_id=1, host='localhost', port=9092, rack=None)
        manager.cluster._brokers = {1: broker}
        assert client.least_loaded_node() == 1

    def test_connection_delay(self, client, manager):
        assert client.connection_delay('node-1') == 0

    def test_close(self, client, manager):
        client.close()

    def test_close_node(self, client, manager):
        manager.get_connection('bootstrap-0')
        client.close('bootstrap-0')


class TestKafkaNetClientAwaitReady:
    def test_await_ready_already_connected(self, client, manager):
        conn = MagicMock()
        conn.connected = True
        conn.paused = set()
        manager._conns['node-1'] = conn
        assert client.await_ready('node-1', timeout_ms=100)

    def test_await_ready_timeout_raises(self, client, manager):
        with pytest.raises(Errors.KafkaConnectionError):
            client.await_ready('bootstrap-0', timeout_ms=100)


class TestKafkaNetClientSendAndReceive:
    def test_send_and_receive_timeout(self, client, manager):
        request = MagicMock()
        with pytest.raises(Errors.KafkaConnectionError):
            client.send_and_receive('bootstrap-0', request, timeout_ms=100)


class TestKafkaNetClientVersion:
    def test_get_broker_version_returns_tuple(self, client, manager):
        from kafka.protocol.broker_version_data import BrokerVersionData
        manager.broker_version_data = BrokerVersionData(api_versions={0: (0, 9), 1: (0, 9)})
        result = client.get_broker_version()
        assert isinstance(result, tuple)

    def test_get_broker_version_none(self, client, manager):
        assert client.get_broker_version() is None

    def test_check_version_returns_tuple(self, client, manager):
        from kafka.protocol.broker_version_data import BrokerVersionData
        manager.broker_version_data = BrokerVersionData(api_versions={0: (0, 9), 1: (0, 9)})
        # Pre-set bootstrap future so check_version doesn't try to connect
        manager._bootstrap_future = Future()
        manager._bootstrap_future.success(True)
        with patch.object(manager, 'bootstrap', return_value=manager._bootstrap_future):
            result = client.check_version()
        assert isinstance(result, tuple)


class TestKafkaNetClientWakeup:
    def test_wakeup(self, client):
        # Should not raise
        client.wakeup()
