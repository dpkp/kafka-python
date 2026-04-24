import socket
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from kafka.cluster import ClusterMetadata
from kafka.future import Future
from kafka.net.selector import NetworkSelector
from kafka.net.manager import KafkaConnectionManager
from kafka.net.connection import KafkaConnection
import kafka.errors as Errors
from kafka.protocol.broker_version_data import BrokerVersionData


@pytest.fixture
def net():
    return NetworkSelector()


@pytest.fixture
def cluster():
    return ClusterMetadata(bootstrap_servers=['localhost:9092'])


@pytest.fixture
def manager(net, cluster):
    return KafkaConnectionManager(net, cluster,
                                  socket_connection_timeout_ms=1000,
                                  reconnect_backoff_ms=10,
                                  reconnect_backoff_max_ms=100)


class TestKafkaConnectionManagerConfig:
    def test_default_config(self, net, cluster):
        m = KafkaConnectionManager(net, cluster)
        assert m.config['reconnect_backoff_ms'] == 50
        assert m.config['reconnect_backoff_max_ms'] == 30000
        assert m.config['socket_connection_timeout_ms'] == 5000
        assert m.config['max_in_flight_requests_per_connection'] == 5

    def test_config_override(self, net, cluster):
        m = KafkaConnectionManager(net, cluster, reconnect_backoff_ms=100)
        assert m.config['reconnect_backoff_ms'] == 100

    def test_initial_state(self, manager):
        assert manager._conns == {}
        assert manager._backoff == {}
        assert manager.broker_version_data is None
        assert not manager.bootstrapped

    def test_api_versions(self, net, cluster):
        m = KafkaConnectionManager(net, cluster, api_version=(1, 0))
        assert m.broker_version_data == BrokerVersionData((1, 0))


class TestKafkaConnectionManagerBackoff:
    def test_connection_delay_no_backoff(self, manager):
        assert manager.connection_delay('node-1') == 0

    def test_update_backoff(self, manager):
        manager.update_backoff('node-1')
        assert manager.connection_delay('node-1') > 0
        failures, _ = manager._backoff['node-1']
        assert failures == 1

    def test_exponential_backoff(self, manager):
        manager.update_backoff('node-1')
        _, backoff1 = manager._backoff['node-1']
        manager.update_backoff('node-1')
        _, backoff2 = manager._backoff['node-1']
        failures, _ = manager._backoff['node-1']
        assert failures == 2
        # Second backoff should be later (exponential)
        assert backoff2 > backoff1

    def test_reset_backoff(self, manager):
        manager.update_backoff('node-1')
        assert manager.connection_delay('node-1') > 0
        manager.reset_backoff('node-1')
        assert manager.connection_delay('node-1') == 0

    def test_reset_backoff_nonexistent(self, manager):
        manager.reset_backoff('node-1')
        assert manager.connection_delay('node-1') == 0


class TestKafkaConnectionManagerGetConnection:
    def test_get_connection_none_raises(self, manager):
        with pytest.raises(Errors.NodeNotReadyError):
            manager.get_connection(None)

    def test_get_connection_during_backoff_raises(self, manager):
        manager.update_backoff('bootstrap-0')
        with pytest.raises(Errors.NodeNotReadyError):
            manager.get_connection('bootstrap-0')

    def test_get_connection_creates_connection(self, manager):
        conn = manager.get_connection('bootstrap-0')
        assert isinstance(conn, KafkaConnection)
        assert conn.node_id == 'bootstrap-0'
        assert 'bootstrap-0' in manager._conns
        assert conn.broker_version_data is None

    def test_get_connection_returns_cached(self, manager):
        conn1 = manager.get_connection('bootstrap-0')
        conn2 = manager.get_connection('bootstrap-0')
        assert conn1 is conn2

    def test_get_connection_passes_broker_version_data(self, manager):
        manager.broker_version_data = BrokerVersionData((4, 0))
        conn = manager.get_connection('bootstrap-0')
        assert isinstance(conn, KafkaConnection)
        assert conn.broker_version_data == BrokerVersionData((4, 0))


class TestKafkaConnectionManagerSend:
    def test_send_during_backoff(self, manager):
        manager.update_backoff('bootstrap-0')
        f = manager.send(MagicMock(), node_id='bootstrap-0')
        assert f.failed()
        assert isinstance(f.exception, Errors.NodeNotReadyError)

    def test_send_buffers_during_init(self, manager):
        request = MagicMock()
        f = manager.send(request, node_id='bootstrap-0')
        assert not f.is_done
        conn = manager._conns['bootstrap-0']
        assert len(conn._request_buffer) == 1

    def test_send_no_node_before_bootstrap(self, manager):
        # Before bootstrap, least_loaded_node returns None -- send fails
        request = MagicMock()
        f = manager.send(request)
        assert f.failed()
        assert isinstance(f.exception, Errors.NodeNotReadyError)


class TestKafkaConnectionManagerBootstrap:
    def test_bootstrap_returns_future(self, manager):
        f = manager.bootstrap()
        assert isinstance(f, Future)
        assert not f.is_done

    def test_bootstrap_idempotent(self, manager):
        f1 = manager.bootstrap()
        f2 = manager.bootstrap()
        assert f1 is f2

    def test_bootstrap_connection_failure(self, net):
        cluster = ClusterMetadata(bootstrap_servers=['localhost:1'])
        manager = KafkaConnectionManager(net, cluster,
                                         socket_connection_timeout_ms=500,
                                         reconnect_backoff_ms=10,
                                         reconnect_backoff_max_ms=100)
        f = manager.bootstrap(timeout_ms=2000)
        manager.poll(timeout_ms=3000, future=f)
        assert f.failed()
        assert isinstance(f.exception, Errors.KafkaConnectionError)
        assert not manager._conns
        failures, _ = manager._backoff['bootstrap-0']
        assert failures > 1

    def test_bootstrapped_property(self, manager):
        assert not manager.bootstrapped
        manager._bootstrap_future = Future()
        assert not manager.bootstrapped
        manager._bootstrap_future.success(True)
        assert manager.bootstrapped

    def test_bootstrap_retries_empty_brokers(self, net):
        cluster = ClusterMetadata(bootstrap_servers=['localhost:9092'])
        manager = KafkaConnectionManager(net, cluster,
                                         socket_connection_timeout_ms=1000,
                                         reconnect_backoff_ms=10)
        conn = MagicMock()
        conn.connected = True
        conn.init_future = Future()
        conn.init_future.success(True)
        conn.close_future = Future()
        conn.paused = set()
        conn.in_flight_requests = []

        call_count = [0]
        def mock_send_request(request):
            call_count[0] += 1
            f = Future()
            f.success(MagicMock())
            return f
        conn.send_request.side_effect = mock_send_request

        # The bootstrap loop closes/pops the conn in a `finally` on every
        # iteration, so patch get_connection to hand back the same mock on
        # each retry instead of letting the manager open a fresh real socket.
        def mock_get_connection(node_id, **kwargs):
            manager._conns[node_id] = conn
            return conn

        # First update_metadata leaves brokers empty; second populates them.
        # The real update_metadata also clears metadata_refresh_in_progress
        # so subsequent metadata_request() calls don't raise.
        def mock_update_metadata(response):
            cluster.metadata_refresh_in_progress = False
            if call_count[0] >= 2:
                cluster._brokers = {1: MagicMock(node_id=1)}

        with patch.object(manager, 'get_connection', side_effect=mock_get_connection), \
             patch.object(cluster, 'update_metadata', side_effect=mock_update_metadata):
            f = manager.bootstrap(timeout_ms=2000)
            manager.poll(timeout_ms=2000, future=f)

        assert f.succeeded()
        assert call_count[0] >= 2


class TestKafkaConnectionManagerLeastLoaded:
    def test_no_brokers_before_bootstrap(self, manager):
        # Before bootstrap, brokers() returns empty -- least_loaded_node returns None
        assert manager.least_loaded_node() is None

    def test_prefers_connected_idle(self, manager):
        conn1 = MagicMock()
        conn1.connected = True
        conn1.paused = set()
        conn1.in_flight_requests = []
        manager._conns['node-1'] = conn1

        conn2 = MagicMock()
        conn2.connected = True
        conn2.paused = set()
        conn2.in_flight_requests = [1]
        manager._conns['node-2'] = conn2

        # Need brokers in cluster for least_loaded_node
        broker1 = MagicMock()
        broker1.node_id = 'node-1'
        broker2 = MagicMock()
        broker2.node_id = 'node-2'
        with patch.object(manager.cluster, 'brokers', return_value=[broker1, broker2]):
            result = manager.least_loaded_node()
            assert result == 'node-1'


class TestKafkaConnectionManagerConnectionTimeout:
    def test_connect_to_timeout_fires(self, net):
        """The timeout scheduled in connect_to should close the
        connection when it does not complete in time."""
        # Listen but never accept -- connect will hang in EINPROGRESS
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(('127.0.0.1', 0))
        listener.listen(0)  # backlog=0: may refuse after 1 pending
        _, port = listener.getsockname()

        # Fill the single-connection backlog so subsequent connects hang
        blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        blocker.connect(('127.0.0.1', port))

        try:
            cluster = ClusterMetadata(bootstrap_servers=['127.0.0.1:%d' % port])
            manager = KafkaConnectionManager(net, cluster, socket_connection_timeout_ms=100)
            conn = manager.get_connection('bootstrap-0')
            manager.poll(timeout_ms=1000, future=conn.init_future)
            assert conn.init_future.is_done
        finally:
            blocker.close()
            listener.close()


class TestKafkaConnectionManagerMetadataRefresh:
    def test_update_metadata_returns_future(self, manager):
        f = manager.update_metadata()
        assert isinstance(f, Future)
        assert not f.is_done

    def test_update_metadata_deduplicates(self, manager):
        f1 = manager.update_metadata()
        f2 = manager.update_metadata()
        assert f1 is f2

    def test_update_metadata_new_future_after_done(self, manager):
        f1 = manager.update_metadata()
        f1.success(True)
        f2 = manager.update_metadata()
        assert f2 is not f1

    def test_update_metadata_sets_cluster_need_update(self, manager):
        manager.update_metadata()
        assert manager.cluster._need_update

    def test_refresh_metadata_schedules_next(self, net, cluster):
        manager = KafkaConnectionManager(net, cluster,
                                         socket_connection_timeout_ms=1000,
                                         reconnect_backoff_ms=10)
        # Simulate a connected node
        conn = MagicMock()
        conn.connected = True
        conn.init_future = Future().success(True)
        conn.paused = set()
        conn.in_flight_requests = []
        conn.send_request.return_value = Future()
        manager._conns['node-1'] = conn
        with patch.object(cluster, 'brokers', return_value=[MagicMock(node_id='node-1')]):
            f = manager.update_metadata()
            # Run the scheduled _refresh_metadata task
            net.poll(timeout_ms=100)
            # Should have called send_request on the connection
            assert conn.send_request.called

    def test_refresh_metadata_retries_no_node(self, net, cluster):
        manager = KafkaConnectionManager(net, cluster,
                                         socket_connection_timeout_ms=1000,
                                         reconnect_backoff_ms=50)
        # No connected nodes, empty cluster
        with patch.object(cluster, 'brokers', return_value=[]):
            f = manager.update_metadata()
            net.poll(timeout_ms=0)
            # Should not have resolved yet (retry scheduled)
            assert not f.is_done
            # Should have a scheduled retry
            assert len(net._scheduled) > 0

    def test_bootstrap_triggers_refresh_loop(self, net, cluster):
        """bootstrap() schedules the periodic metadata refresh loop, so
        update_metadata fires without anyone calling it from compat.poll()."""
        manager = KafkaConnectionManager(net, cluster,
                                         socket_connection_timeout_ms=1000,
                                         reconnect_backoff_ms=10)
        assert manager._refresh_loop_task is None
        call_count = [0]
        done = threading.Event()
        orig = manager.update_metadata

        def counting_update():
            call_count[0] += 1
            if call_count[0] >= 1:
                done.set()
            return orig()

        with patch.object(manager, 'update_metadata', side_effect=counting_update):
            manager.bootstrap(timeout_ms=100)
            assert manager._refresh_loop_task is not None
            manager.start()
            try:
                assert done.wait(timeout=2), "refresh loop never called update_metadata"
            finally:
                manager.stop(timeout=2)
        assert call_count[0] >= 1

    def test_refresh_loop_spawned_once(self, net, cluster):
        """Calling bootstrap() multiple times must not spawn multiple refresh
        loop tasks."""
        manager = KafkaConnectionManager(net, cluster,
                                         socket_connection_timeout_ms=1000,
                                         reconnect_backoff_ms=10)
        manager.bootstrap(timeout_ms=100)
        task = manager._refresh_loop_task
        assert task is not None
        manager.bootstrap(timeout_ms=100)
        assert manager._refresh_loop_task is task


class TestKafkaConnectionManagerClose:
    def test_close_single_connection(self, manager):
        conn = manager.get_connection('bootstrap-0')
        assert 'bootstrap-0' in manager._conns
        manager.close('bootstrap-0')
        assert conn.init_future.is_done

    def test_close_all_connections(self, manager):
        manager.get_connection('bootstrap-0')
        assert len(manager._conns) > 0
        manager.close()
        # close_future callbacks should remove from _conns
        manager.poll(timeout_ms=100)
        assert len(manager._conns) == 0

    def test_close_nonexistent_node(self, manager):
        # Should not raise
        manager.close('nonexistent')

    def test_close_no_connections(self, manager):
        # Should not raise
        manager.close()


class TestKafkaConnectionManagerRun:
    def test_run_function(self, manager):
        def test_coro():
            return 42
        assert manager.run(test_coro) == 42

    def test_run_async_coro_function(self, manager):
        async def test_coro():
            return 100
        assert manager.run(test_coro) == 100

    def test_run_async_coro_with_args(self, manager):
        async def test_coro(foo):
            return foo
        assert manager.run(test_coro, 123) == 123

    def test_run_async_coro(self, manager):
        async def test_coro():
            return 49
        assert manager.run(test_coro()) == 49

    def test_run_async_chain(self, manager):
        async def test_coro_foo():
            return 'foo!'
        async def test_coro_bar():
            return await test_coro_foo()
        assert manager.run(test_coro_bar()) == 'foo!'

    def test_run_raises(self, manager):
        async def bad_coro():
            raise ValueError('bad_coro')
        with pytest.raises(ValueError, match='bad_coro'):
            manager.run(bad_coro)

    def test_call_soon_does_not_raise(self, manager):
        async def bad_coro():
            raise ValueError('bad_coro')
        future = manager.call_soon(bad_coro)
        assert not future.is_done
        manager.poll(future=future)
        assert future.failed()
        assert isinstance(future.exception, ValueError)
        assert future.exception.args[0] == 'bad_coro'
