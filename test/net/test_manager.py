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


class TestKafkaConnectionManagerNetResolution:
    """Backend selection lives on the manager (not the compat shim)."""

    def test_default_resolves_to_selector(self):
        m = KafkaConnectionManager()
        assert isinstance(m._net, NetworkSelector)

    def test_explicit_instance_used_as_is(self, net):
        assert KafkaConnectionManager(net)._net is net

    def test_name_resolves(self):
        assert isinstance(KafkaConnectionManager('selector')._net, NetworkSelector)


class TestKafkaConnectionManagerConfig:
    def test_default_config(self, net):
        m = KafkaConnectionManager(net)
        assert m.config['reconnect_backoff_ms'] == 50
        assert m.config['reconnect_backoff_max_ms'] == 30000
        assert m.config['socket_connection_setup_timeout_ms'] == 10000
        assert m.config['socket_connection_setup_timeout_max_ms'] == 30000
        assert m.config['max_in_flight_requests_per_connection'] == 5

    def test_config_override(self, net):
        m = KafkaConnectionManager(net, reconnect_backoff_ms=100)
        assert m.config['reconnect_backoff_ms'] == 100

    def test_default_api_timeout_ms_default(self, net):
        m = KafkaConnectionManager(net)
        assert m.config['default_api_timeout_ms'] == 60000

    def test_default_api_timeout_ms_flows_to_net_backend(self):
        # net=None -> the manager constructs its own backend from config, so the
        # user-supplied default_api_timeout_ms must reach it (issue #3121).
        m = KafkaConnectionManager(default_api_timeout_ms=12345)
        try:
            assert m.config['default_api_timeout_ms'] == 12345
            assert m._net.config['default_api_timeout_ms'] == 12345
        finally:
            m.close()

    def test_initial_state(self, net):
        manager = KafkaConnectionManager(net)
        assert manager._conns == {}
        assert manager._backoff == {}
        assert manager.broker_version_data is None
        assert not manager.bootstrapped

    def test_api_versions(self, net):
        m = KafkaConnectionManager(net, api_version=(1, 0))
        assert m.broker_version_data == BrokerVersionData((1, 0))

    def test_create_future_forwards_to_backend(self, net):
        m = KafkaConnectionManager(net)
        fut = m.create_future()
        assert isinstance(fut, Future)
        assert not fut.is_done

    def test_client_dns_lookup_default(self, net):
        m = KafkaConnectionManager(net)
        assert m.config['client_dns_lookup'] == 'use_all_dns_ips'
        assert m.cluster.config['client_dns_lookup'] == 'use_all_dns_ips'

    def test_client_dns_lookup_canonical_passthrough(self, net):
        m = KafkaConnectionManager(net, client_dns_lookup='resolve_canonical_bootstrap_servers_only')
        assert m.cluster.config['client_dns_lookup'] == 'resolve_canonical_bootstrap_servers_only'

    def test_client_dns_lookup_invalid_rejected(self, net):
        with pytest.raises(ValueError, match='client_dns_lookup'):
            KafkaConnectionManager(net, client_dns_lookup='default')
        with pytest.raises(ValueError, match='client_dns_lookup'):
            KafkaConnectionManager(net, client_dns_lookup='garbage')


class TestKafkaConnectionManagerProxyConfig:
    def test_proxy_url_default_none(self, net):
        m = KafkaConnectionManager(net)
        assert m.config['proxy_url'] is None

    def test_proxy_url_passthrough(self, net):
        m = KafkaConnectionManager(net, proxy_url='socks5://proxy:1080')
        assert m.config['proxy_url'] == 'socks5://proxy:1080'

    def test_socks5_proxy_legacy_alias(self, net):
        m = KafkaConnectionManager(net, socks5_proxy='socks5://proxy:1080')
        assert m.config['proxy_url'] == 'socks5://proxy:1080'

    def test_proxy_url_takes_precedence_over_legacy(self, net):
        m = KafkaConnectionManager(
            net,
            socks5_proxy='socks5://legacy:1080',
            proxy_url='socks5://new:1080',
        )
        assert m.config['proxy_url'] == 'socks5://new:1080'

    def test_connect_passes_proxy_url(self, net):
        """_connect must forward the configured proxy_url to
        net.create_connection. Regression guard against the kwarg name
        drifting from the create_connection signature."""
        m = KafkaConnectionManager(net, proxy_url='socks5://proxy:1080')
        node = MagicMock(host='broker', port=9092, node_id='bootstrap-0')
        conn = KafkaConnection(net, node_id='bootstrap-0', **m.config)

        async def fake_create_connection(protocol, host, port, **kwargs):
            # Close mid-connect so _connect short-circuits before
            # connection_made()/initialize() -- we only assert the forwarded kwarg.
            conn.close()
            return MagicMock()

        with patch.object(net, 'create_connection',
                          side_effect=fake_create_connection) as mc:
            net.run(m._connect(node, conn))
        assert mc.call_args.kwargs.get('proxy_url') == 'socks5://proxy:1080'


class TestKafkaConnectionManagerBackoff:
    def test_connection_delay_no_backoff(self, manager):
        assert manager.connection_delay('node-1') == 0

    def test_update_backoff(self, manager):
        manager.update_backoff('node-1')
        assert manager.connection_delay('node-1') > 0
        failures, _, _ = manager._backoff['node-1']
        assert failures == 1

    def test_exponential_backoff(self, manager):
        manager.update_backoff('node-1')
        _, backoff1, connect1 = manager._backoff['node-1']
        manager.update_backoff('node-1')
        _, backoff2, connect2 = manager._backoff['node-1']
        failures, _, _ = manager._backoff['node-1']
        assert failures == 2
        # Second backoff should be later (exponential)
        assert backoff2 > backoff1
        # Second connect timeout should be greater
        assert connect2 > connect1

    def test_reset_backoff(self, manager):
        manager.update_backoff('node-1')
        assert manager.connection_delay('node-1') > 0
        manager.reset_backoff('node-1')
        assert manager.connection_delay('node-1') == 0

    def test_reset_backoff_nonexistent(self, manager):
        manager.reset_backoff('node-1')
        assert manager.connection_delay('node-1') == 0


class TestKafkaConnectionManagerAuthFailure:
    def test_no_failure_by_default(self, manager):
        assert manager.auth_failure('node-1') is None
        manager.maybe_raise_auth_failure('node-1')  # no-op

    def test_set_via_direct_dict_then_raise(self, manager):
        err = Errors.SaslAuthenticationFailedError('bad creds')
        manager._auth_failures['node-1'] = err
        assert manager.auth_failure('node-1') is err
        with pytest.raises(Errors.SaslAuthenticationFailedError):
            manager.maybe_raise_auth_failure('node-1')

    def test_successful_connect_clears_failure(self, manager):
        err = Errors.SaslAuthenticationFailedError('bad creds')
        manager._auth_failures['node-1'] = err
        # Simulate the body of _connect's success path.
        manager._auth_failures.pop('node-1', None)
        assert manager.auth_failure('node-1') is None

    def test_get_connection_raises_sticky_auth_failure(self, manager):
        err = Errors.SaslAuthenticationFailedError('bad creds')
        manager._auth_failures['bootstrap-0'] = err
        with pytest.raises(Errors.SaslAuthenticationFailedError):
            manager.get_connection('bootstrap-0')

    def test_send_propagates_sticky_auth_failure(self, manager):
        err = Errors.SaslAuthenticationFailedError('bad creds')
        manager._auth_failures['bootstrap-0'] = err
        with pytest.raises(Errors.SaslAuthenticationFailedError):
            manager.send(MagicMock(), node_id='bootstrap-0')


class TestKafkaConnectionManagerGetConnection:
    def test_get_connection_none_raises(self, manager):
        with pytest.raises(Errors.NodeNotReadyError):
            manager.get_connection(None)

    def test_get_connection_during_backoff_raises(self, manager):
        manager.update_backoff('bootstrap-0')
        with pytest.raises(Errors.NodeNotReadyError):
            manager.get_connection('bootstrap-0')

    def test_get_connection_creates_connection(self, net):
        manager = KafkaConnectionManager(net)
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
        with patch("time.monotonic", return_value=100.0):
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

    def test_send_forwards_request_timeout_ms_to_connection(self, manager):
        request = MagicMock()
        request.expect_response.return_value = True
        request.API_VERSION = 1
        captured = {}
        with patch.object(KafkaConnection, 'send_request', autospec=True) as mock:
            def capture(self_, req, request_timeout_ms=None):
                captured['request_timeout_ms'] = request_timeout_ms
                return Future()
            mock.side_effect = capture
            # Ensure node exists so get_connection returns a KafkaConnection.
            manager.send(request, node_id='bootstrap-0', request_timeout_ms=305000)
        assert captured['request_timeout_ms'] == 305000


class TestKafkaConnectionManagerBootstrap:
    def test_bootstrap_async_returns_future(self, manager):
        f = manager.bootstrap_async()
        assert isinstance(f, Future)
        assert not f.is_done

    def test_bootstrap_async_idempotent(self, manager):
        f1 = manager.bootstrap_async()
        f2 = manager.bootstrap_async()
        assert f1 is f2

    def test_bootstrap_connection_failure(self, net):
        manager = KafkaConnectionManager(net,
                                         bootstrap_servers=['localhost:1'],
                                         socket_connection_setup_timeout_ms=500,
                                         socket_connection_setup_timeout_max_ms=500,
                                         reconnect_backoff_ms=10,
                                         reconnect_backoff_max_ms=100)
        with pytest.raises(Errors.KafkaTimeoutError):
            manager.bootstrap(timeout_ms=2000)
        assert not manager._conns
        failures, _, _ = manager._backoff['bootstrap-0']
        assert failures > 1

    def test_bootstrap_handshake_failure_backs_off(self, net):
        """A broker that accepts the TCP connection but drops it during the
        version handshake must not cause the bootstrap loop to spin. Each such
        failure has to record backoff so retries are spaced out (otherwise the
        loop burns the entire bootstrap timeout retrying thousands of times)."""
        accepts = []
        stop = threading.Event()

        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(('127.0.0.1', 0))
        listener.listen(128)
        listener.settimeout(0.1)
        _, port = listener.getsockname()

        def serve():
            while not stop.is_set():
                try:
                    sock, _ = listener.accept()
                except socket.timeout:
                    continue
                except OSError:
                    break
                accepts.append(1)
                sock.close()  # drop the connection mid-handshake

        server = threading.Thread(target=serve)
        server.start()
        try:
            manager = KafkaConnectionManager(net,
                                             bootstrap_servers=['127.0.0.1:%d' % port],
                                             reconnect_backoff_ms=50,
                                             reconnect_backoff_max_ms=200)
            with pytest.raises(Errors.KafkaTimeoutError):
                manager.bootstrap(timeout_ms=1000)
        finally:
            stop.set()
            server.join()
            listener.close()

        # Backoff must have been recorded for the repeatedly-dropped bootstrap node.
        failures, _, _ = manager._backoff['bootstrap-0']
        assert failures > 1
        # With backoff applied, a 1s window allows only a handful of attempts.
        # Without it the loop spins (hundreds/thousands of connects).
        assert len(accepts) < 50, 'bootstrap spun without backoff: %d attempts' % len(accepts)

    def test_bootstrapped_property(self, manager):
        assert not manager.bootstrapped
        manager._bootstrap_future = Future()
        assert not manager.bootstrapped
        manager._bootstrap_future.success(True)
        assert manager.bootstrapped

    def test_bootstrap_retries_empty_brokers(self, manager):
        cluster = manager.cluster

        # First metadata update leaves brokers empty; second populates them.
        call_count = [0]
        def mock_update_metadata(response):
            call_count[0] += 1
            if call_count[0] >= 2:
                cluster._brokers = {1: MagicMock(node_id=1)}

        with patch.object(cluster, 'update_metadata', side_effect=mock_update_metadata):
            manager.bootstrap(timeout_ms=2000)

        assert manager.bootstrapped
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
            manager = KafkaConnectionManager(net, bootstrap_servers=['127.0.0.1:%d' % port], socket_connection_setup_timeout_ms=100)
            conn = manager.get_connection('bootstrap-0')
            net.poll(timeout_ms=1000, future=conn.init_future)
            assert conn.init_future.is_done
        finally:
            blocker.close()
            listener.close()


class TestKafkaConnectionManagerClose:
    def test_close_single_connection(self, manager, net):
        conn = manager.get_connection('bootstrap-0')
        assert 'bootstrap-0' in manager._conns
        manager.close('bootstrap-0')
        assert conn.init_future.is_done

    def test_close_all_connections(self, manager, net):
        manager.get_connection('bootstrap-0')
        assert len(manager._conns) > 0
        manager.close()
        # close_future callbacks should remove from _conns
        net.poll(timeout_ms=100)
        assert len(manager._conns) == 0

    def test_close_nonexistent_node(self, manager):
        # Should not raise
        manager.close('nonexistent')

    def test_close_no_connections(self, manager):
        # Should not raise
        manager.close()


class TestKafkaConnectionManagerConnectRace:
    """A connection can be closed (by manager.close() / bootstrap teardown)
    while its _connect() coroutine is still awaiting net.create_connection.
    When the transport finally arrives, _connect must not resurrect the dead
    connection via connection_made() -- doing so flips it back to
    `initializing`."""

    def test_connect_discards_transport_when_closed_during_build(self, net):
        manager = KafkaConnectionManager(net)
        node = MagicMock(host='broker', port=9092, node_id='bootstrap-0')
        conn = KafkaConnection(net, node_id='bootstrap-0', **manager.config)
        transport = MagicMock()

        async def fake_create_connection(protocol, host, port, **kwargs):
            # Simulate a concurrent close landing mid-connect, then the new
            # create_connection contract: the backend wires the protocol (which
            # refuses, since the conn closed) and cleans up the transport.
            conn.close()
            try:
                protocol.connection_made(transport)
            except Exception:
                transport.close()
                raise

        with patch.object(net, 'create_connection',
                          side_effect=fake_create_connection):
            net.run(manager._connect(node, conn))

        # Dead connection stays dead -- not resurrected to `initializing`.
        assert conn.closed
        assert conn.initializing is False
        assert conn.transport is None
        # And the orphaned transport is cleaned up rather than leaked.
        transport.close.assert_called_once()

    def test_connection_made_refuses_closed_connection(self, net):
        conn = KafkaConnection(net, node_id='bootstrap-0')
        conn.close()
        assert conn.closed
        with pytest.raises(Errors.KafkaConnectionError):
            conn.connection_made(MagicMock())


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

    def test_call_soon_does_not_raise(self, manager, net):
        async def bad_coro():
            raise ValueError('bad_coro')
        future = manager.call_soon(bad_coro)
        assert not future.is_done
        net.poll(future=future)
        assert future.failed()
        assert isinstance(future.exception, ValueError)
        assert future.exception.args[0] == 'bad_coro'

    def test_run_survives_gc_during_poll(self, manager, monkeypatch):
        """Regression: an aggressive gc.collect() between _poll_once
        iterations must not close orphan-cycle suspended coroutines and mask
        the real result with GeneratorExit.

        The wrapper Future returned by manager.call_soon pins its Task via
        a no-op callback so the cycle (Future_yielded <-> _poll_once cb <->
        Task <-> coroutine <-> Future_yielded) has an external reference
        for as long as the wrapper Future is pending.
        """
        import gc
        from kafka.net.selector import NetworkSelector

        # Force a GC cycle on every _poll_once entry to deterministically
        # trigger the orphan-collection race that was masking timeouts in CI.
        orig_poll_once = NetworkSelector._poll_once

        def aggressive_poll_once(self, timeout=None):
            gc.collect()
            return orig_poll_once(self, timeout)
        monkeypatch.setattr(NetworkSelector, '_poll_once', aggressive_poll_once)

        async def hangs_then_times_out():
            # Awaits a bare (loop-awaitable) future that nothing references
            # externally -- exactly the orphan-cycle shape that CPython's gc
            # collects.
            await manager.create_future()

        # wait_for should fail with KafkaTimeoutError, not GeneratorExit.
        async def waiter():
            inner = manager.call_soon(hangs_then_times_out)
            return await manager.wait_for(inner, timeout_ms=50)

        with pytest.raises(Errors.KafkaTimeoutError):
            manager.run(waiter)
