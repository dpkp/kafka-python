import asyncio
import socket
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import kafka.errors as Errors
from kafka.net.proxy import KafkaTCPProxy, KafkaTCPProxyStates

from kafka.net.http_connect import HttpConnectProxyProtocol
from kafka.net.socks5 import Socks5ProxyProtocol


def _drive(coro):
    """Run a create_connection coroutine to completion off any real IO loop.

    The orchestration tests stub every await point (backend + do_connect), so a
    throwaway asyncio loop is enough to drive create_connection deterministically.
    """
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class _StubNet:
    """Minimal NetBackend stand-in for exercising proxy.create_connection.

    Records getaddrinfo / create_connection calls and, like a real backend,
    wires a transport onto the protocol via connection (here: sets _transport).
    """

    def __init__(self, addrs=()):
        self.addrs = list(addrs)
        self.getaddrinfo_calls = 0
        self.create_connection_calls = []
        self.transports = []

    async def getaddrinfo(self, host, port):
        self.getaddrinfo_calls += 1
        return self.addrs

    async def create_connection(self, protocol, host, port, *, ssl=None,
                                socket_options=(), timeout_at=None):
        transport = MagicMock(name='proxy_transport')
        self.create_connection_calls.append((protocol, host, port))
        self.transports.append(transport)
        protocol._transport = transport


def _stub_do_connect(proxy, outcomes):
    """Replace proxy.do_connect with a scripted sequence.

    Each outcome is either None (negotiation succeeded) or an exception to raise
    (negotiation failed for that addrinfo).
    """
    it = iter(outcomes)

    async def _do_connect(timeout_at=None):
        outcome = next(it)
        if isinstance(outcome, BaseException):
            raise outcome

    proxy.do_connect = _do_connect


_IPV4_ADDR = (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, '', ('10.0.0.1', 9092))
_IPV4_ADDR2 = (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, '', ('10.0.0.2', 9092))


class TestKafkaTCPProxyRegistry:
    def test_socks5(self, net):
        assert 'socks5' in KafkaTCPProxy._registry
        factory = KafkaTCPProxy(net, 'socks5://foo.bar')
        assert isinstance(factory, Socks5ProxyProtocol)

    def test_socks5h(self, net):
        assert 'socks5h' in KafkaTCPProxy._registry
        factory = KafkaTCPProxy(net, 'socks5h://foo.bar')
        assert isinstance(factory, Socks5ProxyProtocol)

    def test_http(self, net):
        assert 'http' in KafkaTCPProxy._registry
        factory = KafkaTCPProxy(net, 'http://proxy:8080')
        assert isinstance(factory, HttpConnectProxyProtocol)

    def test_unknown_scheme_raises(self, net):
        with pytest.raises(ValueError, match='Unsupported proxy url scheme'):
            KafkaTCPProxy(net, 'ftp://proxy:8080')

    def test_no_scheme_raises(self, net):
        with pytest.raises(ValueError, match='scheme'):
            KafkaTCPProxy(net, 'no-scheme')

    def test_empty_string_raises(self, net):
        with pytest.raises(ValueError, match='scheme'):
            KafkaTCPProxy(net, '')

    def test_kafka_net_import_registers_socks5(self):
        # Importing kafka.net must register Socks5ProxyProtocol. Regression guard
        # against accidentally dropping the import from kafka/net/__init__.py.
        import kafka.net  # noqa: F401
        assert KafkaTCPProxy._registry['socks5'] is Socks5ProxyProtocol
        assert KafkaTCPProxy._registry['socks5h'] is Socks5ProxyProtocol

    def test_subclass_auto_registers(self, net):
        class _TestProxy(KafkaTCPProxy):
            SCHEMES = ('test-autoregister',)
            def __init__(self, net, proxy_url):
                self.proxy_url = proxy_url
        try:
            assert KafkaTCPProxy._registry['test-autoregister'] is _TestProxy
            proxy = KafkaTCPProxy(net, 'test-autoregister://x')
            assert isinstance(proxy, _TestProxy)
            assert proxy.proxy_url == 'test-autoregister://x'  # pylint: disable=no-member
        finally:
            KafkaTCPProxy._registry.pop('test-autoregister', None)

    def test_duplicate_scheme_last_wins(self):
        prior = KafkaTCPProxy._registry.get('test-dup')
        class _First(KafkaTCPProxy):
            SCHEMES = ('test-dup',)
        class _Second(KafkaTCPProxy):
            SCHEMES = ('test-dup',)
        try:
            assert KafkaTCPProxy._registry['test-dup'] is _Second
        finally:
            if prior is None:
                KafkaTCPProxy._registry.pop('test-dup', None)
            else:
                KafkaTCPProxy._registry['test-dup'] = prior


class TestCreateConnectionOrchestration:
    """P3: exercise create_connection's DNS / retry / handoff wiring with the
    per-proxy negotiation (do_connect) stubbed out."""

    def test_remote_lookup_skips_dns_and_connects_to_proxy(self):
        # http proxy uses remote lookup: no local getaddrinfo, connect to proxy.
        net = _StubNet()
        proxy = KafkaTCPProxy(net, 'http://proxy:8080')
        _stub_do_connect(proxy, [None])
        protocol = MagicMock()

        _drive(proxy.create_connection(protocol, 'broker', 9092))

        assert net.getaddrinfo_calls == 0
        assert net.create_connection_calls == [(proxy, 'proxy', 8080)]
        # transport handed off to the real protocol, then released by the proxy
        protocol.connection_made.assert_called_once_with(net.transports[0])
        assert proxy._transport is None

    def test_local_lookup_resolves_then_connects(self):
        # socks5 (no 'h') resolves the broker locally before connecting.
        net = _StubNet(addrs=[_IPV4_ADDR])
        proxy = KafkaTCPProxy(net, 'socks5://proxy:1080')
        _stub_do_connect(proxy, [None])
        protocol = MagicMock()

        _drive(proxy.create_connection(protocol, 'broker', 9092))

        assert net.getaddrinfo_calls == 1
        assert net.create_connection_calls == [(proxy, 'proxy', 1080)]
        protocol.connection_made.assert_called_once_with(net.transports[0])

    def test_retries_next_address_on_failure(self):
        net = _StubNet(addrs=[_IPV4_ADDR, _IPV4_ADDR2])
        proxy = KafkaTCPProxy(net, 'socks5://proxy:1080')
        # first addr fails negotiation, second succeeds
        _stub_do_connect(proxy, [Errors.KafkaConnectionError('nope'), None])
        protocol = MagicMock()

        _drive(proxy.create_connection(protocol, 'broker', 9092))

        assert len(net.create_connection_calls) == 2
        # handed off with the second (successful) transport
        protocol.connection_made.assert_called_once_with(net.transports[1])

    def test_all_addresses_fail_raises(self):
        net = _StubNet(addrs=[_IPV4_ADDR, _IPV4_ADDR2])
        proxy = KafkaTCPProxy(net, 'socks5://proxy:1080')
        _stub_do_connect(proxy, [Errors.KafkaConnectionError('nope'),
                                 Errors.KafkaConnectionError('nope2')])
        protocol = MagicMock()

        with pytest.raises(Errors.KafkaConnectionError, match='Unable to connect'):
            _drive(proxy.create_connection(protocol, 'broker', 9092))
        assert len(net.create_connection_calls) == 2
        protocol.connection_made.assert_not_called()

    def test_protocol_refusing_transport_aborts_and_reraises(self):
        # KafkaConnection may refuse the transport (closed mid-connect) by
        # raising from connection_made; the proxy must abort it and propagate.
        net = _StubNet()
        proxy = KafkaTCPProxy(net, 'http://proxy:8080')
        _stub_do_connect(proxy, [None])
        boom = Errors.KafkaConnectionError('closed during connect')
        protocol = MagicMock()
        protocol.connection_made.side_effect = boom

        with pytest.raises(Errors.KafkaConnectionError, match='closed during connect'):
            _drive(proxy.create_connection(protocol, 'broker', 9092))
        net.transports[0].abort.assert_called_once_with(boom)


class TestCreateConnectionSSL:
    """P5: TLS is layered on top of the established proxy tunnel."""

    @patch('kafka.net.proxy.KafkaSSLTransport')
    def test_ssl_wraps_tunnel_before_handoff(self, mock_ssl_cls):
        net = _StubNet()
        proxy = KafkaTCPProxy(net, 'http://proxy:8080')
        _stub_do_connect(proxy, [None])
        ssl_wrapper = mock_ssl_cls.return_value
        ssl_wrapper.handshake = AsyncMock()
        ssl_ctx = MagicMock(name='ssl_ctx')
        protocol = MagicMock()

        _drive(proxy.create_connection(protocol, 'broker', 9092, ssl=ssl_ctx))

        tunnel = net.transports[0]
        # wrapper built for the broker host, layered onto the tunnel, handshaked
        mock_ssl_cls.assert_called_once_with(net, ssl_ctx, host='broker')
        ssl_wrapper.connection_made.assert_called_once_with(tunnel)
        ssl_wrapper.handshake.assert_awaited_once()
        # the real protocol receives the SSL transport, not the raw tunnel
        protocol.connection_made.assert_called_once_with(ssl_wrapper)
        assert proxy._transport is None

    def test_no_ssl_hands_off_raw_tunnel(self):
        net = _StubNet()
        proxy = KafkaTCPProxy(net, 'http://proxy:8080')
        _stub_do_connect(proxy, [None])
        protocol = MagicMock()

        _drive(proxy.create_connection(protocol, 'broker', 9092, ssl=None))

        protocol.connection_made.assert_called_once_with(net.transports[0])


class TestConnectionLost:
    """P4: a transport drop mid-negotiation must fail the pending connect
    cleanly (regression for connection_lost calling the .failure setter, not
    the .failed predicate)."""

    TARGET = (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, '', ('1.2.3.4', 9092))

    def test_drop_mid_negotiation_fails_pending_connect(self, net):
        proxy = KafkaTCPProxy(net, 'socks5://proxy:1080')
        proxy.set_addrinfo(self.TARGET)
        proxy.connection_made(MagicMock())  # negotiation started, future pending
        assert not proxy._connect_future.is_done

        exc = Errors.KafkaConnectionError('proxy dropped')
        proxy.connection_lost(exc)

        assert proxy._connect_future.failed()
        assert proxy._connect_future.exception is exc
        assert proxy._state == KafkaTCPProxyStates.DISCONNECTED

    def test_lost_before_connect_is_noop(self, net):
        # _connect_future is None before connection_made; must not raise.
        proxy = KafkaTCPProxy(net, 'socks5://proxy:1080')
        proxy.connection_lost(Errors.KafkaConnectionError('early drop'))
        assert proxy._state == KafkaTCPProxyStates.DISCONNECTED

    def test_lost_after_complete_does_not_overwrite(self, net):
        proxy = KafkaTCPProxy(net, 'socks5://proxy:1080')
        proxy.set_addrinfo(self.TARGET)
        proxy.connection_made(MagicMock())
        proxy.data_received(b'\x05\x00')  # no-auth selected -> request submitted
        # grant: VER REP RSV ATYP=ipv4 + 4 addr + 2 port
        proxy.data_received(b'\x05\x00\x00\x01' + b'\x7f\x00\x00\x01' + b'\x00\x00')
        assert proxy._connect_future.succeeded()

        proxy.connection_lost(Errors.KafkaConnectionError('late drop'))
        # already-resolved future is left untouched
        assert proxy._connect_future.succeeded()
