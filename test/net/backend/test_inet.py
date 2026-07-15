import errno
import socket
from unittest.mock import MagicMock, call, patch

import pytest

from kafka.net.backend.inet import create_connection, KafkaNetSocket
from kafka.net.socks5 import Socks5Proxy
from kafka.net.http_connect import HttpConnectProxy
import kafka.errors as Errors


class TestDnsLookup:
    def test_valid_host(self):
        results = KafkaNetSocket().dns_lookup('localhost', 9092)
        assert len(results) > 0
        for res in results:
            assert len(res) == 5

    def test_invalid_host(self):
        with patch('kafka.net.backend.inet.socket.getaddrinfo', side_effect=socket.gaierror):
            results = KafkaNetSocket().dns_lookup('invalid.host', 9092)
            assert results == []

    def test_numeric_host(self):
        results = KafkaNetSocket().dns_lookup('127.0.0.1', 9092)
        assert len(results) > 0
        assert results[0][4][0] == '127.0.0.1'


class TestSockConnect:
    def test_immediate_connect(self, net):
        factory = KafkaNetSocket()
        sock = MagicMock()
        sock.connect_ex.return_value = 0
        result = net.run(factory.sock_connect(net, sock, ('127.0.0.1', 9092)))
        assert result is sock
        sock.connect_ex.assert_called_once_with(('127.0.0.1', 9092))

    def test_eisconn(self, net):
        factory = KafkaNetSocket()
        sock = MagicMock()
        sock.connect_ex.return_value = errno.EISCONN
        result = net.run(factory.sock_connect(net, sock, ('127.0.0.1', 9092)))
        assert result is sock

    def test_connection_refused(self, net):
        factory = KafkaNetSocket()
        sock = MagicMock()
        sock.connect_ex.return_value = errno.ECONNREFUSED
        with pytest.raises(Errors.KafkaConnectionError):
            net.run(factory.sock_connect(net, sock, ('127.0.0.1', 9092)))

    def test_socket_error_uses_errno(self, net):
        factory = KafkaNetSocket()
        sock = MagicMock()
        sock.connect_ex.side_effect = socket.error(errno.ECONNREFUSED, 'refused')
        with pytest.raises(Errors.KafkaConnectionError):
            net.run(factory.sock_connect(net, sock, ('127.0.0.1', 9092)))

    def test_error_after_wait_write(self, net):
        """connect_ex returns EINPROGRESS, then after wait_write fires the
        second connect_ex returns the real error."""
        factory = KafkaNetSocket()
        # socketpair endpoints are always immediately writable, so wait_write
        # fires on the first poll and we re-enter the loop.
        rsock, wsock = socket.socketpair()
        rsock.setblocking(False)
        wsock.setblocking(False)
        try:
            sock = MagicMock()
            sock.connect_ex.side_effect = [errno.EINPROGRESS, errno.ECONNREFUSED]
            sock.fileno.return_value = wsock.fileno()
            with pytest.raises(Errors.KafkaConnectionError):
                net.run(factory.sock_connect(net, sock, ('127.0.0.1', 9092)))
            assert sock.connect_ex.call_count == 2
        finally:
            rsock.close()
            wsock.close()


class TestCreateConnection:
    def test_dns_failure(self, net):
        with patch('kafka.net.backend.inet.KafkaNetSocket.dns_lookup', return_value=[]):
            with pytest.raises(Errors.KafkaConnectionError, match='DNS'):
                net.run(create_connection(net, 'badhost', 9092))

    def test_socket_init_failure(self, net):
        fake_addr = [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('127.0.0.1', 9092))]
        with patch('kafka.net.backend.inet.KafkaNetSocket.dns_lookup', return_value=fake_addr), \
             patch('kafka.net.backend.inet.socket.socket', side_effect=OSError('no socket')):
             with pytest.raises(Errors.KafkaConnectionError):
                net.run(create_connection(net, 'host', 9092))

    def test_successful_connection(self, net):
        fake_addr = [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('127.0.0.1', 9092))]
        mock_sock = MagicMock()
        mock_sock.connect_ex.return_value = 0
        with patch('kafka.net.backend.inet.KafkaNetSocket.dns_lookup', return_value=fake_addr), \
             patch('kafka.net.backend.inet.socket.socket', return_value=mock_sock):
            result = net.run(
                create_connection(net, 'host', 9092))
            assert result is mock_sock
            mock_sock.setblocking.assert_called_with(False)

    def test_tries_multiple_addresses(self, net):
        addr1 = (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('10.0.0.1', 9092))
        addr2 = (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('10.0.0.2', 9092))
        mock_sock1 = MagicMock()
        mock_sock1.connect_ex.return_value = errno.ECONNREFUSED
        mock_sock2 = MagicMock()
        mock_sock2.connect_ex.return_value = 0
        sockets = iter([mock_sock1, mock_sock2])
        with patch('kafka.net.backend.inet.KafkaNetSocket.dns_lookup', return_value=[addr1, addr2]), \
             patch('kafka.net.backend.inet.socket.socket', side_effect=lambda *a: next(sockets)):
            result = net.run(
                create_connection(net, 'host', 9092))
            assert result is mock_sock2

    def test_socket_options_applied(self, net):
        fake_addr = [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('127.0.0.1', 9092))]
        mock_sock = MagicMock()
        mock_sock.connect_ex.return_value = 0
        opts = [
            (socket.IPPROTO_TCP, socket.TCP_NODELAY, 1),
            (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
        ]
        with patch('kafka.net.backend.inet.KafkaNetSocket.dns_lookup', return_value=fake_addr), \
             patch('kafka.net.backend.inet.socket.socket', return_value=mock_sock):
            net.run(create_connection(net, 'host', 9092, socket_options=opts))
        mock_sock.setsockopt.assert_has_calls([
            call(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1),
            call(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
        ])
        assert mock_sock.setsockopt.call_count == 2


class TestCreateConnectionWithProxy:
    def test_proxy_creates_socket(self, net):
        mock_sock = MagicMock()
        mock_sock.connect_ex.return_value = 0
        fake_addr = (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('127.0.0.1', 9092))
        with patch('kafka.net.socks5.Socks5Proxy._get_proxy_addr'), \
             patch('kafka.net.backend.inet.KafkaNetSocket.dns_lookup', return_value=[fake_addr]), \
             patch('kafka.net.socks5.Socks5Proxy.connect', return_value=mock_sock) as mock_connect:
            result = net.run(
                create_connection(net, 'broker', 9092, proxy_url='socks5://proxy:1080'))
            mock_connect.assert_called_once_with(net, fake_addr, (), timeout_at=None)
            assert result is mock_sock

    def test_proxy_remote_dns_skips_local_lookup(self, net):
        mock_sock = MagicMock()
        mock_sock.connect_ex.return_value = 0
        with patch('kafka.net.socks5.Socks5Proxy._get_proxy_addr'), \
             patch('kafka.net.socks5.Socks5Proxy.socket', return_value=mock_sock), \
             patch('kafka.net.socks5.Socks5Proxy.connect_ex', return_value=0), \
             patch('kafka.net.backend.inet.KafkaNetSocket.dns_lookup') as mock_dns:
            result = net.run(
                create_connection(net, 'broker', 9092, proxy_url='socks5h://proxy:1080'))
            mock_dns.assert_not_called()

    def test_no_proxy_uses_direct_socket(self, net):
        fake_addr = (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('127.0.0.1', 9092))
        mock_sock = MagicMock()
        mock_sock.connect_ex.return_value = 0
        with patch('kafka.net.backend.inet.KafkaNetSocket.dns_lookup', return_value=[fake_addr]), \
             patch('kafka.net.backend.inet.socket.socket', return_value=mock_sock), \
             patch('kafka.net.socks5.Socks5Proxy.connect') as mock_connect:
            result = net.run(
                create_connection(net, 'host', 9092))
            mock_connect.assert_not_called()
            assert result is mock_sock

    def test_socks5h_does_dns_for_proxy_not_target(self, net):
        """Companion to test_proxy_remote_dns_skips_local_lookup: with
        _get_proxy_addr running normally, exactly one dns_lookup is made and
        it is for the proxy hostname, not the target."""
        proxy_addr = (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('1.2.3.4', 1080))
        mock_sock = MagicMock()
        with patch('kafka.net.backend.inet.KafkaNetSocket.dns_lookup', return_value=[proxy_addr]) as mock_dns, \
             patch('kafka.net.socks5.Socks5Proxy.socket', return_value=mock_sock), \
             patch('kafka.net.socks5.Socks5Proxy.connect_ex', return_value=0):
            net.run(
                create_connection(net, 'broker', 9092, proxy_url='socks5h://proxy:1080'))
        assert mock_dns.call_count == 1
        assert mock_dns.call_args.args[:2] == ('proxy', 1080)

    def test_socks5_proxy_dns_gaierror_raises(self):
        with patch('kafka.net.backend.inet.socket.getaddrinfo', side_effect=socket.gaierror):
            with pytest.raises(Errors.KafkaConnectionError):
                KafkaNetSocket('socks5://bogus.proxy:1080')

    def test_socks5_proxy_dns_empty_raises(self):
        with patch('kafka.net.backend.inet.KafkaNetSocket.dns_lookup', return_value=[]):
            with pytest.raises(Errors.KafkaConnectionError):
                KafkaNetSocket('socks5://proxy:1080')

    def test_proxy_connect_dispatches_through_inherited_connect(self, net):
        """create_connection -> Socks5Proxy.connect (inherited from
        KafkaNetSocket) -> Socks5Proxy.socket + Socks5Proxy.connect_ex."""
        fake_addr = (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('127.0.0.1', 9092))
        mock_sock = MagicMock()
        with patch('kafka.net.socks5.Socks5Proxy._get_proxy_addr'), \
             patch('kafka.net.backend.inet.KafkaNetSocket.dns_lookup', return_value=[fake_addr]), \
             patch('kafka.net.socks5.Socks5Proxy.socket', return_value=mock_sock) as mock_socket, \
             patch('kafka.net.socks5.Socks5Proxy.connect_ex', return_value=0) as mock_connect_ex:
            result = net.run(
                create_connection(net, 'broker', 9092, proxy_url='socks5://proxy:1080'))
        mock_socket.assert_called_once_with(socket.AF_INET, socket.SOCK_STREAM, 6)
        mock_connect_ex.assert_called_once_with(mock_sock, ('127.0.0.1', 9092))
        assert result is mock_sock


class TestKafkaNetSocketRegistry:
    def test_socks5(self):
        assert 'socks5' in KafkaNetSocket._registry
        with patch('kafka.net.socks5.Socks5Proxy._get_proxy_addr'):
            factory = KafkaNetSocket('socks5://foo.bar')
        assert isinstance(factory, Socks5Proxy)

    def test_socks5h(self):
        assert 'socks5h' in KafkaNetSocket._registry
        with patch('kafka.net.socks5.Socks5Proxy._get_proxy_addr'):
            factory = KafkaNetSocket('socks5h://foo.bar')
        assert isinstance(factory, Socks5Proxy)

    def test_http(self):
        assert 'http' in KafkaNetSocket._registry
        with patch('kafka.net.http_connect.HttpConnectProxy._get_proxy_addr'):
            factory = KafkaNetSocket('http://proxy:8080')
        assert isinstance(factory, HttpConnectProxy)

    def test_default(self):
        factory = KafkaNetSocket()
        assert type(factory) is KafkaNetSocket

    def test_unknown_scheme_raises(self):
        with pytest.raises(ValueError, match='Unsupported proxy url scheme'):
            KafkaNetSocket('ftp://proxy:8080')

    def test_no_scheme_raises(self):
        with pytest.raises(ValueError, match='scheme'):
            KafkaNetSocket('no-scheme')

    def test_empty_string_raises(self):
        with pytest.raises(ValueError, match='scheme'):
            KafkaNetSocket('')

    def test_kafka_net_import_registers_socks5(self):
        # Importing kafka.net must register Socks5Proxy. Regression guard
        # against accidentally dropping the import from kafka/net/__init__.py.
        import kafka.net  # noqa: F401
        assert KafkaNetSocket._registry['socks5'] is Socks5Proxy
        assert KafkaNetSocket._registry['socks5h'] is Socks5Proxy

    def test_subclass_auto_registers(self):
        class _TestProxy(KafkaNetSocket):
            SCHEMES = ('test-autoregister',)
            def __init__(self, proxy_url):
                self.proxy_url = proxy_url
        try:
            assert KafkaNetSocket._registry['test-autoregister'] is _TestProxy
            sock = KafkaNetSocket('test-autoregister://x')
            assert isinstance(sock, _TestProxy)
            assert sock.proxy_url == 'test-autoregister://x'  # pylint: disable=no-member
        finally:
            KafkaNetSocket._registry.pop('test-autoregister', None)

    def test_duplicate_scheme_last_wins(self):
        prior = KafkaNetSocket._registry.get('test-dup')
        class _First(KafkaNetSocket):
            SCHEMES = ('test-dup',)
        class _Second(KafkaNetSocket):
            SCHEMES = ('test-dup',)
        try:
            assert KafkaNetSocket._registry['test-dup'] is _Second
        finally:
            if prior is None:
                KafkaNetSocket._registry.pop('test-dup', None)
            else:
                KafkaNetSocket._registry['test-dup'] = prior


class TestKafkaNetSocketExtensionPattern:
    """Validates the contract a third-party scheme handler must implement.

    Two flavours: a SOCKS-style class that overrides only connect_ex (and
    inherits the default socket()/sock_connect() machinery), and an
    asyncio-style class that overrides connect() entirely (e.g. to delegate
    to an existing asyncio socket implementation).
    """

    def test_connect_ex_only_subclass(self, net):
        """An HTTP CONNECT-style handler that only overrides connect_ex."""
        ex_calls = []

        class _HttpConnect(KafkaNetSocket):
            SCHEMES = ('test-httpconnect',)
            def __init__(self, proxy_url):
                self.proxy_url = proxy_url
            def connect_ex(self, sock, sockaddr):
                ex_calls.append((sock, sockaddr))
                return 0

        try:
            fake_addr = (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('10.0.0.1', 9092))
            mock_sock = MagicMock()
            with patch('kafka.net.backend.inet.KafkaNetSocket.dns_lookup', return_value=[fake_addr]), \
                 patch('kafka.net.backend.inet.socket.socket', return_value=mock_sock):
                result = net.run(
                    create_connection(net, 'broker', 9092,
                                      proxy_url='test-httpconnect://proxy:8080'))
            assert result is mock_sock
            assert ex_calls == [(mock_sock, ('10.0.0.1', 9092))]
            mock_sock.setblocking.assert_called_with(False)
        finally:
            KafkaNetSocket._registry.pop('test-httpconnect', None)

    def test_connect_override_subclass(self, net):
        """An asyncio-style handler that overrides connect() entirely; the
        default socket()/sock_connect()/connect_ex() flow is bypassed."""
        connect_calls = []

        class _AsyncIoSock(KafkaNetSocket):
            SCHEMES = ('test-asyncio',)
            def __init__(self, proxy_url):
                self.proxy_url = proxy_url
            async def connect(self, net, addrinfo, socket_options=(), timeout_at=None):
                connect_calls.append((addrinfo, tuple(socket_options), timeout_at))
                return 'asyncio-stream-handle'

        try:
            fake_addr = (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('10.0.0.1', 9092))
            with patch('kafka.net.backend.inet.KafkaNetSocket.dns_lookup', return_value=[fake_addr]), \
                 patch('kafka.net.backend.inet.socket.socket') as mock_sock_cls:
                result = net.run(
                    create_connection(net, 'broker', 9092,
                                      proxy_url='test-asyncio://x',
                                      timeout_at=123))
            assert result == 'asyncio-stream-handle'
            assert connect_calls == [(fake_addr, (), 123)]
            mock_sock_cls.assert_not_called()
        finally:
            KafkaNetSocket._registry.pop('test-asyncio', None)
