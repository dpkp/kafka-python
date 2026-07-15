import errno
import socket
from unittest.mock import MagicMock, patch

import pytest

from kafka.net.http_connect import HttpConnectProxy
from kafka.net.backend.inet import KafkaNetSocket


_FAKE_PROXY_ADDR = (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, '', ('1.2.3.4', 8080))


def _make_proxy(url='http://proxy:8080'):
    with patch.object(HttpConnectProxy, 'dns_lookup', return_value=[_FAKE_PROXY_ADDR]):
        proxy = HttpConnectProxy(url)
    proxy._sock = MagicMock()
    return proxy


class TestHttpConnectProxyRegistry:
    def test_registered_for_http_scheme(self):
        with patch.object(HttpConnectProxy, 'dns_lookup', return_value=[_FAKE_PROXY_ADDR]):
            obj = KafkaNetSocket('http://proxy:8080')
        assert isinstance(obj, HttpConnectProxy)

    def test_unregistered_scheme_raises(self):
        with pytest.raises(ValueError, match='Unsupported proxy url scheme'):
            KafkaNetSocket('socks4://proxy:8080')


class TestHttpConnectProxyDnsLookup:
    def test_broker_lookup_returns_unresolved(self):
        proxy = _make_proxy()
        result = proxy.dns_lookup('broker.kafka.internal', 9092)
        assert result == [(socket.AF_UNSPEC, socket.SOCK_STREAM, socket.IPPROTO_TCP, '', ('broker.kafka.internal', 9092))]

    def test_proxy_lookup_delegates_to_super(self):
        with patch('socket.getaddrinfo', return_value=[_FAKE_PROXY_ADDR]) as mock_gai:
            proxy = _make_proxy()
            proxy.dns_lookup('proxy', 8080, proxy=True)
        mock_gai.assert_called()


class TestHttpConnectProxySocket:
    def test_uses_proxy_family_not_broker_family(self):
        proxy = _make_proxy()
        with patch('socket.socket') as mock_ctor:
            proxy.socket(socket.AF_INET6, socket.SOCK_STREAM, socket.IPPROTO_TCP)
        mock_ctor.assert_called_once_with(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP)


class TestHttpConnectProxyConnectEx:
    def test_success(self):
        proxy = _make_proxy()
        proxy._sock.connect_ex.return_value = 0
        proxy._sock.send.side_effect = lambda b: len(b)
        proxy._sock.recv.return_value = b'HTTP/1.1 200 Connection Established\r\n\r\n'
        assert proxy.connect_ex(proxy._sock, ('broker', 9092)) == 0

    def test_success_no_reason_phrase(self):
        proxy = _make_proxy()
        proxy._sock.connect_ex.return_value = 0
        proxy._sock.send.side_effect = lambda b: len(b)
        proxy._sock.recv.return_value = b'HTTP/1.1 200\r\n\r\n'
        assert proxy.connect_ex(proxy._sock, ('broker', 9092)) == 0

    def test_basic_auth_header_sent_when_credentials_in_url(self):
        import base64
        proxy = _make_proxy('http://user:pass@proxy:8080')
        proxy._sock.connect_ex.return_value = 0
        sent = []
        proxy._sock.send.side_effect = lambda b: sent.append(b) or len(b)
        proxy._sock.recv.return_value = b'HTTP/1.1 200 Connection Established\r\n\r\n'
        proxy.connect_ex(proxy._sock, ('broker', 9092))
        request = b''.join(sent).decode()
        expected = base64.b64encode(b'user:pass').decode()
        assert 'Proxy-Authorization: Basic {}'.format(expected) in request

    def test_non_200_response_returns_econnrefused(self):
        proxy = _make_proxy()
        proxy._sock.connect_ex.return_value = 0
        proxy._sock.send.side_effect = lambda b: len(b)
        proxy._sock.recv.return_value = b'HTTP/1.1 407 Proxy Authentication Required\r\n\r\n'
        assert proxy.connect_ex(proxy._sock, ('broker', 9092)) == errno.ECONNREFUSED

    def test_ewouldblock_while_sending(self):
        proxy = _make_proxy()
        proxy._sock.connect_ex.return_value = 0
        proxy._sock.send.side_effect = OSError(errno.EWOULDBLOCK, 'would block')
        assert proxy.connect_ex(proxy._sock, ('broker', 9092)) == errno.EWOULDBLOCK

    def test_ewouldblock_while_reading(self):
        proxy = _make_proxy()
        proxy._sock.connect_ex.return_value = 0
        proxy._sock.send.side_effect = lambda b: len(b)
        proxy._sock.recv.side_effect = OSError(errno.EWOULDBLOCK, 'would block')
        assert proxy.connect_ex(proxy._sock, ('broker', 9092)) == errno.EWOULDBLOCK

    def test_resumes_after_ewouldblock(self):
        proxy = _make_proxy()
        proxy._sock.connect_ex.return_value = 0
        proxy._sock.send.side_effect = lambda b: len(b)
        proxy._sock.recv.side_effect = [
            OSError(errno.EWOULDBLOCK, 'would block'),
            b'HTTP/1.1 200 Connection Established\r\n\r\n',
        ]
        assert proxy.connect_ex(proxy._sock, ('broker', 9092)) == errno.EWOULDBLOCK
        assert proxy.connect_ex(proxy._sock, ('broker', 9092)) == 0

    def test_eof_during_handshake_returns_econnrefused(self):
        proxy = _make_proxy()
        proxy._sock.connect_ex.return_value = 0
        proxy._sock.send.side_effect = lambda b: len(b)
        proxy._sock.recv.return_value = b''
        assert proxy.connect_ex(proxy._sock, ('broker', 9092)) == errno.ECONNREFUSED
