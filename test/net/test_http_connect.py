import errno
import socket
from unittest.mock import MagicMock, patch

import pytest

from kafka.net.http_connect import HttpConnectProxyProtocol
from kafka.net.proxy import KafkaTCPProxy, KafkaTCPProxyStates


_FAKE_PROXY_ADDR = (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, '', ('1.2.3.4', 8080))


class TestHttpConnectProxyRegistry:
    def test_registered_for_http_scheme(self, net):
        obj = KafkaTCPProxy(net, 'http://proxy:8080')
        assert isinstance(obj, HttpConnectProxyProtocol)

    def test_unregistered_scheme_raises(self, net):
        with pytest.raises(ValueError, match='Unsupported proxy url scheme'):
            KafkaTCPProxy(net, 'socks4://proxy:8080')


class TestHttpConnectProxyStateMachine:
    def test_success(self, net):
        proxy = KafkaTCPProxy(net, 'http://proxy:8080')
        proxy.connection_made(MagicMock())
        proxy.data_received(b'HTTP/1.1 200 Connection Established\r\n\r\n')
        assert proxy._connect_future.is_done
        assert proxy._connect_future.succeeded()
        assert proxy._state == KafkaTCPProxyStates.COMPLETE

    def test_success_no_reason_phrase(self, net):
        proxy = KafkaTCPProxy(net, 'http://proxy:8080')
        proxy.connection_made(MagicMock())
        proxy.data_received(b'HTTP/1.1 200\r\n\r\n')
        assert proxy._connect_future.is_done
        assert proxy._connect_future.succeeded()
        assert proxy._state == KafkaTCPProxyStates.COMPLETE

    def test_basic_auth_header_sent_when_credentials_in_url(self, net):
        import base64
        proxy = KafkaTCPProxy(net, 'http://user:pass@proxy:8080')
        transport = MagicMock()
        sent = []
        transport.write.side_effect = lambda b: sent.append(b) or len(b)
        proxy.connection_made(transport)
        request = b''.join(sent).decode()
        expected = base64.b64encode(b'user:pass').decode()
        assert 'Proxy-Authorization: Basic {}'.format(expected) in request
        assert not proxy._connect_future.is_done
        proxy.data_received(b'HTTP/1.1 200 Connection Established\r\n\r\n')
        assert proxy._connect_future.is_done
        assert proxy._connect_future.succeeded()
        assert proxy._state == KafkaTCPProxyStates.COMPLETE

    def test_non_200_response_disconnects(self, net):
        proxy = KafkaTCPProxy(net, 'http://proxy:8080')
        proxy.connection_made(MagicMock())
        proxy.data_received(b'HTTP/1.1 407 Proxy Authentication Required\r\n\r\n')
        assert proxy._connect_future.is_done
        assert proxy._connect_future.failed()
        assert proxy._state == KafkaTCPProxyStates.DISCONNECTED
