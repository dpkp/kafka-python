import socket
import struct

from unittest.mock import MagicMock

from kafka.net.proxy import KafkaTCPProxy, KafkaTCPProxyStates
from kafka.net.socks5 import Socks5ProxyProtocol


# SOCKS5 reply granting a CONNECT to an IPv4 endpoint:
# VER=05 REP=00 RSV=00 ATYP=01(ipv4) + 4 addr bytes + 2 port bytes.
_GRANT_IPV4 = b'\x05\x00\x00\x01' + b'\x7f\x00\x00\x01' + struct.pack('!H', 0)


def _connect(net, url, target):
    """Build a proxy, wire a recording transport, and run through connection_made.

    Returns (proxy, sent) where ``sent`` accumulates every ``transport.write``.
    Mirrors what manager._connect does: set the target addrinfo before the
    transport's connection_made drives the first state-machine step.
    """
    proxy = KafkaTCPProxy(net, url)
    proxy.set_addrinfo(target)
    sent = []
    transport = MagicMock()
    transport.write.side_effect = lambda b: sent.append(bytes(b)) or len(b)
    proxy.connection_made(transport)
    return proxy, sent


class TestSocks5Registry:
    def test_dispatch(self, net):
        assert isinstance(KafkaTCPProxy(net, 'socks5://p:1080'), Socks5ProxyProtocol)
        assert isinstance(KafkaTCPProxy(net, 'socks5h://p:1080'), Socks5ProxyProtocol)

    def test_remote_lookup_only_for_socks5h(self, net):
        assert KafkaTCPProxy(net, 'socks5://p:1080').use_remote_lookup() is False
        assert KafkaTCPProxy(net, 'socks5h://p:1080').use_remote_lookup() is True


class TestSocks5NoAuth:
    TARGET = (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, '', ('1.2.3.4', 9092))
    REQUEST = b'\x05\x01\x00\x01' + socket.inet_pton(socket.AF_INET, '1.2.3.4') + struct.pack('!H', 9092)

    def test_proposes_no_auth(self, net):
        proxy, sent = _connect(net, 'socks5://proxy:1080', self.TARGET)
        assert b''.join(sent) == b'\x05\x01\x00'
        assert proxy._state == KafkaTCPProxyStates.NEGOTIATING
        assert not proxy._connect_future.is_done

    def test_happy_path(self, net):
        proxy, sent = _connect(net, 'socks5://proxy:1080', self.TARGET)
        # server selects no-auth -> we submit the CONNECT request
        proxy.data_received(b'\x05\x00')
        assert proxy._state == KafkaTCPProxyStates.REQUESTING
        assert b''.join(sent) == b'\x05\x01\x00' + self.REQUEST
        # server grants
        proxy.data_received(_GRANT_IPV4)
        assert proxy._state == KafkaTCPProxyStates.COMPLETE
        assert proxy._connect_future.succeeded()

    def test_request_rejected(self, net):
        proxy, _ = _connect(net, 'socks5://proxy:1080', self.TARGET)
        proxy.data_received(b'\x05\x00')
        proxy.data_received(b'\x05\x01')  # REP=01 -> general failure
        assert proxy._connect_future.failed()
        assert proxy._state == KafkaTCPProxyStates.DISCONNECTED

    def test_bad_socks_version(self, net):
        proxy, _ = _connect(net, 'socks5://proxy:1080', self.TARGET)
        proxy.data_received(b'\x04\x00')
        assert proxy._connect_future.failed()
        assert proxy._state == KafkaTCPProxyStates.DISCONNECTED

    def test_unrecognized_auth_method(self, net):
        proxy, _ = _connect(net, 'socks5://proxy:1080', self.TARGET)
        proxy.data_received(b'\x05\xff')
        assert proxy._connect_future.failed()
        assert proxy._state == KafkaTCPProxyStates.DISCONNECTED


class TestSocks5UserPass:
    TARGET = (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, '', ('1.2.3.4', 9092))
    # subnegotiation: VER=01 ULEN=04 'user' PLEN=04 'pass'
    AUTH = struct.pack('!bb4sb4s', 1, 4, b'user', 4, b'pass')

    def test_proposes_user_pass(self, net):
        proxy, sent = _connect(net, 'socks5://user:pass@proxy:1080', self.TARGET)
        assert b''.join(sent) == b'\x05\x01\x02'
        assert proxy._state == KafkaTCPProxyStates.NEGOTIATING

    def test_auth_success(self, net):
        proxy, sent = _connect(net, 'socks5://user:pass@proxy:1080', self.TARGET)
        # server selects user/pass -> we send credentials
        proxy.data_received(b'\x05\x02')
        assert proxy._state == KafkaTCPProxyStates.AUTHENTICATING
        assert b''.join(sent) == b'\x05\x01\x02' + self.AUTH
        # server accepts credentials -> CONNECT request submitted
        proxy.data_received(b'\x01\x00')
        assert proxy._state == KafkaTCPProxyStates.REQUESTING
        proxy.data_received(_GRANT_IPV4)
        assert proxy._connect_future.succeeded()

    def test_auth_failure(self, net):
        proxy, _ = _connect(net, 'socks5://user:pass@proxy:1080', self.TARGET)
        proxy.data_received(b'\x05\x02')
        proxy.data_received(b'\x01\x01')  # non-zero status -> auth failed
        assert proxy._connect_future.failed()
        assert proxy._state == KafkaTCPProxyStates.DISCONNECTED


class TestSocks5AddressEncoding:
    def _request_after_select(self, net, url, target):
        proxy, sent = _connect(net, url, target)
        proxy.data_received(b'\x05\x00')  # no-auth selected -> REQUEST_SUBMIT
        # sent[0] is the proposal; the remainder is the CONNECT request
        return b''.join(sent[1:])

    def test_ipv4(self, net):
        target = (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, '', ('1.2.3.4', 9092))
        req = self._request_after_select(net, 'socks5://proxy:1080', target)
        assert req == b'\x05\x01\x00\x01' + socket.inet_pton(socket.AF_INET, '1.2.3.4') + struct.pack('!H', 9092)

    def test_ipv6(self, net):
        target = (socket.AF_INET6, socket.SOCK_STREAM, socket.IPPROTO_TCP, '', ('::1', 9092))
        req = self._request_after_select(net, 'socks5://proxy:1080', target)
        assert req == b'\x05\x01\x00\x04' + socket.inet_pton(socket.AF_INET6, '::1') + struct.pack('!H', 9092)

    def test_remote_lookup_domain(self, net):
        # socks5h forwards the hostname unresolved (ATYP=3, domain name).
        target = (socket.AF_UNSPEC, socket.SOCK_STREAM, socket.IPPROTO_TCP, '', ('example.com', 9092))
        req = self._request_after_select(net, 'socks5h://proxy:1080', target)
        expected = b'\x05\x01\x00\x03' + bytes([len('example.com')]) + b'example.com' + struct.pack('!H', 9092)
        assert req == expected


class TestSocks5Fragmented:
    TARGET = (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, '', ('1.2.3.4', 9092))

    def test_byte_by_byte_delivery(self, net):
        # The sans-IO invariant: the state machine must buffer across arbitrarily
        # fragmented data_received calls, not assume a whole message per read.
        proxy, _ = _connect(net, 'socks5://proxy:1080', self.TARGET)
        stream = b'\x05\x00' + _GRANT_IPV4  # method-select reply, then CONNECT grant
        for byte in stream:
            assert not proxy._connect_future.is_done
            proxy.data_received(bytes([byte]))
        assert proxy._state == KafkaTCPProxyStates.COMPLETE
        assert proxy._connect_future.succeeded()
