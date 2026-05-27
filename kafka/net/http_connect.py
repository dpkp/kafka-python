import base64
import errno
import logging
import random
import socket
from urllib.parse import urlparse

from kafka.errors import KafkaConnectionError
from kafka.net.inet import KafkaNetSocket


log = logging.getLogger(__name__)

_WOULD_BLOCK = {errno.EWOULDBLOCK, errno.EAGAIN}
_MAX_RESPONSE_SIZE = 65536


class _States:
    DISCONNECTED = '<disconnected>'
    CONNECTING = '<connecting>'
    SENDING = '<sending>'
    READING = '<reading>'
    COMPLETE = '<complete>'


class HttpConnectProxy(KafkaNetSocket):
    """Tunnels broker connections through an HTTP CONNECT proxy (RFC 7231 s4.3.6).

    Registered for the ``http`` scheme -- pass ``proxy_url='http://host:port'``
    to KafkaConsumer/KafkaProducer/KafkaAdminClient.

    Basic proxy auth is supported via URL credentials: ``http://user:pass@host:8080``.
    Broker hostnames are always forwarded unresolved so the proxy handles DNS.
    """

    SCHEMES = ('http',)

    def __init__(self, proxy_url):
        self._proxy_url = urlparse(proxy_url)
        self._sock = None
        self._state = _States.DISCONNECTED
        self._send_buf = b''
        self._recv_buf = b''
        self._proxy_addr = self._get_proxy_addr()

    def _get_proxy_addr(self):
        addrs = self.dns_lookup(self._proxy_url.hostname, self._proxy_url.port, proxy=True)
        if not addrs:
            raise KafkaConnectionError('Unable to resolve proxy_url via dns')
        return random.choice(addrs)

    def dns_lookup(self, host, port, proxy=False):
        if proxy:
            return super().dns_lookup(host, port, raise_error=True)
        # Always forward broker hostname unresolved; the proxy handles DNS
        return [(socket.AF_UNSPEC, socket.SOCK_STREAM, socket.IPPROTO_TCP, '', (host, port))]

    def socket(self, family=socket.AF_UNSPEC, sock_type=socket.SOCK_STREAM, proto=socket.IPPROTO_TCP):
        self._target_afi = family
        proxy_family, _, _, _, _ = self._proxy_addr
        self._sock = socket.socket(proxy_family, sock_type, proto)
        return self._sock

    def connect_ex(self, sock, addr):
        assert sock is self._sock

        if self._state == _States.DISCONNECTED:
            self._state = _States.CONNECTING

        if self._state == _States.CONNECTING:
            ret = self._do_connecting(addr)
            if ret is not None:
                return ret

        if self._state == _States.SENDING:
            ret = self._do_sending()
            if ret is not None:
                return ret

        if self._state == _States.READING:
            ret = self._do_reading()
            if ret is not None:
                return ret

        if self._state == _States.COMPLETE:
            return 0

        return errno.ECONNREFUSED

    def _do_connecting(self, addr):
        _, _, _, _, proxy_sockaddr = self._proxy_addr
        ret = self._sock.connect_ex(proxy_sockaddr)
        if ret and ret != errno.EISCONN:
            return ret
        host, port = addr[0], addr[1]
        headers = 'CONNECT {0}:{1} HTTP/1.1\r\nHost: {0}:{1}\r\n'.format(host, port)
        if self._proxy_url.username and self._proxy_url.password:
            credentials = base64.b64encode(
                '{0}:{1}'.format(self._proxy_url.username, self._proxy_url.password).encode()
            ).decode()
            headers += 'Proxy-Authorization: Basic {}\r\n'.format(credentials)
        self._send_buf = (headers + '\r\n').encode()
        self._state = _States.SENDING
        return None

    def _do_sending(self):
        while self._send_buf:
            try:
                sent = self._sock.send(self._send_buf)
                if sent == 0:
                    log.error('Proxy closed connection while sending CONNECT request')
                    return errno.ECONNREFUSED
                self._send_buf = self._send_buf[sent:]
            except OSError as exc:
                if exc.errno in _WOULD_BLOCK:
                    return errno.EWOULDBLOCK
                raise
        self._state = _States.READING
        return None

    def _do_reading(self):
        while b'\r\n\r\n' not in self._recv_buf:
            try:
                chunk = self._sock.recv(4096)
                if not chunk:
                    log.error('Proxy closed connection during CONNECT handshake')
                    self._sock.close()
                    return errno.ECONNREFUSED
                self._recv_buf += chunk
                if len(self._recv_buf) > _MAX_RESPONSE_SIZE:
                    log.error('Proxy response exceeded %d bytes without end-of-headers', _MAX_RESPONSE_SIZE)
                    self._sock.close()
                    return errno.ECONNREFUSED
            except OSError as exc:
                if exc.errno in _WOULD_BLOCK:
                    return errno.EWOULDBLOCK
                raise
        first_line = self._recv_buf.split(b'\r\n')[0]
        if b' 200 ' in first_line or first_line.endswith(b' 200'):
            self._state = _States.COMPLETE
            return None
        log.error('HTTP CONNECT to proxy failed: %r', first_line)
        self._sock.close()
        return errno.ECONNREFUSED
