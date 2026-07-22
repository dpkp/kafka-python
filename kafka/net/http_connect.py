import base64
import logging

from kafka.errors import KafkaConnectionError
from kafka.net.proxy import KafkaTCPProxy, KafkaTCPProxyStates


log = logging.getLogger(__name__)


class HttpConnectProxyProtocol(KafkaTCPProxy):
    """Tunnels broker connections through an HTTP CONNECT proxy (RFC 7231 s4.3.6).

    Registered for the ``http`` scheme -- pass ``proxy_url='http://host:port'``
    to KafkaConsumer/KafkaProducer/KafkaAdminClient.

    Basic proxy auth is supported via URL credentials: ``http://user:pass@host:8080``.
    Broker hostnames are always forwarded unresolved so the proxy handles DNS.
    """

    SCHEMES = ('http',)

    def _run_state_machine(self):
        if self._state in (KafkaTCPProxyStates.DISCONNECTED, KafkaTCPProxyStates.COMPLETE):
            return False

        if self._state == KafkaTCPProxyStates.CONNECTING:
            self._do_connecting()

        if self._state == KafkaTCPProxyStates.REQUESTING:
            self._do_requesting()

        if self._state == KafkaTCPProxyStates.COMPLETE:
            return True
        else:
            return False

    def _do_connecting(self):
        headers = 'CONNECT {0}:{1} HTTP/1.1\r\nHost: {0}:{1}\r\n'.format(self._host, self._port)
        if self._proxy_url.username and self._proxy_url.password:
            credentials = base64.b64encode(
                '{0}:{1}'.format(self._proxy_url.username, self._proxy_url.password).encode()
            ).decode()
            headers += 'Proxy-Authorization: Basic {}\r\n'.format(credentials)
        self._transport.write((headers + '\r\n').encode())
        self._state = KafkaTCPProxyStates.REQUESTING

    def _do_requesting(self):
        if b'\r\n\r\n' in self._buf:
            first_line = self._buf.split(b'\r\n')[0]
            if b' 200 ' in first_line or first_line.endswith(b' 200'):
                self._state = KafkaTCPProxyStates.COMPLETE
            else:
                log.error('HTTP CONNECT to proxy failed: %r', first_line)
                self._state = KafkaTCPProxyStates.DISCONNECTED
                raise KafkaConnectionError('HTTP CONNECT to proxy failed: %r' % (first_line,))
