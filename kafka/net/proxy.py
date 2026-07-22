import logging
import socket
import time
from urllib.parse import urlparse

import kafka.errors as Errors
from kafka.net.ssl import KafkaSSLTransport

log = logging.getLogger(__name__)


class KafkaTCPProxyStates:
    DISCONNECTED = '<disconnected>'
    CONNECTING = '<connecting>'
    NEGOTIATE_PROPOSE = '<negotiate_propose>'
    NEGOTIATING = '<negotiating>'
    AUTHENTICATING = '<authenticating>'
    REQUEST_SUBMIT = '<request_submit>'
    REQUESTING = '<requesting>'
    READ_ADDRESS = '<read_address>'
    COMPLETE = '<complete>'


class KafkaTCPProxy:
    # scheme => handling class
    _registry = {}
    SCHEMES = ()

    @classmethod
    def register_class(cls, klass):
        for scheme in klass.SCHEMES:
            cls._registry[scheme] = klass

    def __init_subclass__(cls, **kw):
        super().__init_subclass__(**kw)
        KafkaTCPProxy.register_class(cls)

    def __new__(cls, net, proxy_url):
        if proxy_url is None:
            return super().__new__(cls)
        try:
            parsed = urlparse(proxy_url)
        except Exception:
            raise ValueError('Unable to parse proxy_url: %s' % (proxy_url,))
        if not parsed.scheme:
            raise ValueError('proxy_url requires scheme:// (%s)' % (proxy_url,))
        try:
            klass = KafkaTCPProxy._registry[parsed.scheme]
        except KeyError:
            raise ValueError('Unsupported proxy url scheme: %s' % (parsed.scheme))
        return super().__new__(klass)

    def __init__(self, net, proxy_url):
        self._net = net
        self._proxy_url = urlparse(proxy_url)
        if self.proxy_scheme not in self.SCHEMES:
            raise ValueError('Unsupported proxy scheme: %s' % (self.proxy_scheme,))
        self._transport = None
        self._connect_future = None
        self._buf = b''
        self._addrinfo = None
        self._state = KafkaTCPProxyStates.DISCONNECTED
        self._timeout_at = None

    @property
    def proxy_scheme(self):
        return self._proxy_url.scheme

    @property
    def proxy_host(self):
        return self._proxy_url.hostname

    @property
    def proxy_port(self):
        return self._proxy_url.port

    def set_addrinfo(self, addrinfo):
        self._addrinfo = addrinfo

    @property
    def _afi(self):
        return self._addrinfo[0] if self._addrinfo is not None else None

    @property
    def _host(self):
        return self._addrinfo[4][0] if self._addrinfo is not None else None

    @property
    def _port(self):
        return self._addrinfo[4][1] if self._addrinfo is not None else None

    async def create_connection(self, protocol, host, port, *, ssl=None,
                                socket_options=(), timeout_at=None):
        if self.use_remote_lookup():
            self.set_addrinfo((socket.AF_UNSPEC, socket.SOCK_STREAM, socket.IPPROTO_TCP, '', (host, port)))
            await self._net.create_connection(self, self.proxy_host, self.proxy_port,
                                              ssl=None, # TODO: support TLS connections to proxy
                                              socket_options=socket_options,
                                              timeout_at=timeout_at)
            await self.do_connect(timeout_at=timeout_at)
        else:
            for addrinfo in await self._net.getaddrinfo(host, port):
                self.set_addrinfo(addrinfo)
                await self._net.create_connection(self, self.proxy_host, self.proxy_port,
                                                  ssl=None, # TODO: support TLS connections to proxy
                                                  socket_options=socket_options,
                                                  timeout_at=timeout_at)
                try:
                    await self.do_connect(timeout_at=timeout_at)
                    break
                except Exception as exc:
                    log.debug('Failed to connect to %s: %s', addrinfo, exc)
                    self._transport.abort(exc)
                    self.connection_lost(exc)
            else:
                raise Errors.KafkaConnectionError('Unable to connect to %s:%d via proxy' % (host, port))

        if ssl:
            ssl_wrapper = KafkaSSLTransport(self._net, ssl, host=host)
            ssl_wrapper.connection_made(self._transport)
            await ssl_wrapper.handshake()
            self._transport = ssl_wrapper

        try:
            protocol.connection_made(self._transport)
        except Exception as e:
            self._transport.abort(e)
            raise
        self._transport = None

        # TODO: what about transport.get_peer() / host_port() ?

    def use_remote_lookup(self):
        return True

    def connection_lost(self, exc):
        self._state = KafkaTCPProxyStates.DISCONNECTED
        self._transport = None
        if self._connect_future is not None and not self._connect_future.is_done:
            self._connect_future.failure(exc)

    def connection_made(self, transport):
        self._transport = transport
        self._transport.set_protocol(self)
        self._transport.resume_reading()
        self._buf = b''
        self._state = KafkaTCPProxyStates.CONNECTING
        self._connect_future = self._net.create_future()
        self._wrapped_state_machine()

    def data_received(self, data):
        """Runs a state machine through connection to authentication to
        proxy connection request."""
        self._buf += data
        self._wrapped_state_machine()

    def _wrapped_state_machine(self):
        try:
            if self._timeout_at is not None and self._timeout_at <= time.monotonic():
                raise Errors.KafkaTimeoutError('Proxy connection timeout')
            if self._run_state_machine():
                self._connect_future.success(True)
        except Exception as exc:
            self._state = KafkaTCPProxyStates.DISCONNECTED
            self._connect_future.failure(exc)

    async def do_connect(self, timeout_at=None):
        self._timeout_at = timeout_at
        self._wrapped_state_machine()
        await self._connect_future

    def _run_state_machine(self):
        raise NotImplementedError()
