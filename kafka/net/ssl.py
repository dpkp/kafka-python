from collections import deque
import copy
import enum
import logging
import ssl

import kafka.errors as Errors

log = logging.getLogger(__name__)


class ConnectionState(enum.Enum):
    HANDSHAKE    = 'handshake'
    CONNECTED    = 'connected'
    CLOSED       = 'closed'


class KafkaSSLTransport:
    DEFAULT_CONFIG = {
        'ssl_context': None,
        'ssl_check_hostname': True,
        'ssl_cafile': None,
        'ssl_certfile': None,
        'ssl_keyfile': None,
        'ssl_password': None,
        'ssl_crlfile': None,
    }
    def __init__(self, net, ssl_context, host=None):
        self._net = net
        self._state = None
        self._connect_future = self._net.create_future()
        self._ssl_context = ssl_context
        self.host = host
        server_hostname = host.rstrip('.') if host is not None else None
        self._incoming = ssl.MemoryBIO()
        self._outgoing = ssl.MemoryBIO()
        self._ssl_object = self._ssl_context.wrap_bio(
            self._incoming, self._outgoing,
            server_hostname=server_hostname)
        self._write_buffer = deque()  # list of bytes that are pending ssl.send()
        # recvs from transport, writes to protocol
        self._transport = None
        self._protocol = None
        self._write = False

    @classmethod
    def build_ssl_context(cls, configs):
        config = copy.copy(cls.DEFAULT_CONFIG)
        for key in config:
            if key in configs:
                config[key] = configs[key]

        if config['ssl_context'] is not None:
            return config['ssl_context']
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ctx.check_hostname = config['ssl_check_hostname']
        if config['ssl_cafile']:
            ctx.load_verify_locations(config['ssl_cafile'])
        else:
            ctx.load_default_certs()
        if config['ssl_certfile']:
            ctx.load_cert_chain(
                certfile=config['ssl_certfile'],
                keyfile=config['ssl_keyfile'],
                password=config['ssl_password'],
            )
        if config['ssl_crlfile']:
            ctx.load_verify_locations(crl=config['ssl_crlfile'])
            ctx.verify_flags |= ssl.VERIFY_CRL_CHECK_LEAF
        return ctx

    def close(self, err=None):
        self._state = ConnectionState.CLOSED
        self._write = False
        if self._protocol:
            protocol, self._protocol = self._protocol, None
            protocol.connection_lost(err)
        if self._transport:
            transport, self._transport = self._transport, None
            if err:
                transport.abort(err)
            else:
                transport.close()
        if not self._connect_future.is_done:
            self._connect_future.failure(err or Errors.Cancelled())

    def abort(self, error):
        self.close(error)

    def data_received(self, data):
        # from underlying transport (tcp or proxy)
        if self._state not in (ConnectionState.HANDSHAKE, ConnectionState.CONNECTED):
            log.warning('%s: ignoring data_received %d bytes because not connected', self, len(data))
            return
        log.debug('%s: data_received %d bytes', self, len(data))
        self._incoming.write(data)
        if self._state == ConnectionState.HANDSHAKE:
            self._do_handshake()
            return
        self._do_recv()

    def _do_recv(self):
        data, err = self._ssl_recv()
        if err:
            self.close(err)
        else:
            self._process_outgoing()
            self._protocol.data_received(data)
            self._do_send()

    def write(self, data):
        # from outer protocol (connection)
        if self._state not in (ConnectionState.HANDSHAKE, ConnectionState.CONNECTED):
            log.warning('%s: ignoring write %d bytes because not connected', self, len(data))
            return
        log.debug('%s: write %d bytes', self, len(data))
        self._write_buffer.append(data)
        if self._state == ConnectionState.HANDSHAKE:
            self._do_handshake()
            return
        self._do_send()

    def _do_send(self):
        nbytes, err = self._ssl_send()
        if err:
            self.close(err)
        else:
            self._process_outgoing()

    def _process_outgoing(self):
        if not self._write:
            return
        data = self._outgoing.read()
        if len(data):
            self._transport.write(data)

    def set_protocol(self, protocol):
        """Set a new protocol."""
        self._protocol = protocol
        log.debug('%s: Set protocol %s', self, protocol)

    def get_protocol(self):
        """Return the current protocol."""
        return self._protocol

    def _ssl_recv(self):
        recvd = []
        err = None
        while True:
            try:
                data = self._ssl_object.read(4096)
                if not data:
                    log.error('%s: socket disconnected', self)
                    err = Errors.KafkaConnectionError('socket disconnected')
                    break
                else:
                    recvd.append(data)

            except (ssl.SSLWantReadError, ssl.SSLWantWriteError):
                break
            except BaseException as e:
                log.exception('%s: Error receiving ssl data'
                              ' closing transport', self)
                err = Errors.KafkaConnectionError(e)
                break

        recvd_data = b''.join(recvd)
        return recvd_data, err

    def _ssl_send(self):
        total_bytes = 0
        if self._state == ConnectionState.CLOSED:
            return total_bytes, Errors.KafkaConnectionError('Connection closed')
        while self._write_buffer:
            next_chunk = self._write_buffer.popleft()
            # Wrap in memoryview so partial-send slicing is O(1) instead of
            # copying the unsent tail on every BlockingIOError / short write.
            if not isinstance(next_chunk, memoryview):
                next_chunk = memoryview(next_chunk)
            while next_chunk:
                try:
                    sent_bytes = self._ssl_object.write(next_chunk)
                    total_bytes += sent_bytes
                    next_chunk = next_chunk[sent_bytes:]
                except (ssl.SSLWantReadError, ssl.SSLWantWriteError):
                    self._write_buffer.appendleft(next_chunk)
                    self._process_outgoing()
                    return total_bytes, None
                except BaseException as e:
                    log.exception("%s: Error sending request data: %s", self, e)
                    return total_bytes, Errors.KafkaConnectionError(e)
        return total_bytes, None

    def _do_handshake(self):
        log.debug('%s: _do_handshake', self)
        try:
            self._ssl_object.do_handshake()
        except (ssl.SSLWantReadError, ssl.SSLWantWriteError) as e:
            log.debug('%s: %s', self, e)
            self._process_outgoing()
            pass
        except BaseException as exc:
            log.error("%s: Error during TLS Handshake: %s", self, exc)
            self.close(exc)
        else:
            log.info('%s: connected', self)
            self._state = ConnectionState.CONNECTED
            self._connect_future.success(True)
            self._do_send()
            return

    async def handshake(self):
        self._do_handshake()
        await self._connect_future

    @property
    def last_activity(self):
        return self._transport.last_activity

    def is_closing(self):
        return self._state is ConnectionState.CLOSED

    def pause_reading(self):
        return self._transport.pause_reading()

    def resume_reading(self):
        return self._transport.resume_reading()

    def pause_writing(self):
        self._write = False

    def resume_writing(self):
        self._write = True
        self._process_outgoing()

    def host_port(self):
        if self._transport:
            return self._transport.host_port()

    def connection_made(self, transport):
        self._transport = transport
        self._transport.set_protocol(self)
        self._state = ConnectionState.HANDSHAKE
        self._transport.resume_reading()
        self.resume_writing()

    def connection_lost(self, exc):
        self.abort(exc)

    def get_peer(self):
        if self._transport:
            return self._transport.get_peer()

    def __str__(self):
        return f"<{self.__class__.__name__} [{self.host_port()}]>"
