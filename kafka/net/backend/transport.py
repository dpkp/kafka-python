from collections import deque
import copy
import enum
import logging
import selectors
import socket
import ssl
import time

import kafka.errors as Errors


log = logging.getLogger(__name__)


class KafkaTCPTransport:
    def __init__(self, net, sock, host=None):
        self._net = net
        self._sock = sock
        self.host = host
        self._closed = False
        self._write_buffer = deque()
        self._writing = False
        self._read_task = None
        self._write_task = None
        self._protocol = None
        self._read = False
        self._write = True
        self.last_write = time.monotonic()
        self.last_read = time.monotonic()

    @property
    def last_activity(self):
        return max(self.last_write, self.last_read)

    def is_closing(self):
        """Return True if the transport is closing or closed."""
        return self._closed

    def close(self):
        """Close the transport.

        Buffered data will be flushed asynchronously.  No more data
        will be received.  After all buffered data is flushed, the
        protocol's connection_lost() method will (eventually) be
        called with None as its argument.
        """
        if not self._closed:
            log.info('%s: Closing transport', self)
            self._closed = True
            self._read = False
            if not self._write_buffer:
                self._close()

    def set_protocol(self, protocol):
        """Set a new protocol."""
        self._protocol = protocol
        log.debug('%s: Set protocol %s', self, protocol)

    def get_protocol(self):
        """Return the current protocol."""
        return self._protocol

    """Interface for read-only transports."""

    def is_reading(self):
        """Return True if the transport is receiving."""
        return self._read

    def pause_reading(self):
        """Pause the receiving end.

        No data will be passed to the protocol's data_received()
        method until resume_reading() is called.
        """
        self._read = False
        log.debug('%s: Paused reading', self)

    def resume_reading(self):
        """Resume the receiving end.

        Data received will once again be passed to the protocol's
        data_received() method.
        """
        if not self._read:
            self._read_task = self._net.call_soon(self._read_from_sock)
        self._read = True
        log.debug('%s: Resumed reading', self)

    async def _read_from_sock(self):
        while self._read and not self._closed:
            await self._net.wait_read(self._sock)
            recvd_data, err = self._sock_recv()
            if err:
                return self.abort(error=err)
            log.debug('%s: received %d bytes', self, len(recvd_data))
            self.last_read = time.monotonic()
            try:
                self._protocol.data_received(recvd_data)
            except Errors.KafkaProtocolError as e:
                return self.abort(error=e)

    def _sock_recv(self):
        recvd = []
        err = None
        while True:
            try:
                data = self._sock.recv(4096)
                # We expect socket.recv to raise an exception if there are no
                # bytes available to read from the socket in non-blocking mode.
                # but if the socket is disconnected, we will get empty data
                # without an exception raised
                if not data:
                    log.error('%s: socket disconnected', self)
                    err = Errors.KafkaConnectionError('socket disconnected')
                    break
                else:
                    recvd.append(data)

            except (BlockingIOError, InterruptedError):
                break
            except BaseException as e:
                log.exception('%s: Error receiving network data'
                              ' closing socket', self)
                err = Errors.KafkaConnectionError(e)
                break

        recvd_data = b''.join(recvd)
        return recvd_data, err

    def write(self, data):
        """Write some data bytes to the transport.

        This does not block; it buffers the data and arranges for it
        to be sent out asynchronously.
        """
        if not self._write or self._closed:
            raise RuntimeError('Transport closed for writes')
        if not data:
            raise ValueError('Cant write empty data')
        self._write_buffer.append(data)
        if not self._writing:
            self._writing = True
            self._write_task =  self._net.call_soon(self._write_to_sock)
        return len(data)

    async def _write_to_sock(self):
        try:
            while self._write_buffer:
                await self._net.wait_write(self._sock)
                total_bytes, err = self._sock_send()
                if err:
                    return self.abort(error=err)
                log.debug('%s: sent %d bytes', self, total_bytes)
                self.last_write = time.monotonic()
        finally:
            self._writing = False
        if self._closed:
            self._close()
        elif not self._write:
            try:
                self._sock.shutdown(socket.SHUT_WR)
            except OSError:
                pass

    def _sock_send(self):
        total_bytes = 0
        if self._sock is None:
            return total_bytes, Errors.KafkaConnectionError('Connection closed during send')
        while self._write_buffer:
            next_chunk = self._write_buffer.popleft()
            # Wrap in memoryview so partial-send slicing is O(1) instead of
            # copying the unsent tail on every BlockingIOError / short write.
            if not isinstance(next_chunk, memoryview):
                next_chunk = memoryview(next_chunk)
            while next_chunk:
                try:
                    sent_bytes = self._sock.send(next_chunk)
                    total_bytes += sent_bytes
                    next_chunk = next_chunk[sent_bytes:]
                except (BlockingIOError, InterruptedError):
                    self._write_buffer.appendleft(next_chunk)
                    return total_bytes, None
                except BaseException as e:
                    log.exception("%s: Error sending request data: %s", self, e)
                    return total_bytes, Errors.KafkaConnectionError(e)
        return total_bytes, None

    def abort(self, error=None):
        """Close the transport immediately.

        Buffered data will be lost.  No more data will be received.
        The protocol's connection_lost() method will (eventually) be
        called with None as its argument.
        """
        if not self._closed:
            log.error('%s: Abort (%s)', self, error)
            self._closed = True
            self._write_buffer.clear()
            self._read = self._write = False
            self._close(error)

    def _close(self, error=None):
        # idempotent; no lock
        sock = self._sock
        self._sock = None
        if sock is not None:
            try:
                self._net.unregister_event(sock, selectors.EVENT_READ | selectors.EVENT_WRITE)
            except (KeyError, ValueError):
                pass
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()
        for task in (self._read_task, self._write_task):
            if task is not None:
                self._net.cancel(task)
        self._read_task = self._write_task = None
        proto = self._protocol
        self._protocol = None
        if proto is not None:
            proto.connection_lost(error)

    def get_peer(self):
        """Get the remote address of this connection.

        Treat this method with caution. It is the unfortunate result of the CGI and Jabber standards,
        but should not be considered reliable for the usual host of reasons;
        port forwarding, proxying, firewalls, IP masquerading, etc.

        Returns IPv4Address or IPv6Address.
        """
        return self._sock.getpeername()

    async def handshake(self):
        log.info('%s: connected to %s', self, self._sock)

    def host_port(self):
        if self._sock is None:
            return 'none'
        try:
            host, port = self._sock.getpeername()[0:2]
        except (OSError, ValueError):
            return 'none'
        try:
            local_port = self._sock.getsockname()[1]
        except (OSError, ValueError):
            return f'{host}:{port}'
        return f'{host}:{port}<-{local_port}'

    def __str__(self):
        state = ' (closed)' if self._closed else ''
        return f"<{self.__class__.__name__} [{self.host_port()}]{state}>"


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
        data, err = self._ssl_recv()
        if err:
            self.close(err)
        else:
            self._process_outgoing()
            self._protocol.data_received(data)

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
