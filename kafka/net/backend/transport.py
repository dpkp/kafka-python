from collections import deque
import enum
import logging
import selectors
import socket
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
