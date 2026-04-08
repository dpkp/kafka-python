from collections import deque
import logging
import selectors
import socket
import ssl
import time

import kafka.errors as Errors


log = logging.getLogger(__name__)


class KafkaTCPTransport:
    def __init__(self, net, sock):
        self._net = net
        self._sock = sock
        self._closed = False
        self._write_buffer = deque()
        self._protocol = None
        self._read = False
        self._write = True
        self.last_write = time.monotonic()
        self.last_read = time.monotonic()

    @property
    def last_activity(self):
        return max(self.last_write, self.last_read)

    # AsyncIO
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
            self._net.call_soon(self._read_from_sock)
        self._read = True
        log.debug('%s: Resumed reading', self)

    async def _read_from_sock(self):
        while self._read:
            await self._net.wait_read(self._sock)
            recvd_data, err = self._sock_recv()
            log.debug('%s: received %d bytes', self, len(recvd_data))
            self.last_read = time.monotonic()
            if self._protocol and self._protocol._sensors:
                self._protocol._sensors.bytes_received.record(len(recvd_data))

            try:
                self._protocol.data_received(recvd_data)
            except Errors.KafkaProtocolError as e:
                if err is None:
                    err = e
            if err:
                self.abort(error=err)

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

    """Interface for write-only transports."""

    def set_write_buffer_limits(self, high=None, low=None):
        """Set the high- and low-water limits for write flow control.

        These two values control when to call the protocol's
        pause_writing() and resume_writing() methods.  If specified,
        the low-water limit must be less than or equal to the
        high-water limit.  Neither value can be negative.

        The defaults are implementation-specific.  If only the
        high-water limit is given, the low-water limit defaults to an
        implementation-specific value less than or equal to the
        high-water limit.  Setting high to zero forces low to zero as
        well, and causes pause_writing() to be called whenever the
        buffer becomes non-empty.  Setting low to zero causes
        resume_writing() to be called only once the buffer is empty.
        Use of zero for either limit is generally sub-optimal as it
        reduces opportunities for doing I/O and computation
        concurrently.
        """
        raise NotImplementedError

    def get_write_buffer_size(self):
        """Return the current size of the write buffer."""
        raise NotImplementedError

    def get_write_buffer_limits(self):
        """Get the high and low watermarks for write flow control.
        Return a tuple (low, high) where low and high are
        positive number of bytes."""
        raise NotImplementedError

    def write(self, data):
        """Write some data bytes to the transport.

        This does not block; it buffers the data and arranges for it
        to be sent out asynchronously.
        """
        if not self._write or self._closed:
            raise RuntimeError('Transport closed for writes')
        if not data:
            raise ValueError('Cant write empty data')
        if not self._write_buffer:
            self._net.call_soon(self._write_to_sock)
        self._write_buffer.append(data)

    def writelines(self, list_of_data):
        """Write a list (or any iterable) of data bytes to the transport."""
        if not self._write or self._closed:
            raise RuntimeError('Transport closed for writes')
        if not self._write_buffer:
            self._net.call_soon(self._write_to_sock)
        self._write_buffer.extend(list_of_data)

    async def _write_to_sock(self):
        while self._write and not self._closed and self._write_buffer:
            await self._net.wait_write(self._sock)
            total_bytes, err = self._sock_send()
            log.debug('%s: sent %d bytes', self, total_bytes)
            self.last_write = time.monotonic()
            if self._protocol and self._protocol._sensors:
                self._protocol._sensors.bytes_sent.record(total_bytes)
        if self._closed:
            self._close()
        elif not self._write:
            self._sock.shutdown(socket.SHUT_WR)

    def _sock_send(self):
        total_bytes = 0
        err = None
        while self._write_buffer:
            next_chunk = self._write_buffer.popleft()
            while next_chunk:
                try:
                    sent_bytes = self._sock.send(next_chunk)
                    total_bytes += sent_bytes
                    next_chunk = next_chunk[sent_bytes:]
                except (BlockingIOError, InterruptedError):
                    self._write_buffer.appendleft(next_chunk)
                    return total_bytes, err
                except BaseException as e:
                    log.exception("%s: Error sending request data: %s", self, e)
                    err = Errors.KafkaConnectionError(e)
                    return total_bytes, err
        return total_bytes, err

    def write_eof(self):
        """Close the write end after flushing buffered data.

        (This is like typing ^D into a UNIX program reading from stdin.)

        Data may still be received.
        """
        log.debug('%s: write_eof', self)
        self._write = False
        if not self._write_buffer:
            self._sock.shutdown(socket.SHUT_WR)

    def can_write_eof(self):
        """Return True if this transport supports write_eof(), False if not."""
        return True

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
        if self._sock:
            self._net.unregister_event(self._sock, selectors.EVENT_READ | selectors.EVENT_WRITE)
            self._sock.close()
            self._sock = None
        if self._protocol:
            self._protocol.connection_lost(error)
            self._protocol = None

  # Twisted
    def abortConnection(self):
        """Close the connection abruptly."""
        return self.abort()

    def getHost(self):
        """Similar to getPeer, but returns an address describing this side of the connection.

        Returns IPv4Address or IPv6Address.
        """
        return self._sock.getsockname()

    def getPeer(self):
        """Get the remote address of this connection.

        Treat this method with caution. It is the unfortunate result of the CGI and Jabber standards,
        but should not be considered reliable for the usual host of reasons;
        port forwarding, proxying, firewalls, IP masquerading, etc.

        Returns IPv4Address or IPv6Address.
        """
        return self._sock.getpeername()

    def getTcpKeepAlive(self):
        """Return if SO_KEEPALIVE is enabled."""
        return self._sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE)

    def getTcpNoDelay(self):
        """Return if TCP_NODELAY is enabled."""
        return self._sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY)

    def loseWriteConnection(self):
        """Half-close the write side of a TCP connection."""
        return self.write_eof()

    def setTcpKeepAlive(self, enabled):
        """Enable/disable SO_KEEPALIVE."""
        return self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, enabled)

    def setTcpNoDelay(self, enabled):
        """Enable/disable TCP_NODELAY."""
        return self._sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, enabled)

    def loseConnection(self):
        """Close my connection, after writing all pending data.

        Note that if there is a registered producer on a transport it will not be closed until the producer has been unregistered.
        """
        return self.close()

    #def write(self, data):
    #    """Write some data to the physical connection, in sequence, in a non-blocking fashion.
    #
    #    If possible, make sure that it is all written. No data will ever be lost,
    #    although (obviously) the connection may be closed before it all gets through.
    #    """
    #    pass

    def writeSequence(self, data):
        """Write an iterable of byte strings to the physical connection.

        If possible, make sure that all of the data is written to the socket at once,
        without first copying it all into a single byte string.
        """
        return self.writelines(data)

    async def handshake(self):
        pass

    def host_port(self):
        host, port = self.getPeer()[0:2]
        local_port = self._sock.getsockname()[1]
        return '%s:%d<-%d' % (host, port, local_port)

    def __str__(self):
        state = ' (closed)' if self._closed else ''
        return f"<KafkaTCPTransport [{self.host_port()}]{state}>"


class KafkaSSLTransport(KafkaTCPTransport):
    def __init__(self, net, sock, ssl_context, server_hostname=None):
        self._ssl_context = ssl_context
        sock = ssl_context.wrap_socket(
            sock, server_hostname=server_hostname, do_handshake_on_connect=False)
        super().__init__(net, sock)

    async def handshake(self):
        while True:
            try:
                self._sock.do_handshake()
                return
            except ssl.SSLWantReadError:
                await self._net.wait_read(self._sock)
            except ssl.SSLWantWriteError:
                await self._net.wait_write(self._sock)

    def _sock_recv(self):
        recvd = []
        err = None
        while True:
            try:
                data = self._sock.recv(4096)
                if not data:
                    log.error('%s: socket disconnected', self)
                    err = Errors.KafkaConnectionError('socket disconnected')
                    break
                else:
                    recvd.append(data)
            except (BlockingIOError, InterruptedError,
                    ssl.SSLWantReadError, ssl.SSLWantWriteError):
                break
            except BaseException as e:
                log.exception('%s: Error receiving network data'
                              ' closing socket', self)
                err = Errors.KafkaConnectionError(e)
                break
        recvd_data = b''.join(recvd)
        return recvd_data, err

    def _sock_send(self):
        total_bytes = 0
        err = None
        while self._write_buffer:
            next_chunk = self._write_buffer.popleft()
            while next_chunk:
                try:
                    sent_bytes = self._sock.send(next_chunk)
                    total_bytes += sent_bytes
                    next_chunk = next_chunk[sent_bytes:]
                except (BlockingIOError, InterruptedError,
                        ssl.SSLWantReadError, ssl.SSLWantWriteError):
                    self._write_buffer.appendleft(next_chunk)
                    return total_bytes, err
                except BaseException as e:
                    log.exception("%s: Error sending request data: %s", self, e)
                    err = Errors.KafkaConnectionError(e)
                    return total_bytes, err
        return total_bytes, err

    def __str__(self):
        return ("<KafkaSSLTransport [%s:%d]" % self.getPeer()[0:2]) + (" (closed)>" if self._closed else ">")
