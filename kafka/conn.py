from collections import deque
import copy
import logging
from random import shuffle
from select import select
import socket
import struct
from threading import local

import six

from kafka.common import ConnectionError
from kafka.protocol.api import RequestHeader
from kafka.protocol.types import Int32


log = logging.getLogger(__name__)

DEFAULT_SOCKET_TIMEOUT_SECONDS = 120
DEFAULT_KAFKA_PORT = 9092


class BrokerConnection(local):
    def __init__(self, host, port, timeout=DEFAULT_SOCKET_TIMEOUT_SECONDS,
                 client_id='kafka-python-0.10.0', correlation_id=0):
        super(BrokerConnection, self).__init__()
        self.host = host
        self.port = port
        self.timeout = timeout
        self._write_fd = None
        self._read_fd = None
        self.correlation_id = correlation_id
        self.client_id = client_id
        self.in_flight_requests = deque()

    def connect(self):
        if self.connected():
            self.close()
        try:
            sock = socket.create_connection((self.host, self.port), self.timeout)
            self._write_fd = sock.makefile('wb')
            self._read_fd = sock.makefile('rb')
        except socket.error:
            log.exception("Error in BrokerConnection.connect()")
            return None
        self.in_flight_requests.clear()
        return True

    def connected(self):
        return (self._read_fd is not None and self._write_fd is not None)

    def close(self):
        if self.connected():
            try:
                self._read_fd.close()
                self._write_fd.close()
            except socket.error:
                log.exception("Error in BrokerConnection.close()")
                pass
            self._read_fd = None
            self._write_fd = None
        self.in_flight_requests.clear()

    def send(self, request):
        if not self.connected() and not self.connect():
            return None
        self.correlation_id += 1
        header = RequestHeader(request,
                               correlation_id=self.correlation_id,
                               client_id=self.client_id)
        message = b''.join([header.encode(), request.encode()])
        size = Int32.encode(len(message))
        try:
            self._write_fd.write(size)
            self._write_fd.write(message)
            self._write_fd.flush()
        except socket.error:
            log.exception("Error in BrokerConnection.send()")
            self.close()
            return None
        self.in_flight_requests.append((self.correlation_id, request.RESPONSE_TYPE))
        return self.correlation_id

    def recv(self, timeout=None):
        if not self.connected():
            return None
        readable, _, _ = select([self._read_fd], [], [], timeout)
        if not readable:
            return None
        correlation_id, response_type = self.in_flight_requests.popleft()
        # Current implementation does not use size
        # instead we read directly from the socket fd buffer
        # alternatively, we could read size bytes into a separate buffer
        # and decode from that buffer (and verify buffer is empty afterwards)
        try:
            size = Int32.decode(self._read_fd)
            recv_correlation_id = Int32.decode(self._read_fd)
            if correlation_id != recv_correlation_id:
                raise RuntimeError('Correlation ids do not match!')
            response = response_type.decode(self._read_fd)
        except (RuntimeError, socket.error, struct.error):
            log.exception("Error in BrokerConnection.recv()")
            self.close()
            return None
        return response

    def __getnewargs__(self):
        return (self.host, self.port, self.timeout)

    def __repr__(self):
        return "<BrokerConnection host=%s port=%d>" % (self.host, self.port)


def collect_hosts(hosts, randomize=True):
    """
    Collects a comma-separated set of hosts (host:port) and optionally
    randomize the returned list.
    """

    if isinstance(hosts, six.string_types):
        hosts = hosts.strip().split(',')

    result = []
    for host_port in hosts:

        res = host_port.split(':')
        host = res[0]
        port = int(res[1]) if len(res) > 1 else DEFAULT_KAFKA_PORT
        result.append((host.strip(), port))

    if randomize:
        shuffle(result)

    return result


class KafkaConnection(local):
    """
    A socket connection to a single Kafka broker

    This class is _not_ thread safe. Each call to `send` must be followed
    by a call to `recv` in order to get the correct response. Eventually,
    we can do something in here to facilitate multiplexed requests/responses
    since the Kafka API includes a correlation id.

    Arguments:
        host: the host name or IP address of a kafka broker
        port: the port number the kafka broker is listening on
        timeout: default 120. The socket timeout for sending and receiving data
            in seconds. None means no timeout, so a request can block forever.
    """
    def __init__(self, host, port, timeout=DEFAULT_SOCKET_TIMEOUT_SECONDS):
        super(KafkaConnection, self).__init__()
        self.host = host
        self.port = port
        self.timeout = timeout
        self._sock = None

        self.reinit()

    def __getnewargs__(self):
        return (self.host, self.port, self.timeout)

    def __repr__(self):
        return "<KafkaConnection host=%s port=%d>" % (self.host, self.port)

    ###################
    #   Private API   #
    ###################

    def _raise_connection_error(self):
        # Cleanup socket if we have one
        if self._sock:
            self.close()

        # And then raise
        raise ConnectionError("Kafka @ {0}:{1} went away".format(self.host, self.port))

    def _read_bytes(self, num_bytes):
        bytes_left = num_bytes
        responses = []

        log.debug("About to read %d bytes from Kafka", num_bytes)

        # Make sure we have a connection
        if not self._sock:
            self.reinit()

        while bytes_left:

            try:
                data = self._sock.recv(min(bytes_left, 4096))

                # Receiving empty string from recv signals
                # that the socket is in error.  we will never get
                # more data from this socket
                if data == b'':
                    raise socket.error("Not enough data to read message -- did server kill socket?")

            except socket.error:
                log.exception('Unable to receive data from Kafka')
                self._raise_connection_error()

            bytes_left -= len(data)
            log.debug("Read %d/%d bytes from Kafka", num_bytes - bytes_left, num_bytes)
            responses.append(data)

        return b''.join(responses)

    ##################
    #   Public API   #
    ##################

    # TODO multiplex socket communication to allow for multi-threaded clients

    def get_connected_socket(self):
        if not self._sock:
            self.reinit()
        return self._sock

    def send(self, request_id, payload):
        """
        Send a request to Kafka

        Arguments::
            request_id (int): can be any int (used only for debug logging...)
            payload: an encoded kafka packet (see KafkaProtocol)
        """

        log.debug("About to send %d bytes to Kafka, request %d" % (len(payload), request_id))

        # Make sure we have a connection
        if not self._sock:
            self.reinit()

        try:
            self._sock.sendall(payload)
        except socket.error:
            log.exception('Unable to send payload to Kafka')
            self._raise_connection_error()

    def recv(self, request_id):
        """
        Get a response packet from Kafka

        Arguments:
            request_id: can be any int (only used for debug logging...)

        Returns:
            str: Encoded kafka packet response from server
        """
        log.debug("Reading response %d from Kafka" % request_id)

        # Make sure we have a connection
        if not self._sock:
            self.reinit()

        # Read the size off of the header
        resp = self._read_bytes(4)
        (size,) = struct.unpack('>i', resp)

        # Read the remainder of the response
        resp = self._read_bytes(size)
        return resp

    def copy(self):
        """
        Create an inactive copy of the connection object, suitable for
        passing to a background thread.

        The returned copy is not connected; you must call reinit() before
        using.
        """
        c = copy.deepcopy(self)
        # Python 3 doesn't copy custom attributes of the threadlocal subclass
        c.host = copy.copy(self.host)
        c.port = copy.copy(self.port)
        c.timeout = copy.copy(self.timeout)
        c._sock = None
        return c

    def close(self):
        """
        Shutdown and close the connection socket
        """
        log.debug("Closing socket connection for %s:%d" % (self.host, self.port))
        if self._sock:
            # Call shutdown to be a good TCP client
            # But expect an error if the socket has already been
            # closed by the server
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except socket.error:
                pass

            # Closing the socket should always succeed
            self._sock.close()
            self._sock = None
        else:
            log.debug("No socket found to close!")

    def reinit(self):
        """
        Re-initialize the socket connection
        close current socket (if open)
        and start a fresh connection
        raise ConnectionError on error
        """
        log.debug("Reinitializing socket connection for %s:%d" % (self.host, self.port))

        if self._sock:
            self.close()

        try:
            self._sock = socket.create_connection((self.host, self.port), self.timeout)
        except socket.error:
            log.exception('Unable to connect to kafka broker at %s:%d' % (self.host, self.port))
            self._raise_connection_error()
