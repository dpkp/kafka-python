import collections
import copy
import errno
import logging
import io
from random import shuffle
from select import select
import socket
import struct
from threading import local
import time
import warnings

import six

import kafka.common as Errors
from kafka.future import Future
from kafka.protocol.api import RequestHeader
from kafka.protocol.commit import GroupCoordinatorResponse
from kafka.protocol.types import Int32
from kafka.version import __version__


if six.PY2:
    ConnectionError = socket.error
    BlockingIOError = Exception

log = logging.getLogger(__name__)

DEFAULT_SOCKET_TIMEOUT_SECONDS = 120
DEFAULT_KAFKA_PORT = 9092


class ConnectionStates(object):
    DISCONNECTED = '<disconnected>'
    CONNECTING = '<connecting>'
    CONNECTED = '<connected>'


InFlightRequest = collections.namedtuple('InFlightRequest',
    ['request', 'response_type', 'correlation_id', 'future', 'timestamp'])


class BrokerConnection(object):
    DEFAULT_CONFIG = {
        'client_id': 'kafka-python-' + __version__,
        'request_timeout_ms': 40000,
        'reconnect_backoff_ms': 50,
        'max_in_flight_requests_per_connection': 5,
        'receive_buffer_bytes': None,
        'send_buffer_bytes': None,
        'api_version': (0, 8, 2),  # default to most restrictive
    }

    def __init__(self, host, port, **configs):
        self.host = host
        self.port = port
        self.in_flight_requests = collections.deque()

        self.config = copy.copy(self.DEFAULT_CONFIG)
        for key in self.config:
            if key in configs:
                self.config[key] = configs[key]

        self.state = ConnectionStates.DISCONNECTED
        self._sock = None
        self._rbuffer = io.BytesIO()
        self._receiving = False
        self._next_payload_bytes = 0
        self.last_attempt = 0
        self.last_failure = 0
        self._processing = False
        self._correlation_id = 0

    def connect(self):
        """Attempt to connect and return ConnectionState"""
        if self.state is ConnectionStates.DISCONNECTED:
            self.close()
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if self.config['receive_buffer_bytes'] is not None:
                self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF,
                                      self.config['receive_buffer_bytes'])
            if self.config['send_buffer_bytes'] is not None:
                self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF,
                                      self.config['send_buffer_bytes'])
            self._sock.setblocking(False)
            try:
                ret = self._sock.connect_ex((self.host, self.port))
            except socket.error as ret:
                pass
            self.last_attempt = time.time()

            if not ret or ret is errno.EISCONN:
                self.state = ConnectionStates.CONNECTED
            elif ret in (errno.EINPROGRESS, errno.EALREADY):
                self.state = ConnectionStates.CONNECTING
            else:
                log.error('Connect attempt to %s returned error %s.'
                          ' Disconnecting.', self, ret)
                self.close()
                self.last_failure = time.time()

        if self.state is ConnectionStates.CONNECTING:
            # in non-blocking mode, use repeated calls to socket.connect_ex
            # to check connection status
            request_timeout = self.config['request_timeout_ms'] / 1000.0
            if time.time() > request_timeout + self.last_attempt:
                log.error('Connection attempt to %s timed out', self)
                self.close() # error=TimeoutError ?
                self.last_failure = time.time()

            else:
                try:
                    ret = self._sock.connect_ex((self.host, self.port))
                except socket.error as ret:
                    pass
                if not ret or ret is errno.EISCONN:
                    self.state = ConnectionStates.CONNECTED
                elif ret is not errno.EALREADY:
                    log.error('Connect attempt to %s returned error %s.'
                              ' Disconnecting.', self, ret)
                    self.close()
                    self.last_failure = time.time()
        return self.state

    def blacked_out(self):
        """
        Return true if we are disconnected from the given node and can't
        re-establish a connection yet
        """
        if self.state is ConnectionStates.DISCONNECTED:
            backoff = self.config['reconnect_backoff_ms'] / 1000.0
            if time.time() < self.last_attempt + backoff:
                return True
        return False

    def connected(self):
        """Return True iff socket is connected."""
        return self.state is ConnectionStates.CONNECTED

    def close(self, error=None):
        """Close socket and fail all in-flight-requests.

        Arguments:
            error (Exception, optional): pending in-flight-requests
                will be failed with this exception.
                Default: kafka.common.ConnectionError.
        """
        if self._sock:
            self._sock.close()
            self._sock = None
        self.state = ConnectionStates.DISCONNECTED
        self._receiving = False
        self._next_payload_bytes = 0
        self._rbuffer.seek(0)
        self._rbuffer.truncate()
        if error is None:
            error = Errors.ConnectionError()
        while self.in_flight_requests:
            ifr = self.in_flight_requests.popleft()
            ifr.future.failure(error)

    def send(self, request, expect_response=True):
        """send request, return Future()

        Can block on network if request is larger than send_buffer_bytes
        """
        future = Future()
        if not self.connected():
            return future.failure(Errors.ConnectionError())
        if not self.can_send_more():
            return future.failure(Errors.TooManyInFlightRequests())
        correlation_id = self._next_correlation_id()
        header = RequestHeader(request,
                               correlation_id=correlation_id,
                               client_id=self.config['client_id'])
        message = b''.join([header.encode(), request.encode()])
        size = Int32.encode(len(message))
        try:
            # In the future we might manage an internal write buffer
            # and send bytes asynchronously. For now, just block
            # sending each request payload
            self._sock.setblocking(True)
            sent_bytes = self._sock.send(size)
            assert sent_bytes == len(size)
            sent_bytes = self._sock.send(message)
            assert sent_bytes == len(message)
            self._sock.setblocking(False)
        except (AssertionError, ConnectionError) as e:
            log.exception("Error sending %s to %s", request, self)
            error = Errors.ConnectionError(e)
            self.close(error=error)
            return future.failure(error)
        log.debug('%s Request %d: %s', self, correlation_id, request)

        if expect_response:
            ifr = InFlightRequest(request=request,
                                  correlation_id=correlation_id,
                                  response_type=request.RESPONSE_TYPE,
                                  future=future,
                                  timestamp=time.time())
            self.in_flight_requests.append(ifr)
        else:
            future.success(None)

        return future

    def can_send_more(self):
        """Return True unless there are max_in_flight_requests."""
        max_ifrs = self.config['max_in_flight_requests_per_connection']
        return len(self.in_flight_requests) < max_ifrs

    def recv(self, timeout=0):
        """Non-blocking network receive.

        Return response if available
        """
        assert not self._processing, 'Recursion not supported'
        if not self.connected():
            log.warning('%s cannot recv: socket not connected', self)
            # If requests are pending, we should close the socket and
            # fail all the pending request futures
            if self.in_flight_requests:
                self.close()
            return None

        elif not self.in_flight_requests:
            log.warning('%s: No in-flight-requests to recv', self)
            return None

        elif self._requests_timed_out():
            log.warning('%s timed out after %s ms. Closing connection.',
                        self, self.config['request_timeout_ms'])
            self.close(error=Errors.RequestTimedOutError(
                'Request timed out after %s ms' %
                self.config['request_timeout_ms']))
            return None

        readable, _, _ = select([self._sock], [], [], timeout)
        if not readable:
            return None

        # Not receiving is the state of reading the payload header
        if not self._receiving:
            try:
                # An extremely small, but non-zero, probability that there are
                # more than 0 but not yet 4 bytes available to read
                self._rbuffer.write(self._sock.recv(4 - self._rbuffer.tell()))
            except ConnectionError as e:
                if six.PY2 and e.errno == errno.EWOULDBLOCK:
                    # This shouldn't happen after selecting above
                    # but just in case
                    return None
                log.exception('%s: Error receiving 4-byte payload header -'
                              ' closing socket', self)
                self.close(error=Errors.ConnectionError(e))
                return None
            except BlockingIOError:
                if six.PY3:
                    return None
                raise

            if self._rbuffer.tell() == 4:
                self._rbuffer.seek(0)
                self._next_payload_bytes = Int32.decode(self._rbuffer)
                # reset buffer and switch state to receiving payload bytes
                self._rbuffer.seek(0)
                self._rbuffer.truncate()
                self._receiving = True
            elif self._rbuffer.tell() > 4:
                raise Errors.KafkaError('this should not happen - are you threading?')

        if self._receiving:
            staged_bytes = self._rbuffer.tell()
            try:
                self._rbuffer.write(self._sock.recv(self._next_payload_bytes - staged_bytes))
            except ConnectionError as e:
                # Extremely small chance that we have exactly 4 bytes for a
                # header, but nothing to read in the body yet
                if six.PY2 and e.errno == errno.EWOULDBLOCK:
                    return None
                log.exception('%s: Error in recv', self)
                self.close(error=Errors.ConnectionError(e))
                return None
            except BlockingIOError:
                if six.PY3:
                    return None
                raise

            staged_bytes = self._rbuffer.tell()
            if staged_bytes > self._next_payload_bytes:
                self.close(error=Errors.KafkaError('Receive buffer has more bytes than expected?'))

            if staged_bytes != self._next_payload_bytes:
                return None

            self._receiving = False
            self._next_payload_bytes = 0
            self._rbuffer.seek(0)
            response = self._process_response(self._rbuffer)
            self._rbuffer.seek(0)
            self._rbuffer.truncate()
            return response

    def _process_response(self, read_buffer):
        assert not self._processing, 'Recursion not supported'
        self._processing = True
        ifr = self.in_flight_requests.popleft()

        # verify send/recv correlation ids match
        recv_correlation_id = Int32.decode(read_buffer)

        # 0.8.2 quirk
        if (self.config['api_version'] == (0, 8, 2) and
            ifr.response_type is GroupCoordinatorResponse and
            ifr.correlation_id != 0 and
            recv_correlation_id == 0):
            log.warning('Kafka 0.8.2 quirk -- GroupCoordinatorResponse'
                        ' coorelation id does not match request. This'
                        ' should go away once at least one topic has been'
                        ' initialized on the broker')

        elif ifr.correlation_id != recv_correlation_id:


            error = Errors.CorrelationIdError(
                'Correlation ids do not match: sent %d, recv %d'
                % (ifr.correlation_id, recv_correlation_id))
            ifr.future.failure(error)
            self.close()
            self._processing = False
            return None

        # decode response
        response = ifr.response_type.decode(read_buffer)
        log.debug('%s Response %d: %s', self, ifr.correlation_id, response)
        ifr.future.success(response)
        self._processing = False
        return response

    def _requests_timed_out(self):
        if self.in_flight_requests:
            oldest_at = self.in_flight_requests[0].timestamp
            timeout = self.config['request_timeout_ms'] / 1000.0
            if time.time() >= oldest_at + timeout:
                return True
        return False

    def _next_correlation_id(self):
        self._correlation_id = (self._correlation_id + 1) % 2**31
        return self._correlation_id

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
    """A socket connection to a single Kafka broker

    Arguments:
        host: the host name or IP address of a kafka broker
        port: the port number the kafka broker is listening on
        timeout: default 120. The socket timeout for sending and receiving data
            in seconds. None means no timeout, so a request can block forever.
    """
    def __init__(self, host, port, timeout=DEFAULT_SOCKET_TIMEOUT_SECONDS):
        warnings.warn('KafkaConnection has been deprecated and will be'
                      ' removed in a future release', DeprecationWarning)
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
        raise Errors.ConnectionError("Kafka @ {0}:{1} went away".format(self.host, self.port))

    def _read_bytes(self, num_bytes):
        bytes_left = num_bytes
        responses = []

        log.debug("About to read %d bytes from Kafka", num_bytes)

        # Make sure we have a connection
        if not self._sock:
            self.reinit()

        while bytes_left:

            try:
                # pylint: disable-msg=no-member
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
            # pylint: disable-msg=no-member
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
                # pylint: disable-msg=no-member
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
