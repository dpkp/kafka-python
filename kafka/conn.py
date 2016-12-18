from __future__ import absolute_import

import collections
import copy
import errno
import logging
import io
from random import shuffle
import socket
import ssl
import time

from kafka.vendor import six

import kafka.errors as Errors
from kafka.future import Future
from kafka.metrics.stats import Avg, Count, Max, Rate
from kafka.protocol.api import RequestHeader
from kafka.protocol.admin import SaslHandShakeRequest
from kafka.protocol.commit import GroupCoordinatorResponse
from kafka.protocol.types import Int32
from kafka.version import __version__


if six.PY2:
    ConnectionError = socket.error
    BlockingIOError = Exception

log = logging.getLogger(__name__)

DEFAULT_KAFKA_PORT = 9092

# support older ssl libraries
try:
    ssl.SSLWantReadError
    ssl.SSLWantWriteError
    ssl.SSLZeroReturnError
except:
    log.warning('old ssl module detected.'
                ' ssl error handling may not operate cleanly.'
                ' Consider upgrading to python 3.5 or 2.7')
    ssl.SSLWantReadError = ssl.SSLError
    ssl.SSLWantWriteError = ssl.SSLError
    ssl.SSLZeroReturnError = ssl.SSLError


class ConnectionStates(object):
    DISCONNECTING = '<disconnecting>'
    DISCONNECTED = '<disconnected>'
    CONNECTING = '<connecting>'
    HANDSHAKE = '<handshake>'
    CONNECTED = '<connected>'
    AUTHENTICATING = '<authenticating>'

InFlightRequest = collections.namedtuple('InFlightRequest',
    ['request', 'response_type', 'correlation_id', 'future', 'timestamp'])


class BrokerConnection(object):
    DEFAULT_CONFIG = {
        'client_id': 'kafka-python-' + __version__,
        'node_id': 0,
        'request_timeout_ms': 40000,
        'reconnect_backoff_ms': 50,
        'max_in_flight_requests_per_connection': 5,
        'receive_buffer_bytes': None,
        'send_buffer_bytes': None,
        'socket_options': [(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)],
        'security_protocol': 'PLAINTEXT',
        'ssl_context': None,
        'ssl_check_hostname': True,
        'ssl_cafile': None,
        'ssl_certfile': None,
        'ssl_keyfile': None,
        'ssl_crlfile': None,
        'ssl_password': None,
        'api_version': (0, 8, 2),  # default to most restrictive
        'state_change_callback': lambda conn: True,
        'metrics': None,
        'metric_group_prefix': '',
        'sasl_mechanism': 'PLAIN',
        'sasl_plain_username': None,
        'sasl_plain_password': None
    }
    SASL_MECHANISMS = ('PLAIN',)

    def __init__(self, host, port, afi, **configs):
        """Initialize a kafka broker connection

        Keyword Arguments:
            client_id (str): a name for this client. This string is passed in
                each request to servers and can be used to identify specific
                server-side log entries that correspond to this client. Also
                submitted to GroupCoordinator for logging with respect to
                consumer group administration. Default: 'kafka-python-{version}'
            reconnect_backoff_ms (int): The amount of time in milliseconds to
                wait before attempting to reconnect to a given host.
                Default: 50.
            request_timeout_ms (int): Client request timeout in milliseconds.
                Default: 40000.
            max_in_flight_requests_per_connection (int): Requests are pipelined
                to kafka brokers up to this number of maximum requests per
                broker connection. Default: 5.
            receive_buffer_bytes (int): The size of the TCP receive buffer
                (SO_RCVBUF) to use when reading data. Default: None (relies on
                system defaults). Java client defaults to 32768.
            send_buffer_bytes (int): The size of the TCP send buffer
                (SO_SNDBUF) to use when sending data. Default: None (relies on
                system defaults). Java client defaults to 131072.
            socket_options (list): List of tuple-arguments to socket.setsockopt
                to apply to broker connection sockets. Default:
                [(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)]
            security_protocol (str): Protocol used to communicate with brokers.
                Valid values are: PLAINTEXT, SSL. Default: PLAINTEXT.
            ssl_context (ssl.SSLContext): pre-configured SSLContext for wrapping
                socket connections. If provided, all other ssl_* configurations
                will be ignored. Default: None.
            ssl_check_hostname (bool): flag to configure whether ssl handshake
                should verify that the certificate matches the brokers hostname.
                default: True.
            ssl_cafile (str): optional filename of ca file to use in certificate
                veriication. default: None.
            ssl_certfile (str): optional filename of file in pem format containing
                the client certificate, as well as any ca certificates needed to
                establish the certificate's authenticity. default: None.
            ssl_keyfile (str): optional filename containing the client private key.
                default: None.
            ssl_password (callable, str, bytes, bytearray): optional password or
                callable function that returns a password, for decrypting the
                client private key. Default: None.
            ssl_crlfile (str): optional filename containing the CRL to check for
                certificate expiration. By default, no CRL check is done. When
                providing a file, only the leaf certificate will be checked against
                this CRL. The CRL can only be checked with Python 3.4+ or 2.7.9+.
                default: None.
            api_version (tuple): specify which kafka API version to use. Accepted
                values are: (0, 8, 0), (0, 8, 1), (0, 8, 2), (0, 9), (0, 10)
                If None, KafkaClient will attempt to infer the broker
                version by probing various APIs. Default: None
            api_version_auto_timeout_ms (int): number of milliseconds to throw a
                timeout exception from the constructor when checking the broker
                api version. Only applies if api_version is None
            state_change_callback (callable): function to be called when the
                connection state changes from CONNECTING to CONNECTED etc.
            metrics (kafka.metrics.Metrics): Optionally provide a metrics
                instance for capturing network IO stats. Default: None.
            metric_group_prefix (str): Prefix for metric names. Default: ''
            sasl_mechanism (str): string picking sasl mechanism when security_protocol
                is SASL_PLAINTEXT or SASL_SSL. Currently only PLAIN is supported.
                Default: None
            sasl_plain_username (str): username for sasl PLAIN authentication.
                Default: None
            sasl_plain_password (str): password for sasl PLAIN authentication.
                Default: None
        """
        self.host = host
        self.hostname = host
        self.port = port
        self.afi = afi
        self._init_host = host
        self._init_port = port
        self._init_afi = afi
        self.in_flight_requests = collections.deque()

        self.config = copy.copy(self.DEFAULT_CONFIG)
        for key in self.config:
            if key in configs:
                self.config[key] = configs[key]

        if self.config['receive_buffer_bytes'] is not None:
            self.config['socket_options'].append(
                (socket.SOL_SOCKET, socket.SO_RCVBUF,
                 self.config['receive_buffer_bytes']))
        if self.config['send_buffer_bytes'] is not None:
            self.config['socket_options'].append(
                 (socket.SOL_SOCKET, socket.SO_SNDBUF,
                 self.config['send_buffer_bytes']))

        if self.config['security_protocol'] in ('SASL_PLAINTEXT', 'SASL_SSL'):
            assert self.config['sasl_mechanism'] in self.SASL_MECHANISMS, (
                'sasl_mechanism must be in ' + ', '.join(self.SASL_MECHANISMS))
            if self.config['sasl_mechanism'] == 'PLAIN':
                assert self.config['sasl_plain_username'] is not None, 'sasl_plain_username required for PLAIN sasl'
                assert self.config['sasl_plain_password'] is not None, 'sasl_plain_password required for PLAIN sasl'

        self.state = ConnectionStates.DISCONNECTED
        self._sock = None
        self._ssl_context = None
        if self.config['ssl_context'] is not None:
            self._ssl_context = self.config['ssl_context']
        self._sasl_auth_future = None
        self._rbuffer = io.BytesIO()
        self._receiving = False
        self._next_payload_bytes = 0
        self.last_attempt = 0
        self.last_failure = 0
        self._processing = False
        self._correlation_id = 0
        self._gai = None
        self._gai_index = 0
        self._sensors = None
        if self.config['metrics']:
            self._sensors = BrokerConnectionMetrics(self.config['metrics'],
                                                    self.config['metric_group_prefix'],
                                                    self.config['node_id'])

    def connect(self):
        """Attempt to connect and return ConnectionState"""
        if self.state is ConnectionStates.DISCONNECTED:
            self.close()
            log.debug('%s: creating new socket', str(self))
            # if self.afi is set to AF_UNSPEC, then we need to do a name
            # resolution and try all available address families
            if self._init_afi == socket.AF_UNSPEC:
                if self._gai is None:
                    # XXX: all DNS functions in Python are blocking. If we really
                    # want to be non-blocking here, we need to use a 3rd-party
                    # library like python-adns, or move resolution onto its
                    # own thread. This will be subject to the default libc
                    # name resolution timeout (5s on most Linux boxes)
                    try:
                        self._gai = socket.getaddrinfo(self._init_host,
                                                       self._init_port,
                                                       socket.AF_UNSPEC,
                                                       socket.SOCK_STREAM)
                    except socket.gaierror as ex:
                        raise socket.gaierror('getaddrinfo failed for {0}:{1}, '
                          'exception was {2}. Is your advertised.listeners (called'
                          'advertised.host.name before Kafka 9) correct and resolvable?'.format(
                             self._init_host, self._init_port, ex
                          ))
                    self._gai_index = 0
                else:
                    # if self._gai already exists, then we should try the next
                    # name
                    self._gai_index += 1
                while True:
                    if self._gai_index >= len(self._gai):
                        log.error('Unable to connect to any of the names for {0}:{1}'.format(
                            self._init_host, self._init_port
                        ))
                        self.close()
                        return
                    afi, _, __, ___, sockaddr = self._gai[self._gai_index]
                    if afi not in (socket.AF_INET, socket.AF_INET6):
                        self._gai_index += 1
                        continue
                    break
                self.host, self.port = sockaddr[:2]
                self._sock = socket.socket(afi, socket.SOCK_STREAM)
            else:
                self._sock = socket.socket(self._init_afi, socket.SOCK_STREAM)

            for option in self.config['socket_options']:
                self._sock.setsockopt(*option)

            self._sock.setblocking(False)
            if self.config['security_protocol'] in ('SSL', 'SASL_SSL'):
                self._wrap_ssl()
            self.state = ConnectionStates.CONNECTING
            self.last_attempt = time.time()
            self.config['state_change_callback'](self)

        if self.state is ConnectionStates.CONNECTING:
            # in non-blocking mode, use repeated calls to socket.connect_ex
            # to check connection status
            request_timeout = self.config['request_timeout_ms'] / 1000.0
            ret = None
            try:
                ret = self._sock.connect_ex((self.host, self.port))
                # if we got here through a host lookup, we've found a host,port,af tuple
                # that works save it so we don't do a GAI lookup again
                if self._gai is not None:
                    self.afi = self._sock.family
                    self._gai = None
            except socket.error as err:
                ret = err.errno

            # Connection succeeded
            if not ret or ret == errno.EISCONN:
                log.debug('%s: established TCP connection', str(self))
                if self.config['security_protocol'] in ('SSL', 'SASL_SSL'):
                    log.debug('%s: initiating SSL handshake', str(self))
                    self.state = ConnectionStates.HANDSHAKE
                elif self.config['security_protocol'] == 'SASL_PLAINTEXT':
                    self.state = ConnectionStates.AUTHENTICATING
                else:
                    self.state = ConnectionStates.CONNECTED
                self.config['state_change_callback'](self)

            # Connection failed
            # WSAEINVAL == 10022, but errno.WSAEINVAL is not available on non-win systems
            elif ret not in (errno.EINPROGRESS, errno.EALREADY, errno.EWOULDBLOCK, 10022):
                log.error('Connect attempt to %s returned error %s.'
                          ' Disconnecting.', self, ret)
                self.close()

            # Connection timed out
            elif time.time() > request_timeout + self.last_attempt:
                log.error('Connection attempt to %s timed out', self)
                self.close() # error=TimeoutError ?

            # Needs retry
            else:
                pass

        if self.state is ConnectionStates.HANDSHAKE:
            if self._try_handshake():
                log.debug('%s: completed SSL handshake.', str(self))
                if self.config['security_protocol'] == 'SASL_SSL':
                    self.state = ConnectionStates.AUTHENTICATING
                else:
                    self.state = ConnectionStates.CONNECTED
                self.config['state_change_callback'](self)

        if self.state is ConnectionStates.AUTHENTICATING:
            assert self.config['security_protocol'] in ('SASL_PLAINTEXT', 'SASL_SSL')
            if self._try_authenticate():
                log.info('%s: Authenticated as %s', str(self), self.config['sasl_plain_username'])
                self.state = ConnectionStates.CONNECTED
                self.config['state_change_callback'](self)

        return self.state

    def _wrap_ssl(self):
        assert self.config['security_protocol'] in ('SSL', 'SASL_SSL')
        if self._ssl_context is None:
            log.debug('%s: configuring default SSL Context', str(self))
            self._ssl_context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)  # pylint: disable=no-member
            self._ssl_context.options |= ssl.OP_NO_SSLv2  # pylint: disable=no-member
            self._ssl_context.options |= ssl.OP_NO_SSLv3  # pylint: disable=no-member
            self._ssl_context.verify_mode = ssl.CERT_OPTIONAL
            if self.config['ssl_check_hostname']:
                self._ssl_context.check_hostname = True
            if self.config['ssl_cafile']:
                log.info('%s: Loading SSL CA from %s', str(self), self.config['ssl_cafile'])
                self._ssl_context.load_verify_locations(self.config['ssl_cafile'])
                self._ssl_context.verify_mode = ssl.CERT_REQUIRED
            if self.config['ssl_certfile'] and self.config['ssl_keyfile']:
                log.info('%s: Loading SSL Cert from %s', str(self), self.config['ssl_certfile'])
                log.info('%s: Loading SSL Key from %s', str(self), self.config['ssl_keyfile'])
                self._ssl_context.load_cert_chain(
                    certfile=self.config['ssl_certfile'],
                    keyfile=self.config['ssl_keyfile'],
                    password=self.config['ssl_password'])
            if self.config['ssl_crlfile']:
                if not hasattr(ssl, 'VERIFY_CRL_CHECK_LEAF'):
                    log.error('%s: No CRL support with this version of Python.'
                              ' Disconnecting.', self)
                    self.close()
                    return
                log.info('%s: Loading SSL CRL from %s', str(self), self.config['ssl_crlfile'])
                self._ssl_context.load_verify_locations(self.config['ssl_crlfile'])
                # pylint: disable=no-member
                self._ssl_context.verify_flags |= ssl.VERIFY_CRL_CHECK_LEAF
        log.debug('%s: wrapping socket in ssl context', str(self))
        try:
            self._sock = self._ssl_context.wrap_socket(
                self._sock,
                server_hostname=self.hostname,
                do_handshake_on_connect=False)
        except ssl.SSLError:
            log.exception('%s: Failed to wrap socket in SSLContext!', str(self))
            self.close()
            self.last_failure = time.time()

    def _try_handshake(self):
        assert self.config['security_protocol'] in ('SSL', 'SASL_SSL')
        try:
            self._sock.do_handshake()
            return True
        # old ssl in python2.6 will swallow all SSLErrors here...
        except (ssl.SSLWantReadError, ssl.SSLWantWriteError):
            pass
        except ssl.SSLZeroReturnError:
            log.warning('SSL connection closed by server during handshake.')
            self.close()
        # Other SSLErrors will be raised to user

        return False

    def _try_authenticate(self):
        assert self.config['api_version'] is None or self.config['api_version'] >= (0, 10)

        if self._sasl_auth_future is None:
            # Build a SaslHandShakeRequest message
            request = SaslHandShakeRequest[0](self.config['sasl_mechanism'])
            future = Future()
            sasl_response = self._send(request)
            sasl_response.add_callback(self._handle_sasl_handshake_response, future)
            sasl_response.add_errback(lambda f, e: f.failure(e), future)
            self._sasl_auth_future = future
        self._recv()
        if self._sasl_auth_future.failed():
            raise self._sasl_auth_future.exception # pylint: disable-msg=raising-bad-type
        return self._sasl_auth_future.succeeded()

    def _handle_sasl_handshake_response(self, future, response):
        error_type = Errors.for_code(response.error_code)
        if error_type is not Errors.NoError:
            error = error_type(self)
            self.close(error=error)
            return future.failure(error_type(self))

        if self.config['sasl_mechanism'] == 'PLAIN':
            return self._try_authenticate_plain(future)
        else:
            return future.failure(
                Errors.UnsupportedSaslMechanismError(
                    'kafka-python does not support SASL mechanism %s' %
                    self.config['sasl_mechanism']))

    def _try_authenticate_plain(self, future):
        if self.config['security_protocol'] == 'SASL_PLAINTEXT':
            log.warning('%s: Sending username and password in the clear', str(self))

        data = b''
        try:
            self._sock.setblocking(True)
            # Send PLAIN credentials per RFC-4616
            msg = bytes('\0'.join([self.config['sasl_plain_username'],
                                   self.config['sasl_plain_username'],
                                   self.config['sasl_plain_password']]).encode('utf-8'))
            size = Int32.encode(len(msg))
            self._sock.sendall(size + msg)

            # The server will send a zero sized message (that is Int32(0)) on success.
            # The connection is closed on failure
            while len(data) < 4:
                fragment = self._sock.recv(4 - len(data))
                if not fragment:
                    log.error('%s: Authentication failed for user %s', self, self.config['sasl_plain_username'])
                    error = Errors.AuthenticationFailedError(
                        'Authentication failed for user {0}'.format(
                            self.config['sasl_plain_username']))
                    future.failure(error)
                    raise error
                data += fragment
            self._sock.setblocking(False)
        except (AssertionError, ConnectionError) as e:
            log.exception("%s: Error receiving reply from server",  self)
            error = Errors.ConnectionError("%s: %s" % (str(self), e))
            future.failure(error)
            self.close(error=error)

        if data != b'\x00\x00\x00\x00':
            return future.failure(Errors.AuthenticationFailedError())

        return future.success(True)

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

    def connecting(self):
        """Returns True if still connecting (this may encompass several
        different states, such as SSL handshake, authorization, etc)."""
        return self.state in (ConnectionStates.CONNECTING,
                              ConnectionStates.HANDSHAKE,
                              ConnectionStates.AUTHENTICATING)

    def disconnected(self):
        """Return True iff socket is closed"""
        return self.state is ConnectionStates.DISCONNECTED

    def close(self, error=None):
        """Close socket and fail all in-flight-requests.

        Arguments:
            error (Exception, optional): pending in-flight-requests
                will be failed with this exception.
                Default: kafka.errors.ConnectionError.
        """
        if self.state is not ConnectionStates.DISCONNECTED:
            self.state = ConnectionStates.DISCONNECTING
            self.config['state_change_callback'](self)
        if self._sock:
            self._sock.close()
            self._sock = None
        self.state = ConnectionStates.DISCONNECTED
        self.last_failure = time.time()
        self._receiving = False
        self._next_payload_bytes = 0
        self._rbuffer.seek(0)
        self._rbuffer.truncate()
        if error is None:
            error = Errors.ConnectionError(str(self))
        while self.in_flight_requests:
            ifr = self.in_flight_requests.popleft()
            ifr.future.failure(error)
        self.config['state_change_callback'](self)

    def send(self, request, expect_response=True):
        """send request, return Future()

        Can block on network if request is larger than send_buffer_bytes
        """
        future = Future()
        if self.connecting():
            return future.failure(Errors.NodeNotReadyError(str(self)))
        elif not self.connected():
            return future.failure(Errors.ConnectionError(str(self)))
        elif not self.can_send_more():
            return future.failure(Errors.TooManyInFlightRequests(str(self)))
        return self._send(request, expect_response=expect_response)

    def _send(self, request, expect_response=True):
        future = Future()
        correlation_id = self._next_correlation_id()
        header = RequestHeader(request,
                               correlation_id=correlation_id,
                               client_id=self.config['client_id'])
        message = b''.join([header.encode(), request.encode()])
        size = Int32.encode(len(message))
        data = size + message
        try:
            # In the future we might manage an internal write buffer
            # and send bytes asynchronously. For now, just block
            # sending each request payload
            self._sock.setblocking(True)
            total_sent = 0
            while total_sent < len(data):
                sent_bytes = self._sock.send(data[total_sent:])
                total_sent += sent_bytes
            assert total_sent == len(data)
            if self._sensors:
                self._sensors.bytes_sent.record(total_sent)
            self._sock.setblocking(False)
        except (AssertionError, ConnectionError) as e:
            log.exception("Error sending %s to %s", request, self)
            error = Errors.ConnectionError("%s: %s" % (str(self), e))
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
        """Return True unless there are max_in_flight_requests_per_connection."""
        max_ifrs = self.config['max_in_flight_requests_per_connection']
        return len(self.in_flight_requests) < max_ifrs

    def recv(self):
        """Non-blocking network receive.

        Return response if available
        """
        assert not self._processing, 'Recursion not supported'
        if not self.connected() and not self.state is ConnectionStates.AUTHENTICATING:
            log.warning('%s cannot recv: socket not connected', self)
            # If requests are pending, we should close the socket and
            # fail all the pending request futures
            if self.in_flight_requests:
                self.close()
            return None

        elif not self.in_flight_requests:
            log.warning('%s: No in-flight-requests to recv', self)
            return None

        response = self._recv()
        if not response and self.requests_timed_out():
            log.warning('%s timed out after %s ms. Closing connection.',
                        self, self.config['request_timeout_ms'])
            self.close(error=Errors.RequestTimedOutError(
                'Request timed out after %s ms' %
                self.config['request_timeout_ms']))
            return None
        return response

    def _recv(self):
        # Not receiving is the state of reading the payload header
        if not self._receiving:
            try:
                bytes_to_read = 4 - self._rbuffer.tell()
                data = self._sock.recv(bytes_to_read)
                # We expect socket.recv to raise an exception if there is not
                # enough data to read the full bytes_to_read
                # but if the socket is disconnected, we will get empty data
                # without an exception raised
                if not data:
                    log.error('%s: socket disconnected', self)
                    self.close(error=Errors.ConnectionError('socket disconnected'))
                    return None
                self._rbuffer.write(data)
            except ssl.SSLWantReadError:
                return None
            except ConnectionError as e:
                if six.PY2 and e.errno == errno.EWOULDBLOCK:
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
                bytes_to_read = self._next_payload_bytes - staged_bytes
                data = self._sock.recv(bytes_to_read)
                # We expect socket.recv to raise an exception if there is not
                # enough data to read the full bytes_to_read
                # but if the socket is disconnected, we will get empty data
                # without an exception raised
                if bytes_to_read and not data:
                    log.error('%s: socket disconnected', self)
                    self.close(error=Errors.ConnectionError('socket disconnected'))
                    return None
                self._rbuffer.write(data)
            except ssl.SSLWantReadError:
                return None
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
            if self._sensors:
                self._sensors.bytes_received.record(4 + self._rbuffer.tell())
            self._rbuffer.seek(0)
            response = self._process_response(self._rbuffer)
            self._rbuffer.seek(0)
            self._rbuffer.truncate()
            return response

    def _process_response(self, read_buffer):
        assert not self._processing, 'Recursion not supported'
        self._processing = True
        ifr = self.in_flight_requests.popleft()
        if self._sensors:
            self._sensors.request_time.record((time.time() - ifr.timestamp) * 1000)

        # verify send/recv correlation ids match
        recv_correlation_id = Int32.decode(read_buffer)

        # 0.8.2 quirk
        if (self.config['api_version'] == (0, 8, 2) and
            ifr.response_type is GroupCoordinatorResponse[0] and
            ifr.correlation_id != 0 and
            recv_correlation_id == 0):
            log.warning('Kafka 0.8.2 quirk -- GroupCoordinatorResponse'
                        ' coorelation id does not match request. This'
                        ' should go away once at least one topic has been'
                        ' initialized on the broker')

        elif ifr.correlation_id != recv_correlation_id:
            error = Errors.CorrelationIdError(
                '%s: Correlation ids do not match: sent %d, recv %d'
                % (str(self), ifr.correlation_id, recv_correlation_id))
            ifr.future.failure(error)
            self.close()
            self._processing = False
            return None

        # decode response
        try:
            response = ifr.response_type.decode(read_buffer)
        except ValueError:
            read_buffer.seek(0)
            buf = read_buffer.read()
            log.error('%s Response %d [ResponseType: %s Request: %s]:'
                      ' Unable to decode %d-byte buffer: %r', self,
                      ifr.correlation_id, ifr.response_type,
                      ifr.request, len(buf), buf)
            ifr.future.failure(Errors.UnknownError('Unable to decode response'))
            self.close()
            self._processing = False
            return None

        log.debug('%s Response %d: %s', self, ifr.correlation_id, response)
        ifr.future.success(response)
        self._processing = False
        return response

    def requests_timed_out(self):
        if self.in_flight_requests:
            oldest_at = self.in_flight_requests[0].timestamp
            timeout = self.config['request_timeout_ms'] / 1000.0
            if time.time() >= oldest_at + timeout:
                return True
        return False

    def _next_correlation_id(self):
        self._correlation_id = (self._correlation_id + 1) % 2**31
        return self._correlation_id

    def check_version(self, timeout=2, strict=False):
        """Attempt to guess the broker version.

        Note: This is a blocking call.

        Returns: version tuple, i.e. (0, 10), (0, 9), (0, 8, 2), ...
        """
        # Monkeypatch some connection configurations to avoid timeouts
        override_config = {
            'request_timeout_ms': timeout * 1000,
            'max_in_flight_requests_per_connection': 5
        }
        stashed = {}
        for key in override_config:
            stashed[key] = self.config[key]
            self.config[key] = override_config[key]

        # kafka kills the connection when it doesnt recognize an API request
        # so we can send a test request and then follow immediately with a
        # vanilla MetadataRequest. If the server did not recognize the first
        # request, both will be failed with a ConnectionError that wraps
        # socket.error (32, 54, or 104)
        from .protocol.admin import ApiVersionRequest, ListGroupsRequest
        from .protocol.commit import OffsetFetchRequest, GroupCoordinatorRequest
        from .protocol.metadata import MetadataRequest

        # Socket errors are logged as exceptions and can alarm users. Mute them
        from logging import Filter

        class ConnFilter(Filter):
            def filter(self, record):
                if record.funcName == 'check_version':
                    return True
                return False
        log_filter = ConnFilter()
        log.addFilter(log_filter)

        test_cases = [
            ((0, 10), ApiVersionRequest[0]()),
            ((0, 9), ListGroupsRequest[0]()),
            ((0, 8, 2), GroupCoordinatorRequest[0]('kafka-python-default-group')),
            ((0, 8, 1), OffsetFetchRequest[0]('kafka-python-default-group', [])),
            ((0, 8, 0), MetadataRequest[0]([])),
        ]

        def connect():
            self.connect()
            if self.connected():
                return
            timeout_at = time.time() + timeout
            while time.time() < timeout_at and self.connecting():
                if self.connect() is ConnectionStates.CONNECTED:
                    return
                time.sleep(0.05)
            raise Errors.NodeNotReadyError()

        for version, request in test_cases:
            connect()
            f = self.send(request)
            # HACK: sleeping to wait for socket to send bytes
            time.sleep(0.1)
            # when broker receives an unrecognized request API
            # it abruptly closes our socket.
            # so we attempt to send a second request immediately
            # that we believe it will definitely recognize (metadata)
            # the attempt to write to a disconnected socket should
            # immediately fail and allow us to infer that the prior
            # request was unrecognized
            mr = self.send(MetadataRequest[0]([]))

            if self._sock:
                self._sock.setblocking(True)
            while not (f.is_done and mr.is_done):
                self.recv()
            if self._sock:
                self._sock.setblocking(False)

            if f.succeeded():
                log.info('Broker version identifed as %s', '.'.join(map(str, version)))
                log.info('Set configuration api_version=%s to skip auto'
                         ' check_version requests on startup', version)
                break

            # Only enable strict checking to verify that we understand failure
            # modes. For most users, the fact that the request failed should be
            # enough to rule out a particular broker version.
            if strict:
                # If the socket flush hack did not work (which should force the
                # connection to close and fail all pending requests), then we
                # get a basic Request Timeout. This is not ideal, but we'll deal
                if isinstance(f.exception, Errors.RequestTimedOutError):
                    pass

                # 0.9 brokers do not close the socket on unrecognized api
                # requests (bug...). In this case we expect to see a correlation
                # id mismatch
                elif (isinstance(f.exception, Errors.CorrelationIdError) and
                      version == (0, 10)):
                    pass
                elif six.PY2:
                    assert isinstance(f.exception.args[0], socket.error)
                    assert f.exception.args[0].errno in (32, 54, 104)
                else:
                    assert isinstance(f.exception.args[0], ConnectionError)
            log.info("Broker is not v%s -- it did not recognize %s",
                     version, request.__class__.__name__)
        else:
            raise Errors.UnrecognizedBrokerVersion()

        log.removeFilter(log_filter)
        for key in stashed:
            self.config[key] = stashed[key]
        return version

    def __repr__(self):
        return "<BrokerConnection host=%s/%s port=%d>" % (self.hostname, self.host,
                                                          self.port)


class BrokerConnectionMetrics(object):
    def __init__(self, metrics, metric_group_prefix, node_id):
        self.metrics = metrics

        # Any broker may have registered summary metrics already
        # but if not, we need to create them so we can set as parents below
        all_conns_transferred = metrics.get_sensor('bytes-sent-received')
        if not all_conns_transferred:
            metric_group_name = metric_group_prefix + '-metrics'

            bytes_transferred = metrics.sensor('bytes-sent-received')
            bytes_transferred.add(metrics.metric_name(
                'network-io-rate', metric_group_name,
                'The average number of network operations (reads or writes) on all'
                ' connections per second.'), Rate(sampled_stat=Count()))

            bytes_sent = metrics.sensor('bytes-sent',
                                        parents=[bytes_transferred])
            bytes_sent.add(metrics.metric_name(
                'outgoing-byte-rate', metric_group_name,
                'The average number of outgoing bytes sent per second to all'
                ' servers.'), Rate())
            bytes_sent.add(metrics.metric_name(
                'request-rate', metric_group_name,
                'The average number of requests sent per second.'),
                Rate(sampled_stat=Count()))
            bytes_sent.add(metrics.metric_name(
                'request-size-avg', metric_group_name,
                'The average size of all requests in the window.'), Avg())
            bytes_sent.add(metrics.metric_name(
                'request-size-max', metric_group_name,
                'The maximum size of any request sent in the window.'), Max())

            bytes_received = metrics.sensor('bytes-received',
                                            parents=[bytes_transferred])
            bytes_received.add(metrics.metric_name(
                'incoming-byte-rate', metric_group_name,
                'Bytes/second read off all sockets'), Rate())
            bytes_received.add(metrics.metric_name(
                'response-rate', metric_group_name,
                'Responses received sent per second.'),
                Rate(sampled_stat=Count()))

            request_latency = metrics.sensor('request-latency')
            request_latency.add(metrics.metric_name(
                'request-latency-avg', metric_group_name,
                'The average request latency in ms.'),
                Avg())
            request_latency.add(metrics.metric_name(
                'request-latency-max', metric_group_name,
                'The maximum request latency in ms.'),
                Max())

        # if one sensor of the metrics has been registered for the connection,
        # then all other sensors should have been registered; and vice versa
        node_str = 'node-{0}'.format(node_id)
        node_sensor = metrics.get_sensor(node_str + '.bytes-sent')
        if not node_sensor:
            metric_group_name = metric_group_prefix + '-node-metrics.' + node_str

            bytes_sent = metrics.sensor(
                node_str + '.bytes-sent',
                parents=[metrics.get_sensor('bytes-sent')])
            bytes_sent.add(metrics.metric_name(
                'outgoing-byte-rate', metric_group_name,
                'The average number of outgoing bytes sent per second.'),
                Rate())
            bytes_sent.add(metrics.metric_name(
                'request-rate', metric_group_name,
                'The average number of requests sent per second.'),
                Rate(sampled_stat=Count()))
            bytes_sent.add(metrics.metric_name(
                'request-size-avg', metric_group_name,
                'The average size of all requests in the window.'),
                Avg())
            bytes_sent.add(metrics.metric_name(
                'request-size-max', metric_group_name,
                'The maximum size of any request sent in the window.'),
                Max())

            bytes_received = metrics.sensor(
                node_str + '.bytes-received',
                parents=[metrics.get_sensor('bytes-received')])
            bytes_received.add(metrics.metric_name(
                'incoming-byte-rate', metric_group_name,
                'Bytes/second read off node-connection socket'),
                Rate())
            bytes_received.add(metrics.metric_name(
                'response-rate', metric_group_name,
                'The average number of responses received per second.'),
                Rate(sampled_stat=Count()))

            request_time = metrics.sensor(
                node_str + '.latency',
                parents=[metrics.get_sensor('request-latency')])
            request_time.add(metrics.metric_name(
                'request-latency-avg', metric_group_name,
                'The average request latency in ms.'),
                Avg())
            request_time.add(metrics.metric_name(
                'request-latency-max', metric_group_name,
                'The maximum request latency in ms.'),
                Max())

        self.bytes_sent = metrics.sensor(node_str + '.bytes-sent')
        self.bytes_received = metrics.sensor(node_str + '.bytes-received')
        self.request_time = metrics.sensor(node_str + '.latency')


def _address_family(address):
    """
        Attempt to determine the family of an address (or hostname)

        :return: either socket.AF_INET or socket.AF_INET6 or socket.AF_UNSPEC if the address family
                 could not be determined
    """
    if address.startswith('[') and address.endswith(']'):
        return socket.AF_INET6
    for af in (socket.AF_INET, socket.AF_INET6):
        try:
            socket.inet_pton(af, address)
            return af
        except (ValueError, AttributeError, socket.error):
            continue
    return socket.AF_UNSPEC


def get_ip_port_afi(host_and_port_str):
    """
        Parse the IP and port from a string in the format of:

            * host_or_ip          <- Can be either IPv4 address literal or hostname/fqdn
            * host_or_ipv4:port   <- Can be either IPv4 address literal or hostname/fqdn
            * [host_or_ip]        <- IPv6 address literal
            * [host_or_ip]:port.  <- IPv6 address literal

        .. note:: IPv6 address literals with ports *must* be enclosed in brackets

        .. note:: If the port is not specified, default will be returned.

        :return: tuple (host, port, afi), afi will be socket.AF_INET or socket.AF_INET6 or socket.AF_UNSPEC
    """
    host_and_port_str = host_and_port_str.strip()
    if host_and_port_str.startswith('['):
        af = socket.AF_INET6
        host, rest = host_and_port_str[1:].split(']')
        if rest:
            port = int(rest[1:])
        else:
            port = DEFAULT_KAFKA_PORT
        return host, port, af
    else:
        if ':' not in host_and_port_str:
            af = _address_family(host_and_port_str)
            return host_and_port_str, DEFAULT_KAFKA_PORT, af
        else:
            # now we have something with a colon in it and no square brackets. It could be
            # either an IPv6 address literal (e.g., "::1") or an IP:port pair or a host:port pair
            try:
                # if it decodes as an IPv6 address, use that
                socket.inet_pton(socket.AF_INET6, host_and_port_str)
                return host_and_port_str, DEFAULT_KAFKA_PORT, socket.AF_INET6
            except AttributeError:
                log.warning('socket.inet_pton not available on this platform.'
                            ' consider pip install win_inet_pton')
                pass
            except (ValueError, socket.error):
                # it's a host:port pair
                pass
            host, port = host_and_port_str.rsplit(':', 1)
            port = int(port)

            af = _address_family(host)
            return host, port, af


def collect_hosts(hosts, randomize=True):
    """
    Collects a comma-separated set of hosts (host:port) and optionally
    randomize the returned list.
    """

    if isinstance(hosts, six.string_types):
        hosts = hosts.strip().split(',')

    result = []
    afi = socket.AF_INET
    for host_port in hosts:

        host, port, afi = get_ip_port_afi(host_port)

        if port < 0:
            port = DEFAULT_KAFKA_PORT

        result.append((host, port, afi))

    if randomize:
        shuffle(result)

    return result
