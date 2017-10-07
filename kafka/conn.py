from __future__ import absolute_import

import collections
import copy
import errno
import logging
from random import shuffle, uniform
import socket
import time
import sys

from kafka.vendor import six

import kafka.errors as Errors
from kafka.future import Future
from kafka.metrics.stats import Avg, Count, Max, Rate
from kafka.protocol.api import RequestHeader
from kafka.protocol.admin import SaslHandShakeRequest
from kafka.protocol.commit import GroupCoordinatorResponse, OffsetFetchRequest
from kafka.protocol.frame import KafkaBytes
from kafka.protocol.metadata import MetadataRequest
from kafka.protocol.types import Int32
from kafka.version import __version__


if six.PY2:
    ConnectionError = socket.error
    BlockingIOError = Exception

log = logging.getLogger(__name__)

DEFAULT_KAFKA_PORT = 9092

try:
    import ssl
    ssl_available = True
    try:
        SSLEOFError = ssl.SSLEOFError
        SSLWantReadError = ssl.SSLWantReadError
        SSLWantWriteError = ssl.SSLWantWriteError
        SSLZeroReturnError = ssl.SSLZeroReturnError
    except:
        # support older ssl libraries
        log.warning('Old SSL module detected.'
                    ' SSL error handling may not operate cleanly.'
                    ' Consider upgrading to Python 3.3 or 2.7.9')
        SSLEOFError = ssl.SSLError
        SSLWantReadError = ssl.SSLError
        SSLWantWriteError = ssl.SSLError
        SSLZeroReturnError = ssl.SSLError
except ImportError:
    # support Python without ssl libraries
    ssl_available = False
    class SSLWantReadError(Exception):
        pass
    class SSLWantWriteError(Exception):
        pass

# needed for SASL_GSSAPI authentication:
try:
    import gssapi
    from gssapi.raw.misc import GSSError
except ImportError:
    #no gssapi available, will disable gssapi mechanism
    gssapi = None
    GSSError = None

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
    """Initialize a Kafka broker connection

    Keyword Arguments:
        client_id (str): a name for this client. This string is passed in
            each request to servers and can be used to identify specific
            server-side log entries that correspond to this client. Also
            submitted to GroupCoordinator for logging with respect to
            consumer group administration. Default: 'kafka-python-{version}'
        reconnect_backoff_ms (int): The amount of time in milliseconds to
            wait before attempting to reconnect to a given host.
            Default: 50.
        reconnect_backoff_max_ms (int): The maximum amount of time in
            milliseconds to wait when reconnecting to a broker that has
            repeatedly failed to connect. If provided, the backoff per host
            will increase exponentially for each consecutive connection
            failure, up to this maximum. To avoid connection storms, a
            randomization factor of 0.2 will be applied to the backoff
            resulting in a random range between 20% below and 20% above
            the computed value. Default: 1000.
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
            Valid values are: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL.
            Default: PLAINTEXT.
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
        api_version (tuple): Specify which Kafka API version to use.
            Accepted values are: (0, 8, 0), (0, 8, 1), (0, 8, 2), (0, 9),
            (0, 10). Default: (0, 8, 2)
        api_version_auto_timeout_ms (int): number of milliseconds to throw a
            timeout exception from the constructor when checking the broker
            api version. Only applies if api_version is None
        state_change_callback (callable): function to be called when the
            connection state changes from CONNECTING to CONNECTED etc.
        metrics (kafka.metrics.Metrics): Optionally provide a metrics
            instance for capturing network IO stats. Default: None.
        metric_group_prefix (str): Prefix for metric names. Default: ''
        sasl_mechanism (str): Authentication mechanism when security_protocol
            is configured for SASL_PLAINTEXT or SASL_SSL. Valid values are:
            PLAIN, GSSAPI. Default: PLAIN
        sasl_plain_username (str): username for sasl PLAIN authentication.
            Default: None
        sasl_plain_password (str): password for sasl PLAIN authentication.
            Default: None
        sasl_kerberos_service_name (str): Service name to include in GSSAPI
            sasl mechanism handshake. Default: 'kafka'
    """

    DEFAULT_CONFIG = {
        'client_id': 'kafka-python-' + __version__,
        'node_id': 0,
        'request_timeout_ms': 40000,
        'reconnect_backoff_ms': 50,
        'reconnect_backoff_max_ms': 1000,
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
        'sasl_plain_password': None,
        'sasl_kerberos_service_name': 'kafka'
    }
    SECURITY_PROTOCOLS = ('PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL')
    SASL_MECHANISMS = ('PLAIN', 'GSSAPI')

    def __init__(self, host, port, afi, **configs):
        self.hostname = host
        self.host = host
        self.port = port
        self.afi = afi
        self._init_host = host
        self._init_port = port
        self._init_afi = afi
        self.in_flight_requests = collections.deque()
        self._api_versions = None

        self.config = copy.copy(self.DEFAULT_CONFIG)
        for key in self.config:
            if key in configs:
                self.config[key] = configs[key]

        self.node_id = self.config.pop('node_id')

        if self.config['receive_buffer_bytes'] is not None:
            self.config['socket_options'].append(
                (socket.SOL_SOCKET, socket.SO_RCVBUF,
                 self.config['receive_buffer_bytes']))
        if self.config['send_buffer_bytes'] is not None:
            self.config['socket_options'].append(
                 (socket.SOL_SOCKET, socket.SO_SNDBUF,
                 self.config['send_buffer_bytes']))

        assert self.config['security_protocol'] in self.SECURITY_PROTOCOLS, (
            'security_protcol must be in ' + ', '.join(self.SECURITY_PROTOCOLS))

        if self.config['security_protocol'] in ('SSL', 'SASL_SSL'):
            assert ssl_available, "Python wasn't built with SSL support"

        if self.config['security_protocol'] in ('SASL_PLAINTEXT', 'SASL_SSL'):
            assert self.config['sasl_mechanism'] in self.SASL_MECHANISMS, (
                'sasl_mechanism must be in ' + ', '.join(self.SASL_MECHANISMS))
            if self.config['sasl_mechanism'] == 'PLAIN':
                assert self.config['sasl_plain_username'] is not None, 'sasl_plain_username required for PLAIN sasl'
                assert self.config['sasl_plain_password'] is not None, 'sasl_plain_password required for PLAIN sasl'
            if self.config['sasl_mechanism'] == 'GSSAPI':
                assert gssapi is not None, 'GSSAPI lib not available'
                assert self.config['sasl_kerberos_service_name'] is not None, 'sasl_kerberos_service_name required for GSSAPI sasl'

        self.state = ConnectionStates.DISCONNECTED
        self._reset_reconnect_backoff()
        self._sock = None
        self._ssl_context = None
        if self.config['ssl_context'] is not None:
            self._ssl_context = self.config['ssl_context']
        self._sasl_auth_future = None
        self._header = KafkaBytes(4)
        self._rbuffer = None
        self._receiving = False
        self.last_attempt = 0
        self._processing = False
        self._correlation_id = 0
        self._gai = None
        self._gai_index = 0
        self._sensors = None
        if self.config['metrics']:
            self._sensors = BrokerConnectionMetrics(self.config['metrics'],
                                                    self.config['metric_group_prefix'],
                                                    self.node_id)

    def connect(self):
        """Attempt to connect and return ConnectionState"""
        if self.state is ConnectionStates.DISCONNECTED:
            log.debug('%s: creating new socket', self)
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
                        log.warning('DNS lookup failed for %s:%d,'
                                    ' exception was %s. Is your'
                                    ' advertised.listeners (called'
                                    ' advertised.host.name before Kafka 9)'
                                    ' correct and resolvable?',
                                    self._init_host, self._init_port, ex)
                        self._gai = []
                    self._gai_index = 0
                else:
                    # if self._gai already exists, then we should try the next
                    # name
                    self._gai_index += 1
                while True:
                    if self._gai_index >= len(self._gai):
                        error = 'Unable to connect to any of the names for {0}:{1}'.format(
                            self._init_host, self._init_port)
                        log.error(error)
                        self.close(Errors.ConnectionError(error))
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
                log.debug('%s: setting socket option %s', self, option)
                self._sock.setsockopt(*option)

            self._sock.setblocking(False)
            if self.config['security_protocol'] in ('SSL', 'SASL_SSL'):
                self._wrap_ssl()
            log.info('%s: connecting to %s:%d', self, self.host, self.port)
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
                log.debug('%s: established TCP connection', self)
                if self.config['security_protocol'] in ('SSL', 'SASL_SSL'):
                    log.debug('%s: initiating SSL handshake', self)
                    self.state = ConnectionStates.HANDSHAKE
                elif self.config['security_protocol'] == 'SASL_PLAINTEXT':
                    log.debug('%s: initiating SASL authentication', self)
                    self.state = ConnectionStates.AUTHENTICATING
                else:
                    # security_protocol PLAINTEXT
                    log.debug('%s: Connection complete.', self)
                    self.state = ConnectionStates.CONNECTED
                    self._reset_reconnect_backoff()
                self.config['state_change_callback'](self)

            # Connection failed
            # WSAEINVAL == 10022, but errno.WSAEINVAL is not available on non-win systems
            elif ret not in (errno.EINPROGRESS, errno.EALREADY, errno.EWOULDBLOCK, 10022):
                log.error('Connect attempt to %s returned error %s.'
                          ' Disconnecting.', self, ret)
                self.close(Errors.ConnectionError(ret))

            # Connection timed out
            elif time.time() > request_timeout + self.last_attempt:
                log.error('Connection attempt to %s timed out', self)
                self.close(Errors.ConnectionError('timeout'))

            # Needs retry
            else:
                pass

        if self.state is ConnectionStates.HANDSHAKE:
            if self._try_handshake():
                log.debug('%s: completed SSL handshake.', self)
                if self.config['security_protocol'] == 'SASL_SSL':
                    log.debug('%s: initiating SASL authentication', self)
                    self.state = ConnectionStates.AUTHENTICATING
                else:
                    log.debug('%s: Connection complete.', self)
                    self.state = ConnectionStates.CONNECTED
                self.config['state_change_callback'](self)

        if self.state is ConnectionStates.AUTHENTICATING:
            assert self.config['security_protocol'] in ('SASL_PLAINTEXT', 'SASL_SSL')
            if self._try_authenticate():
                log.debug('%s: Connection complete.', self)
                self.state = ConnectionStates.CONNECTED
                self._reset_reconnect_backoff()
                self.config['state_change_callback'](self)

        return self.state

    def _wrap_ssl(self):
        assert self.config['security_protocol'] in ('SSL', 'SASL_SSL')
        if self._ssl_context is None:
            log.debug('%s: configuring default SSL Context', self)
            self._ssl_context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)  # pylint: disable=no-member
            self._ssl_context.options |= ssl.OP_NO_SSLv2  # pylint: disable=no-member
            self._ssl_context.options |= ssl.OP_NO_SSLv3  # pylint: disable=no-member
            self._ssl_context.verify_mode = ssl.CERT_OPTIONAL
            if self.config['ssl_check_hostname']:
                self._ssl_context.check_hostname = True
            if self.config['ssl_cafile']:
                log.info('%s: Loading SSL CA from %s', self, self.config['ssl_cafile'])
                self._ssl_context.load_verify_locations(self.config['ssl_cafile'])
                self._ssl_context.verify_mode = ssl.CERT_REQUIRED
            if self.config['ssl_certfile'] and self.config['ssl_keyfile']:
                log.info('%s: Loading SSL Cert from %s', self, self.config['ssl_certfile'])
                log.info('%s: Loading SSL Key from %s', self, self.config['ssl_keyfile'])
                self._ssl_context.load_cert_chain(
                    certfile=self.config['ssl_certfile'],
                    keyfile=self.config['ssl_keyfile'],
                    password=self.config['ssl_password'])
            if self.config['ssl_crlfile']:
                if not hasattr(ssl, 'VERIFY_CRL_CHECK_LEAF'):
                    error = 'No CRL support with this version of Python.'
                    log.error('%s: %s Disconnecting.', self, error)
                    self.close(Errors.ConnectionError(error))
                    return
                log.info('%s: Loading SSL CRL from %s', self, self.config['ssl_crlfile'])
                self._ssl_context.load_verify_locations(self.config['ssl_crlfile'])
                # pylint: disable=no-member
                self._ssl_context.verify_flags |= ssl.VERIFY_CRL_CHECK_LEAF
        log.debug('%s: wrapping socket in ssl context', self)
        try:
            self._sock = self._ssl_context.wrap_socket(
                self._sock,
                server_hostname=self.hostname,
                do_handshake_on_connect=False)
        except ssl.SSLError as e:
            log.exception('%s: Failed to wrap socket in SSLContext!', self)
            self.close(e)

    def _try_handshake(self):
        assert self.config['security_protocol'] in ('SSL', 'SASL_SSL')
        try:
            self._sock.do_handshake()
            return True
        # old ssl in python2.6 will swallow all SSLErrors here...
        except (SSLWantReadError, SSLWantWriteError):
            pass
        except (SSLZeroReturnError, ConnectionError, SSLEOFError):
            log.warning('SSL connection closed by server during handshake.')
            self.close(Errors.ConnectionError('SSL connection closed by server during handshake'))
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
        elif self.config['sasl_mechanism'] == 'GSSAPI':
            return self._try_authenticate_gssapi(future)
        else:
            return future.failure(
                Errors.UnsupportedSaslMechanismError(
                    'kafka-python does not support SASL mechanism %s' %
                    self.config['sasl_mechanism']))

    def _try_authenticate_plain(self, future):
        if self.config['security_protocol'] == 'SASL_PLAINTEXT':
            log.warning('%s: Sending username and password in the clear', self)

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
            error = Errors.ConnectionError("%s: %s" % (self, e))
            future.failure(error)
            self.close(error=error)

        if data != b'\x00\x00\x00\x00':
            return future.failure(Errors.AuthenticationFailedError())

        log.info('%s: Authenticated as %s', self, self.config['sasl_plain_username'])
        return future.success(True)

    def _try_authenticate_gssapi(self, future):
        data = b''
        gssname = self.config['sasl_kerberos_service_name'] + '@' + self.hostname
        ctx_Name = gssapi.Name(gssname, name_type=gssapi.NameType.hostbased_service)
        ctx_CanonName = ctx_Name.canonicalize(gssapi.MechType.kerberos)
        log.debug('%s: canonical Servicename: %s', self, ctx_CanonName)
        ctx_Context = gssapi.SecurityContext(name=ctx_CanonName, usage='initiate')
        # Exchange tokens until authentication either succeeds or fails:
        received_token = None
        try:
            while not ctx_Context.complete:
                # calculate the output token
                try:
                    output_token = ctx_Context.step(received_token)
                except GSSError as e:
                    log.exception("%s: Error invalid token received from server",  self)
                    error = Errors.ConnectionError("%s: %s" % (self, e))

                if not output_token:
                    if ctx_Context.complete:
                        log.debug("%s: Security Context complete ", self)
                    log.debug("%s: Successful GSSAPI handshake for %s", self, ctx_Context.initiator_name)
                    break
                try:
                    self._sock.setblocking(True)
                    # Send output token
                    msg = output_token
                    size = Int32.encode(len(msg))
                    self._sock.sendall(size + msg)

                    # The server will send a token back. Processing of this token either
                    # establishes a security context, or it needs further token exchange.
                    # The gssapi will be able to identify the needed next step.
                    # The connection is closed on failure.
                    response = self._sock.recv(2000)
                    self._sock.setblocking(False)

                except (AssertionError, ConnectionError) as e:
                    log.exception("%s: Error receiving reply from server",  self)
                    error = Errors.ConnectionError("%s: %s" % (self, e))
                    future.failure(error)
                    self.close(error=error)

                # pass the received token back to gssapi, strip the first 4 bytes
                received_token = response[4:]

        except Exception as e:
            log.exception("%s: GSSAPI handshake error",  self)
            error = Errors.ConnectionError("%s: %s" % (self, e))
            future.failure(error)
            self.close(error=error)

        log.info('%s: Authenticated as %s', self, gssname)
        return future.success(True)

    def blacked_out(self):
        """
        Return true if we are disconnected from the given node and can't
        re-establish a connection yet
        """
        if self.state is ConnectionStates.DISCONNECTED:
            if time.time() < self.last_attempt + self._reconnect_backoff:
                return True
        return False

    def connection_delay(self):
        time_waited_ms = time.time() - (self.last_attempt or 0)
        if self.state is ConnectionStates.DISCONNECTED:
            return max(self._reconnect_backoff - time_waited_ms, 0)
        elif self.connecting():
            return 0
        else:
            return 999999999

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

    def _reset_reconnect_backoff(self):
        self._failures = 0
        self._reconnect_backoff = self.config['reconnect_backoff_ms'] / 1000.0

    def _update_reconnect_backoff(self):
        if self.config['reconnect_backoff_max_ms'] > self.config['reconnect_backoff_ms']:
            self._failures += 1
            self._reconnect_backoff = self.config['reconnect_backoff_ms'] * 2 ** (self._failures - 1)
            self._reconnect_backoff = min(self._reconnect_backoff, self.config['reconnect_backoff_max_ms'])
            self._reconnect_backoff *= uniform(0.8, 1.2)
            self._reconnect_backoff /= 1000.0
            log.debug('%s: reconnect backoff %s after %s failures', self, self._reconnect_backoff, self._failures)

    def close(self, error=None):
        """Close socket and fail all in-flight-requests.

        Arguments:
            error (Exception, optional): pending in-flight-requests
                will be failed with this exception.
                Default: kafka.errors.ConnectionError.
        """
        if self.state is ConnectionStates.DISCONNECTED:
            if error is not None:
                if sys.version_info >= (3, 2):
                    log.warning('%s: close() called on disconnected connection with error: %s', self, error, stack_info=True)
                else:
                    log.warning('%s: close() called on disconnected connection with error: %s', self, error)
            return

        log.info('%s: Closing connection. %s', self, error or '')
        self.state = ConnectionStates.DISCONNECTING
        self.config['state_change_callback'](self)
        self._update_reconnect_backoff()
        if self._sock:
            self._sock.close()
            self._sock = None
        self.state = ConnectionStates.DISCONNECTED
        self.last_attempt = time.time()
        self._sasl_auth_future = None
        self._reset_buffer()
        if error is None:
            error = Errors.Cancelled(str(self))
        while self.in_flight_requests:
            ifr = self.in_flight_requests.popleft()
            ifr.future.failure(error)
        self.config['state_change_callback'](self)

    def _reset_buffer(self):
        self._receiving = False
        self._header.seek(0)
        self._rbuffer = None

    def send(self, request):
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
        return self._send(request)

    def _send(self, request):
        assert self.state in (ConnectionStates.AUTHENTICATING, ConnectionStates.CONNECTED)
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
            error = Errors.ConnectionError("%s: %s" % (self, e))
            self.close(error=error)
            return future.failure(error)
        log.debug('%s Request %d: %s', self, correlation_id, request)

        if request.expect_response():
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
                self.close(Errors.ConnectionError('Socket not connected during recv with in-flight-requests'))
            return ()

        elif not self.in_flight_requests:
            log.warning('%s: No in-flight-requests to recv', self)
            return ()

        response = self._recv()
        if not response and self.requests_timed_out():
            log.warning('%s timed out after %s ms. Closing connection.',
                        self, self.config['request_timeout_ms'])
            self.close(error=Errors.RequestTimedOutError(
                'Request timed out after %s ms' %
                self.config['request_timeout_ms']))
            return ()
        return response

    def _recv(self):
        responses = []
        SOCK_CHUNK_BYTES = 4096
        while True:
            try:
                data = self._sock.recv(SOCK_CHUNK_BYTES)
                # We expect socket.recv to raise an exception if there is not
                # enough data to read the full bytes_to_read
                # but if the socket is disconnected, we will get empty data
                # without an exception raised
                if not data:
                    log.error('%s: socket disconnected', self)
                    self.close(error=Errors.ConnectionError('socket disconnected'))
                    break
                else:
                    responses.extend(self.receive_bytes(data))
                    if len(data) < SOCK_CHUNK_BYTES:
                        break
            except SSLWantReadError:
                break
            except ConnectionError as e:
                if six.PY2 and e.errno == errno.EWOULDBLOCK:
                    break
                log.exception('%s: Error receiving network data'
                              ' closing socket', self)
                self.close(error=Errors.ConnectionError(e))
                break
            except BlockingIOError:
                if six.PY3:
                    break
                raise
        return responses

    def receive_bytes(self, data):
        i = 0
        n = len(data)
        responses = []
        if self._sensors:
            self._sensors.bytes_received.record(n)
        while i < n:

            # Not receiving is the state of reading the payload header
            if not self._receiving:
                bytes_to_read = min(4 - self._header.tell(), n - i)
                self._header.write(data[i:i+bytes_to_read])
                i += bytes_to_read

                if self._header.tell() == 4:
                    self._header.seek(0)
                    nbytes = Int32.decode(self._header)
                    # reset buffer and switch state to receiving payload bytes
                    self._rbuffer = KafkaBytes(nbytes)
                    self._receiving = True
                elif self._header.tell() > 4:
                    raise Errors.KafkaError('this should not happen - are you threading?')


            if self._receiving:
                total_bytes = len(self._rbuffer)
                staged_bytes = self._rbuffer.tell()
                bytes_to_read = min(total_bytes - staged_bytes, n - i)
                self._rbuffer.write(data[i:i+bytes_to_read])
                i += bytes_to_read

                staged_bytes = self._rbuffer.tell()
                if staged_bytes > total_bytes:
                    self.close(error=Errors.KafkaError('Receive buffer has more bytes than expected?'))

                if staged_bytes != total_bytes:
                    break

                self._receiving = False
                self._rbuffer.seek(0)
                resp = self._process_response(self._rbuffer)
                if resp is not None:
                    responses.append(resp)
                self._reset_buffer()
        return responses

    def _process_response(self, read_buffer):
        assert not self._processing, 'Recursion not supported'
        self._processing = True
        recv_correlation_id = Int32.decode(read_buffer)

        if not self.in_flight_requests:
            error = Errors.CorrelationIdError(
                '%s: No in-flight-request found for server response'
                ' with correlation ID %d'
                % (self, recv_correlation_id))
            self.close(error)
            self._processing = False
            return None
        else:
            ifr = self.in_flight_requests.popleft()

        if self._sensors:
            self._sensors.request_time.record((time.time() - ifr.timestamp) * 1000)

        # verify send/recv correlation ids match

        # 0.8.2 quirk
        if (self.config['api_version'] == (0, 8, 2) and
            ifr.response_type is GroupCoordinatorResponse[0] and
            ifr.correlation_id != 0 and
            recv_correlation_id == 0):
            log.warning('Kafka 0.8.2 quirk -- GroupCoordinatorResponse'
                        ' Correlation ID does not match request. This'
                        ' should go away once at least one topic has been'
                        ' initialized on the broker.')

        elif ifr.correlation_id != recv_correlation_id:
            error = Errors.CorrelationIdError(
                '%s: Correlation IDs do not match: sent %d, recv %d'
                % (self, ifr.correlation_id, recv_correlation_id))
            ifr.future.failure(error)
            self.close(error)
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
            error = Errors.UnknownError('Unable to decode response')
            ifr.future.failure(error)
            self.close(error)
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

    def _handle_api_version_response(self, response):
        error_type = Errors.for_code(response.error_code)
        assert error_type is Errors.NoError, "API version check failed"
        self._api_versions = dict([
            (api_key, (min_version, max_version))
            for api_key, min_version, max_version in response.api_versions
        ])
        return self._api_versions

    def _infer_broker_version_from_api_versions(self, api_versions):
        # The logic here is to check the list of supported request versions
        # in reverse order. As soon as we find one that works, return it
        test_cases = [
            # format (<broker version>, <needed struct>)
            ((0, 11, 0), MetadataRequest[4]),
            ((0, 10, 2), OffsetFetchRequest[2]),
            ((0, 10, 1), MetadataRequest[2]),
        ]

        # Get the best match of test cases
        for broker_version, struct in sorted(test_cases, reverse=True):
            if struct.API_KEY not in api_versions:
                continue
            min_version, max_version = api_versions[struct.API_KEY]
            if min_version <= struct.API_VERSION <= max_version:
                return broker_version

        # We know that ApiVersionResponse is only supported in 0.10+
        # so if all else fails, choose that
        return (0, 10, 0)

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

        # kafka kills the connection when it doesn't recognize an API request
        # so we can send a test request and then follow immediately with a
        # vanilla MetadataRequest. If the server did not recognize the first
        # request, both will be failed with a ConnectionError that wraps
        # socket.error (32, 54, or 104)
        from .protocol.admin import ApiVersionRequest, ListGroupsRequest
        from .protocol.commit import OffsetFetchRequest, GroupCoordinatorRequest

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
            # All cases starting from 0.10 will be based on ApiVersionResponse
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
                if isinstance(request, ApiVersionRequest[0]):
                    # Starting from 0.10 kafka broker we determine version
                    # by looking at ApiVersionResponse
                    api_versions = self._handle_api_version_response(f.value)
                    version = self._infer_broker_version_from_api_versions(api_versions)
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
        return "<BrokerConnection node_id=%s host=%s/%s port=%d>" % (
            self.node_id, self.hostname, self.host, self.port)


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
                            ' consider `pip install win_inet_pton`')
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
