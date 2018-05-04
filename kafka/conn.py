from __future__ import absolute_import, division

import collections
import copy
import errno
import io
import logging
from random import shuffle, uniform

# selectors in stdlib as of py3.4
try:
    import selectors  # pylint: disable=import-error
except ImportError:
    # vendored backport module
    from kafka.vendor import selectors34 as selectors

import socket
import struct
import sys
import time

from kafka.vendor import six

import kafka.errors as Errors
from kafka.future import Future
from kafka.metrics.stats import Avg, Count, Max, Rate
from kafka.protocol.admin import SaslHandShakeRequest
from kafka.protocol.commit import OffsetFetchRequest
from kafka.protocol.metadata import MetadataRequest
from kafka.protocol.parser import KafkaProtocol
from kafka.protocol.types import Int32, Int8
from kafka.version import __version__


if six.PY2:
    ConnectionError = socket.error
    BlockingIOError = Exception

log = logging.getLogger(__name__)

DEFAULT_KAFKA_PORT = 9092

SASL_QOP_AUTH = 1
SASL_QOP_AUTH_INT = 2
SASL_QOP_AUTH_CONF = 4

try:
    import ssl
    ssl_available = True
    try:
        SSLEOFError = ssl.SSLEOFError
        SSLWantReadError = ssl.SSLWantReadError
        SSLWantWriteError = ssl.SSLWantWriteError
        SSLZeroReturnError = ssl.SSLZeroReturnError
    except AttributeError:
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


AFI_NAMES = {
    socket.AF_UNSPEC: "unspecified",
    socket.AF_INET: "IPv4",
    socket.AF_INET6: "IPv6",
}


class ConnectionStates(object):
    DISCONNECTING = '<disconnecting>'
    DISCONNECTED = '<disconnected>'
    CONNECTING = '<connecting>'
    HANDSHAKE = '<handshake>'
    CONNECTED = '<connected>'
    AUTHENTICATING = '<authenticating>'


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
        selector (selectors.BaseSelector): Provide a specific selector
            implementation to use for I/O multiplexing.
            Default: selectors.DefaultSelector
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
        'sock_chunk_bytes': 4096,  # undocumented experimental option
        'sock_chunk_buffer_count': 1000,  # undocumented experimental option
        'security_protocol': 'PLAINTEXT',
        'ssl_context': None,
        'ssl_check_hostname': True,
        'ssl_cafile': None,
        'ssl_certfile': None,
        'ssl_keyfile': None,
        'ssl_crlfile': None,
        'ssl_password': None,
        'api_version': (0, 8, 2),  # default to most restrictive
        'selector': selectors.DefaultSelector,
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
        self.host = host
        self.port = port
        self.afi = afi
        self._sock_afi = afi
        self._sock_addr = None
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

        self._protocol = KafkaProtocol(
            client_id=self.config['client_id'],
            api_version=self.config['api_version'])
        self.state = ConnectionStates.DISCONNECTED
        self._reset_reconnect_backoff()
        self._sock = None
        self._ssl_context = None
        if self.config['ssl_context'] is not None:
            self._ssl_context = self.config['ssl_context']
        self._sasl_auth_future = None
        self.last_attempt = 0
        self._gai = []
        self._sensors = None
        if self.config['metrics']:
            self._sensors = BrokerConnectionMetrics(self.config['metrics'],
                                                    self.config['metric_group_prefix'],
                                                    self.node_id)

    def _dns_lookup(self):
        self._gai = dns_lookup(self.host, self.port, self.afi)
        if not self._gai:
            log.error('DNS lookup failed for %s:%i (%s)',
                      self.host, self.port, self.afi)
            return False
        return True

    def _next_afi_sockaddr(self):
        if not self._gai:
            if not self._dns_lookup():
                return
        afi, _, __, ___, sockaddr = self._gai.pop(0)
        return (afi, sockaddr)

    def connect_blocking(self, timeout=float('inf')):
        if self.connected():
            return True
        timeout += time.time()
        # First attempt to perform dns lookup
        # note that the underlying interface, socket.getaddrinfo,
        # has no explicit timeout so we may exceed the user-specified timeout
        while time.time() < timeout:
            if self._dns_lookup():
                break
        else:
            return False

        # Loop once over all returned dns entries
        selector = None
        while self._gai:
            while time.time() < timeout:
                self.connect()
                if self.connected():
                    if selector is not None:
                        selector.close()
                    return True
                elif self.connecting():
                    if selector is None:
                        selector = self.config['selector']()
                        selector.register(self._sock, selectors.EVENT_WRITE)
                    selector.select(1)
                elif self.disconnected():
                    if selector is not None:
                        selector.close()
                        selector = None
                    break
            else:
                break
        return False

    def connect(self):
        """Attempt to connect and return ConnectionState"""
        if self.state is ConnectionStates.DISCONNECTED and not self.blacked_out():
            self.last_attempt = time.time()
            next_lookup = self._next_afi_sockaddr()
            if not next_lookup:
                self.close(Errors.ConnectionError('DNS failure'))
                return
            else:
                log.debug('%s: creating new socket', self)
                self._sock_afi, self._sock_addr = next_lookup
                self._sock = socket.socket(self._sock_afi, socket.SOCK_STREAM)

            for option in self.config['socket_options']:
                log.debug('%s: setting socket option %s', self, option)
                self._sock.setsockopt(*option)

            self._sock.setblocking(False)
            self.state = ConnectionStates.CONNECTING
            if self.config['security_protocol'] in ('SSL', 'SASL_SSL'):
                self._wrap_ssl()
            # _wrap_ssl can alter the connection state -- disconnects on failure
            # so we need to double check that we are still connecting before
            if self.connecting():
                self.config['state_change_callback'](self)
                log.info('%s: connecting to %s:%d [%s %s]', self, self.host,
                         self.port, self._sock_addr, AFI_NAMES[self._sock_afi])

        if self.state is ConnectionStates.CONNECTING:
            # in non-blocking mode, use repeated calls to socket.connect_ex
            # to check connection status
            request_timeout = self.config['request_timeout_ms'] / 1000.0
            ret = None
            try:
                ret = self._sock.connect_ex(self._sock_addr)
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
                    log.info('%s: Connection complete.', self)
                    self.state = ConnectionStates.CONNECTED
                    self._reset_reconnect_backoff()
                self.config['state_change_callback'](self)

            # Connection failed
            # WSAEINVAL == 10022, but errno.WSAEINVAL is not available on non-win systems
            elif ret not in (errno.EINPROGRESS, errno.EALREADY, errno.EWOULDBLOCK, 10022):
                log.error('Connect attempt to %s returned error %s.'
                          ' Disconnecting.', self, ret)
                errstr = errno.errorcode.get(ret, 'UNKNOWN')
                self.close(Errors.ConnectionError('{} {}'.format(ret, errstr)))

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
                    log.info('%s: Connection complete.', self)
                    self.state = ConnectionStates.CONNECTED
                self.config['state_change_callback'](self)

        if self.state is ConnectionStates.AUTHENTICATING:
            assert self.config['security_protocol'] in ('SASL_PLAINTEXT', 'SASL_SSL')
            if self._try_authenticate():
                # _try_authenticate has side-effects: possibly disconnected on socket errors
                if self.state is ConnectionStates.AUTHENTICATING:
                    log.info('%s: Connection complete.', self)
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
                    raise RuntimeError('This version of Python does not support ssl_crlfile!')
                log.info('%s: Loading SSL CRL from %s', self, self.config['ssl_crlfile'])
                self._ssl_context.load_verify_locations(self.config['ssl_crlfile'])
                # pylint: disable=no-member
                self._ssl_context.verify_flags |= ssl.VERIFY_CRL_CHECK_LEAF
        log.debug('%s: wrapping socket in ssl context', self)
        try:
            self._sock = self._ssl_context.wrap_socket(
                self._sock,
                server_hostname=self.host,
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

        for r, f in self.recv():
            f.success(r)

        # A connection error could trigger close() which will reset the future
        if self._sasl_auth_future is None:
            return False
        elif self._sasl_auth_future.failed():
            ex = self._sasl_auth_future.exception
            if not isinstance(ex, Errors.ConnectionError):
                raise ex  # pylint: disable-msg=raising-bad-type
        return self._sasl_auth_future.succeeded()

    def _handle_sasl_handshake_response(self, future, response):
        error_type = Errors.for_code(response.error_code)
        if error_type is not Errors.NoError:
            error = error_type(self)
            self.close(error=error)
            return future.failure(error_type(self))

        if self.config['sasl_mechanism'] not in response.enabled_mechanisms:
            return future.failure(
                Errors.UnsupportedSaslMechanismError(
                    'Kafka broker does not support %s sasl mechanism. Enabled mechanisms are: %s'
                    % (self.config['sasl_mechanism'], response.enabled_mechanisms)))
        elif self.config['sasl_mechanism'] == 'PLAIN':
            return self._try_authenticate_plain(future)
        elif self.config['sasl_mechanism'] == 'GSSAPI':
            return self._try_authenticate_gssapi(future)
        else:
            return future.failure(
                Errors.UnsupportedSaslMechanismError(
                    'kafka-python does not support SASL mechanism %s' %
                    self.config['sasl_mechanism']))

    def _send_bytes_blocking(self, data):
        self._sock.settimeout(self.config['request_timeout_ms'] / 1000)
        total_sent = 0
        try:
            while total_sent < len(data):
                sent_bytes = self._sock.send(data[total_sent:])
                total_sent += sent_bytes
            if total_sent != len(data):
                raise ConnectionError('Buffer overrun during socket send')
            return total_sent
        finally:
            self._sock.settimeout(0.0)

    def _recv_bytes_blocking(self, n):
        self._sock.settimeout(self.config['request_timeout_ms'] / 1000)
        try:
            data = b''
            while len(data) < n:
                fragment = self._sock.recv(n - len(data))
                if not fragment:
                    raise ConnectionError('Connection reset during recv')
                data += fragment
            return data
        finally:
            self._sock.settimeout(0.0)

    def _try_authenticate_plain(self, future):
        if self.config['security_protocol'] == 'SASL_PLAINTEXT':
            log.warning('%s: Sending username and password in the clear', self)

        data = b''
        # Send PLAIN credentials per RFC-4616
        msg = bytes('\0'.join([self.config['sasl_plain_username'],
                               self.config['sasl_plain_username'],
                               self.config['sasl_plain_password']]).encode('utf-8'))
        size = Int32.encode(len(msg))
        try:
            self._send_bytes_blocking(size + msg)

            # The server will send a zero sized message (that is Int32(0)) on success.
            # The connection is closed on failure
            data = self._recv_bytes_blocking(4)

        except ConnectionError as e:
            log.exception("%s: Error receiving reply from server",  self)
            error = Errors.ConnectionError("%s: %s" % (self, e))
            self.close(error=error)
            return future.failure(error)

        if data != b'\x00\x00\x00\x00':
            error = Errors.AuthenticationFailedError('Unrecognized response during authentication')
            return future.failure(error)

        log.info('%s: Authenticated as %s via PLAIN', self, self.config['sasl_plain_username'])
        return future.success(True)

    def _try_authenticate_gssapi(self, future):
        auth_id = self.config['sasl_kerberos_service_name'] + '@' + self.host
        gssapi_name = gssapi.Name(
            auth_id,
            name_type=gssapi.NameType.hostbased_service
        ).canonicalize(gssapi.MechType.kerberos)
        log.debug('%s: GSSAPI name: %s', self, gssapi_name)

        # Establish security context and negotiate protection level
        # For reference RFC 2222, section 7.2.1
        try:
            # Exchange tokens until authentication either succeeds or fails
            client_ctx = gssapi.SecurityContext(name=gssapi_name, usage='initiate')
            received_token = None
            while not client_ctx.complete:
                # calculate an output token from kafka token (or None if first iteration)
                output_token = client_ctx.step(received_token)

                # pass output token to kafka, or send empty response if the security
                # context is complete (output token is None in that case)
                if output_token is None:
                    self._send_bytes_blocking(Int32.encode(0))
                else:
                    msg = output_token
                    size = Int32.encode(len(msg))
                    self._send_bytes_blocking(size + msg)

                # The server will send a token back. Processing of this token either
                # establishes a security context, or it needs further token exchange.
                # The gssapi will be able to identify the needed next step.
                # The connection is closed on failure.
                header = self._recv_bytes_blocking(4)
                (token_size,) = struct.unpack('>i', header)
                received_token = self._recv_bytes_blocking(token_size)

            # Process the security layer negotiation token, sent by the server
            # once the security context is established.

            # unwraps message containing supported protection levels and msg size
            msg = client_ctx.unwrap(received_token).message
            # Kafka currently doesn't support integrity or confidentiality security layers, so we
            # simply set QoP to 'auth' only (first octet). We reuse the max message size proposed
            # by the server
            msg = Int8.encode(SASL_QOP_AUTH & Int8.decode(io.BytesIO(msg[0:1]))) + msg[1:]
            # add authorization identity to the response, GSS-wrap and send it
            msg = client_ctx.wrap(msg + auth_id.encode(), False).message
            size = Int32.encode(len(msg))
            self._send_bytes_blocking(size + msg)

        except ConnectionError as e:
            log.exception("%s: Error receiving reply from server",  self)
            error = Errors.ConnectionError("%s: %s" % (self, e))
            self.close(error=error)
            return future.failure(error)
        except Exception as e:
            return future.failure(e)

        log.info('%s: Authenticated as %s via GSSAPI', self, gssapi_name)
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
        """
        Return the number of milliseconds to wait, based on the connection
        state, before attempting to send data. When disconnected, this respects
        the reconnect backoff time. When connecting, returns 0 to allow
        non-blocking connect to finish. When connected, returns a very large
        number to handle slow/stalled connections.
        """
        time_waited = time.time() - (self.last_attempt or 0)
        if self.state is ConnectionStates.DISCONNECTED:
            return max(self._reconnect_backoff - time_waited, 0) * 1000
        elif self.connecting():
            return 0
        else:
            return float('inf')

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
        # Do not mark as failure if there are more dns entries available to try
        if len(self._gai) > 0:
            return
        if self.config['reconnect_backoff_max_ms'] > self.config['reconnect_backoff_ms']:
            self._failures += 1
            self._reconnect_backoff = self.config['reconnect_backoff_ms'] * 2 ** (self._failures - 1)
            self._reconnect_backoff = min(self._reconnect_backoff, self.config['reconnect_backoff_max_ms'])
            self._reconnect_backoff *= uniform(0.8, 1.2)
            self._reconnect_backoff /= 1000.0
            log.debug('%s: reconnect backoff %s after %s failures', self, self._reconnect_backoff, self._failures)

    def _close_socket(self):
        if self._sock:
            self._sock.close()
            self._sock = None

    def __del__(self):
        self._close_socket()

    def close(self, error=None):
        """Close socket and fail all in-flight-requests.

        Arguments:
            error (Exception, optional): pending in-flight-requests
                will be failed with this exception.
                Default: kafka.errors.ConnectionError.
        """
        if self.state is ConnectionStates.DISCONNECTED:
            if error is not None:
                log.warning('%s: Duplicate close() with error: %s', self, error)
            return
        log.info('%s: Closing connection. %s', self, error or '')
        self.state = ConnectionStates.DISCONNECTING
        self.config['state_change_callback'](self)
        self._update_reconnect_backoff()
        self._close_socket()
        self.state = ConnectionStates.DISCONNECTED
        self._sasl_auth_future = None
        self._protocol = KafkaProtocol(
            client_id=self.config['client_id'],
            api_version=self.config['api_version'])
        if error is None:
            error = Errors.Cancelled(str(self))
        while self.in_flight_requests:
            (_, future, _) = self.in_flight_requests.popleft()
            future.failure(error)
        self.config['state_change_callback'](self)

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
        correlation_id = self._protocol.send_request(request)
        data = self._protocol.send_bytes()
        try:
            # In the future we might manage an internal write buffer
            # and send bytes asynchronously. For now, just block
            # sending each request payload
            sent_time = time.time()
            total_bytes = self._send_bytes_blocking(data)
            if self._sensors:
                self._sensors.bytes_sent.record(total_bytes)
        except ConnectionError as e:
            log.exception("Error sending %s to %s", request, self)
            error = Errors.ConnectionError("%s: %s" % (self, e))
            self.close(error=error)
            return future.failure(error)
        log.debug('%s Request %d: %s', self, correlation_id, request)

        if request.expect_response():
            ifr = (correlation_id, future, sent_time)
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

        Return list of (response, future) tuples
        """
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

        responses = self._recv()
        if not responses and self.requests_timed_out():
            log.warning('%s timed out after %s ms. Closing connection.',
                        self, self.config['request_timeout_ms'])
            self.close(error=Errors.RequestTimedOutError(
                'Request timed out after %s ms' %
                self.config['request_timeout_ms']))
            return ()

        # augment respones w/ correlation_id, future, and timestamp
        for i, response in enumerate(responses):
            (correlation_id, future, timestamp) = self.in_flight_requests.popleft()
            latency_ms = (time.time() - timestamp) * 1000
            if self._sensors:
                self._sensors.request_time.record(latency_ms)

            log.debug('%s Response %d (%s ms): %s', self, correlation_id, latency_ms, response)
            responses[i] = (response, future)

        return responses

    def _recv(self):
        """Take all available bytes from socket, return list of any responses from parser"""
        recvd = []
        while len(recvd) < self.config['sock_chunk_buffer_count']:
            try:
                data = self._sock.recv(self.config['sock_chunk_bytes'])
                # We expect socket.recv to raise an exception if there are no
                # bytes available to read from the socket in non-blocking mode.
                # but if the socket is disconnected, we will get empty data
                # without an exception raised
                if not data:
                    log.error('%s: socket disconnected', self)
                    self.close(error=Errors.ConnectionError('socket disconnected'))
                    return []
                else:
                    recvd.append(data)

            except SSLWantReadError:
                break
            except ConnectionError as e:
                if six.PY2 and e.errno == errno.EWOULDBLOCK:
                    break
                log.exception('%s: Error receiving network data'
                              ' closing socket', self)
                self.close(error=Errors.ConnectionError(e))
                return []
            except BlockingIOError:
                if six.PY3:
                    break
                raise

        recvd_data = b''.join(recvd)
        if self._sensors:
            self._sensors.bytes_received.record(len(recvd_data))

        try:
            responses = self._protocol.receive_bytes(recvd_data)
        except Errors.KafkaProtocolError as e:
            self.close(e)
            return []
        else:
            return [resp for (_, resp) in responses]  # drop correlation id

    def requests_timed_out(self):
        if self.in_flight_requests:
            (_, _, oldest_at) = self.in_flight_requests[0]
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
            ((1, 0, 0), MetadataRequest[5]),
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
        log.info('Probing node %s broker version', self.node_id)
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
        from kafka.protocol.admin import ApiVersionRequest, ListGroupsRequest
        from kafka.protocol.commit import OffsetFetchRequest, GroupCoordinatorRequest

        test_cases = [
            # All cases starting from 0.10 will be based on ApiVersionResponse
            ((0, 10), ApiVersionRequest[0]()),
            ((0, 9), ListGroupsRequest[0]()),
            ((0, 8, 2), GroupCoordinatorRequest[0]('kafka-python-default-group')),
            ((0, 8, 1), OffsetFetchRequest[0]('kafka-python-default-group', [])),
            ((0, 8, 0), MetadataRequest[0]([])),
        ]

        for version, request in test_cases:
            if not self.connect_blocking(timeout):
                raise Errors.NodeNotReadyError()
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

            selector = self.config['selector']()
            selector.register(self._sock, selectors.EVENT_READ)
            while not (f.is_done and mr.is_done):
                for response, future in self.recv():
                    future.success(response)
                selector.select(1)
            selector.close()

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

        for key in stashed:
            self.config[key] = stashed[key]
        return version

    def __str__(self):
        return "<BrokerConnection node_id=%s host=%s:%d %s [%s %s]>" % (
            self.node_id, self.host, self.port, self.state,
            AFI_NAMES[self._sock_afi], self._sock_addr)


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


def is_inet_4_or_6(gai):
    """Given a getaddrinfo struct, return True iff ipv4 or ipv6"""
    return gai[0] in (socket.AF_INET, socket.AF_INET6)


def dns_lookup(host, port, afi=socket.AF_UNSPEC):
    """Returns a list of getaddrinfo structs, optionally filtered to an afi (ipv4 / ipv6)"""
    # XXX: all DNS functions in Python are blocking. If we really
    # want to be non-blocking here, we need to use a 3rd-party
    # library like python-adns, or move resolution onto its
    # own thread. This will be subject to the default libc
    # name resolution timeout (5s on most Linux boxes)
    try:
        return list(filter(is_inet_4_or_6,
                           socket.getaddrinfo(host, port, afi,
                                              socket.SOCK_STREAM)))
    except socket.gaierror as ex:
        log.warning('DNS lookup failed for %s:%d,'
                    ' exception was %s. Is your'
                    ' advertised.listeners (called'
                    ' advertised.host.name before Kafka 9)'
                    ' correct and resolvable?',
                    host, port, ex)
        return []
