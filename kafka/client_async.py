from __future__ import absolute_import, division

import collections
import copy
import logging
import random
import socket
import threading
import time
import weakref

# selectors in stdlib as of py3.4
try:
    import selectors  # pylint: disable=import-error
except ImportError:
    # vendored backport module
    from kafka.vendor import selectors34 as selectors

from kafka.vendor import six

from kafka.cluster import ClusterMetadata
from kafka.conn import BrokerConnection, ConnectionStates, get_ip_port_afi
from kafka import errors as Errors
from kafka.future import Future
from kafka.metrics import AnonMeasurable
from kafka.metrics.stats import Avg, Count, Rate
from kafka.metrics.stats.rate import TimeUnit
from kafka.protocol.broker_api_versions import BROKER_API_VERSIONS
from kafka.protocol.metadata import MetadataRequest
from kafka.util import Dict, WeakMethod
# Although this looks unused, it actually monkey-patches socket.socketpair()
# and should be left in as long as we're using socket.socketpair() in this file
from kafka.vendor import socketpair # noqa: F401
from kafka.version import __version__

if six.PY2:
    ConnectionError = None


log = logging.getLogger('kafka.client')


class KafkaClient(object):
    """
    A network client for asynchronous request/response network I/O.

    This is an internal class used to implement the user-facing producer and
    consumer clients.

    This class is not thread-safe!

    Attributes:
        cluster (:any:`ClusterMetadata`): Local cache of cluster metadata, retrieved
            via MetadataRequests during :meth:`~kafka.KafkaClient.poll`.

    Keyword Arguments:
        bootstrap_servers: 'host[:port]' string (or list of 'host[:port]'
            strings) that the client should contact to bootstrap initial
            cluster metadata. This does not have to be the full node list.
            It just needs to have at least one broker that will respond to a
            Metadata API Request. Default port is 9092. If no servers are
            specified, will default to localhost:9092.
        client_id (str): a name for this client. This string is passed in
            each request to servers and can be used to identify specific
            server-side log entries that correspond to this client. Also
            submitted to GroupCoordinator for logging with respect to
            consumer group administration. Default: 'kafka-python-{version}'
        reconnect_backoff_ms (int): The amount of time in milliseconds to
            wait before attempting to reconnect to a given host.
            Default: 50.
        reconnect_backoff_max_ms (int): The maximum amount of time in
            milliseconds to backoff/wait when reconnecting to a broker that has
            repeatedly failed to connect. If provided, the backoff per host
            will increase exponentially for each consecutive connection
            failure, up to this maximum. Once the maximum is reached,
            reconnection attempts will continue periodically with this fixed
            rate. To avoid connection storms, a randomization factor of 0.2
            will be applied to the backoff resulting in a random range between
            20% below and 20% above the computed value. Default: 30000.
        request_timeout_ms (int): Client request timeout in milliseconds.
            Default: 30000.
        connections_max_idle_ms: Close idle connections after the number of
            milliseconds specified by this config. The broker closes idle
            connections after connections.max.idle.ms, so this avoids hitting
            unexpected socket disconnected errors on the client.
            Default: 540000
        retry_backoff_ms (int): Milliseconds to backoff when retrying on
            errors. Default: 100.
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
        metadata_max_age_ms (int): The period of time in milliseconds after
            which we force a refresh of metadata even if we haven't seen any
            partition leadership changes to proactively discover any new
            brokers or partitions. Default: 300000
        allow_auto_create_topics (bool): Enable/disable auto topic creation
            on metadata request. Only available with api_version >= (0, 11).
            Default: True
        security_protocol (str): Protocol used to communicate with brokers.
            Valid values are: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL.
            Default: PLAINTEXT.
        ssl_context (ssl.SSLContext): Pre-configured SSLContext for wrapping
            socket connections. If provided, all other ssl_* configurations
            will be ignored. Default: None.
        ssl_check_hostname (bool): Flag to configure whether SSL handshake
            should verify that the certificate matches the broker's hostname.
            Default: True.
        ssl_cafile (str): Optional filename of CA file to use in certificate
            verification. Default: None.
        ssl_certfile (str): Optional filename of file in PEM format containing
            the client certificate, as well as any CA certificates needed to
            establish the certificate's authenticity. Default: None.
        ssl_keyfile (str): Optional filename containing the client private key.
            Default: None.
        ssl_password (str): Optional password to be used when loading the
            certificate chain. Default: None.
        ssl_crlfile (str): Optional filename containing the CRL to check for
            certificate expiration. By default, no CRL check is done. When
            providing a file, only the leaf certificate will be checked against
            this CRL. The CRL can only be checked with Python 3.4+ or 2.7.9+.
            Default: None.
        ssl_ciphers (str): optionally set the available ciphers for ssl
            connections. It should be a string in the OpenSSL cipher list
            format. If no cipher can be selected (because compile-time options
            or other configuration forbids use of all the specified ciphers),
            an ssl.SSLError will be raised. See ssl.SSLContext.set_ciphers
        api_version (tuple): Specify which Kafka API version to use. If set to
            None, the client will attempt to determine the broker version via
            ApiVersionsRequest API or, for brokers earlier than 0.10, probing
            various known APIs. Dynamic version checking is performed eagerly
            during __init__ and can raise NoBrokersAvailableError if no connection
            was made before timeout (see api_version_auto_timeout_ms below).
            Different versions enable different functionality.

            Examples:
                (3, 9) most recent broker release, enable all supported features
                (0, 10, 0) enables sasl authentication
                (0, 8, 0) enables basic functionality only

            Default: None
        api_version_auto_timeout_ms (int): number of milliseconds to throw a
            timeout exception from the constructor when checking the broker
            api version. Only applies if api_version set to None.
            Default: 2000
        selector (selectors.BaseSelector): Provide a specific selector
            implementation to use for I/O multiplexing.
            Default: selectors.DefaultSelector
        metrics (kafka.metrics.Metrics): Optionally provide a metrics
            instance for capturing network IO stats. Default: None.
        metric_group_prefix (str): Prefix for metric names. Default: ''
        sasl_mechanism (str): Authentication mechanism when security_protocol
            is configured for SASL_PLAINTEXT or SASL_SSL. Valid values are:
            PLAIN, GSSAPI, OAUTHBEARER, SCRAM-SHA-256, SCRAM-SHA-512.
        sasl_plain_username (str): username for sasl PLAIN and SCRAM authentication.
            Required if sasl_mechanism is PLAIN or one of the SCRAM mechanisms.
        sasl_plain_password (str): password for sasl PLAIN and SCRAM authentication.
            Required if sasl_mechanism is PLAIN or one of the SCRAM mechanisms.
        sasl_kerberos_name (str or gssapi.Name): Constructed gssapi.Name for use with
            sasl mechanism handshake. If provided, sasl_kerberos_service_name and
            sasl_kerberos_domain name are ignored. Default: None.
        sasl_kerberos_service_name (str): Service name to include in GSSAPI
            sasl mechanism handshake. Default: 'kafka'
        sasl_kerberos_domain_name (str): kerberos domain name to use in GSSAPI
            sasl mechanism handshake. Default: one of bootstrap servers
        sasl_oauth_token_provider (kafka.sasl.oauth.AbstractTokenProvider): OAuthBearer
            token provider instance. Default: None
        socks5_proxy (str): Socks5 proxy URL. Default: None
    """

    DEFAULT_CONFIG = {
        'bootstrap_servers': 'localhost',
        'bootstrap_topics_filter': set(),
        'client_id': 'kafka-python-' + __version__,
        'request_timeout_ms': 30000,
        'wakeup_timeout_ms': 3000,
        'connections_max_idle_ms': 9 * 60 * 1000,
        'reconnect_backoff_ms': 50,
        'reconnect_backoff_max_ms': 30000,
        'max_in_flight_requests_per_connection': 5,
        'receive_buffer_bytes': None,
        'send_buffer_bytes': None,
        'socket_options': [(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)],
        'sock_chunk_bytes': 4096,  # undocumented experimental option
        'sock_chunk_buffer_count': 1000,  # undocumented experimental option
        'retry_backoff_ms': 100,
        'allow_auto_create_topics': True,
        'metadata_max_age_ms': 300000,
        'security_protocol': 'PLAINTEXT',
        'ssl_context': None,
        'ssl_check_hostname': True,
        'ssl_cafile': None,
        'ssl_certfile': None,
        'ssl_keyfile': None,
        'ssl_password': None,
        'ssl_crlfile': None,
        'ssl_ciphers': None,
        'api_version': None,
        'api_version_auto_timeout_ms': 2000,
        'selector': selectors.DefaultSelector,
        'metrics': None,
        'metric_group_prefix': '',
        'sasl_mechanism': None,
        'sasl_plain_username': None,
        'sasl_plain_password': None,
        'sasl_kerberos_name': None,
        'sasl_kerberos_service_name': 'kafka',
        'sasl_kerberos_domain_name': None,
        'sasl_oauth_token_provider': None,
        'socks5_proxy': None,
    }

    def __init__(self, **configs):
        self.config = copy.copy(self.DEFAULT_CONFIG)
        for key in self.config:
            if key in configs:
                self.config[key] = configs[key]

        # these properties need to be set on top of the initialization pipeline
        # because they are used when __del__ method is called
        self._closed = False
        self._selector = self.config['selector']()
        self._init_wakeup_socketpair()
        self._wake_lock = threading.Lock()

        self.cluster = ClusterMetadata(**self.config)
        self._topics = set()  # empty set will fetch all topic metadata
        self._metadata_refresh_in_progress = False
        self._conns = Dict()  # object to support weakrefs
        self._api_versions = None
        self._connecting = set()
        self._sending = set()

        # Not currently used, but data is collected internally
        self._last_bootstrap = 0
        self._bootstrap_fails = 0

        self._lock = threading.RLock()

        # when requests complete, they are transferred to this queue prior to
        # invocation. The purpose is to avoid invoking them while holding the
        # lock above.
        self._pending_completion = collections.deque()

        self._idle_expiry_manager = IdleConnectionManager(self.config['connections_max_idle_ms'])
        self._sensors = None
        if self.config['metrics']:
            self._sensors = KafkaClientMetrics(self.config['metrics'],
                                               self.config['metric_group_prefix'],
                                               weakref.proxy(self._conns))

        # Check Broker Version if not set explicitly
        if self.config['api_version'] is None:
            self.config['api_version'] = self.check_version()
        elif self.config['api_version'] in BROKER_API_VERSIONS:
            self._api_versions = BROKER_API_VERSIONS[self.config['api_version']]
        elif (self.config['api_version'] + (0,)) in BROKER_API_VERSIONS:
            log.warning('Configured api_version %s is ambiguous; using %s',
                        self.config['api_version'], self.config['api_version'] + (0,))
            self.config['api_version'] = self.config['api_version'] + (0,)
            self._api_versions = BROKER_API_VERSIONS[self.config['api_version']]
        else:
            compatible_version = None
            for v in sorted(BROKER_API_VERSIONS.keys(), reverse=True):
                if v <= self.config['api_version']:
                    compatible_version = v
                    break
            if compatible_version:
                log.warning('Configured api_version %s not supported; using %s',
                            self.config['api_version'], compatible_version)
                self._api_versions = BROKER_API_VERSIONS[compatible_version]
            else:
                raise Errors.UnrecognizedBrokerVersion(self.config['api_version'])

    def _init_wakeup_socketpair(self):
        self._wake_r, self._wake_w = socket.socketpair()
        self._wake_r.setblocking(False)
        self._wake_w.settimeout(self.config['wakeup_timeout_ms'] / 1000.0)
        self._waking = False
        self._selector.register(self._wake_r, selectors.EVENT_READ)

    def _close_wakeup_socketpair(self):
        if self._wake_r is not None:
            try:
                self._selector.unregister(self._wake_r)
            except (KeyError, ValueError, TypeError):
                pass
            self._wake_r.close()
        if self._wake_w is not None:
            self._wake_w.close()
        self._wake_r = None
        self._wake_w = None

    def _can_connect(self, node_id):
        if node_id not in self._conns:
            if self.cluster.broker_metadata(node_id):
                return True
            return False
        conn = self._conns[node_id]
        return conn.disconnected() and not conn.blacked_out()

    def _conn_state_change(self, node_id, sock, conn):
        with self._lock:
            if conn.state is ConnectionStates.CONNECTING:
                # SSL connections can enter this state 2x (second during Handshake)
                if node_id not in self._connecting:
                    self._connecting.add(node_id)
                try:
                    self._selector.register(sock, selectors.EVENT_WRITE, conn)
                except KeyError:
                    self._selector.modify(sock, selectors.EVENT_WRITE, conn)

                if self.cluster.is_bootstrap(node_id):
                    self._last_bootstrap = time.time()

            elif conn.state is ConnectionStates.API_VERSIONS_SEND:
                try:
                    self._selector.register(sock, selectors.EVENT_WRITE, conn)
                except KeyError:
                    self._selector.modify(sock, selectors.EVENT_WRITE, conn)

            elif conn.state in (ConnectionStates.API_VERSIONS_RECV, ConnectionStates.AUTHENTICATING):
                try:
                    self._selector.register(sock, selectors.EVENT_READ, conn)
                except KeyError:
                    self._selector.modify(sock, selectors.EVENT_READ, conn)

            elif conn.state is ConnectionStates.CONNECTED:
                log.debug("Node %s connected", node_id)
                if node_id in self._connecting:
                    self._connecting.remove(node_id)

                try:
                    self._selector.modify(sock, selectors.EVENT_READ, conn)
                except KeyError:
                    self._selector.register(sock, selectors.EVENT_READ, conn)

                if self._sensors:
                    self._sensors.connection_created.record()

                self._idle_expiry_manager.update(node_id)

                if self.cluster.is_bootstrap(node_id):
                    self._bootstrap_fails = 0
                    if self._api_versions is None:
                        self._api_versions = conn._api_versions

                else:
                    for node_id in list(self._conns.keys()):
                        if self.cluster.is_bootstrap(node_id):
                            self._conns.pop(node_id).close()

            # Connection failures imply that our metadata is stale, so let's refresh
            elif conn.state is ConnectionStates.DISCONNECTED:
                if node_id in self._connecting:
                    self._connecting.remove(node_id)
                try:
                    self._selector.unregister(sock)
                except KeyError:
                    pass

                if self._sensors:
                    self._sensors.connection_closed.record()

                idle_disconnect = False
                if self._idle_expiry_manager.is_expired(node_id):
                    idle_disconnect = True
                self._idle_expiry_manager.remove(node_id)

                # If the connection has already by popped from self._conns,
                # we can assume the disconnect was intentional and not a failure
                if node_id not in self._conns:
                    pass

                elif self.cluster.is_bootstrap(node_id):
                    self._bootstrap_fails += 1

                elif conn.connect_failed() and not self._closed and not idle_disconnect:
                    log.warning("Node %s connection failed -- refreshing metadata", node_id)
                    self.cluster.request_update()

    def maybe_connect(self, node_id, wakeup=True):
        """Queues a node for asynchronous connection during the next .poll()"""
        if self._can_connect(node_id):
            self._connecting.add(node_id)
            # Wakeup signal is useful in case another thread is
            # blocked waiting for incoming network traffic while holding
            # the client lock in poll().
            if wakeup:
                self.wakeup()
            return True
        return False

    def _should_recycle_connection(self, conn):
        # Never recycle unless disconnected
        if not conn.disconnected():
            return False

        # Otherwise, only recycle when broker metadata has changed
        broker = self.cluster.broker_metadata(conn.node_id)
        if broker is None:
            return False

        host, _, _ = get_ip_port_afi(broker.host)
        if conn.host != host or conn.port != broker.port:
            log.info("Broker metadata change detected for node %s"
                     " from %s:%s to %s:%s", conn.node_id, conn.host, conn.port,
                     broker.host, broker.port)
            return True

        return False

    def _init_connect(self, node_id):
        """Idempotent non-blocking connection attempt to the given node id.

        Returns True if connection object exists and is connected / connecting
        """
        with self._lock:
            conn = self._conns.get(node_id)

            # Check if existing connection should be recreated because host/port changed
            if conn is not None and self._should_recycle_connection(conn):
                self._conns.pop(node_id).close()
                conn = None

            if conn is None:
                broker = self.cluster.broker_metadata(node_id)
                if broker is None:
                    log.debug('Broker id %s not in current metadata', node_id)
                    return False

                log.debug("Initiating connection to node %s at %s:%s",
                          node_id, broker.host, broker.port)
                host, port, afi = get_ip_port_afi(broker.host)
                cb = WeakMethod(self._conn_state_change)
                conn = BrokerConnection(host, broker.port, afi,
                                        state_change_callback=cb,
                                        node_id=node_id,
                                        **self.config)
                self._conns[node_id] = conn

            if conn.disconnected():
                conn.connect()
            return not conn.disconnected()

    def ready(self, node_id, metadata_priority=True):
        """Check whether a node is connected and ok to send more requests.

        Arguments:
            node_id (int): the id of the node to check
            metadata_priority (bool): Mark node as not-ready if a metadata
                refresh is required. Default: True

        Returns:
            bool: True if we are ready to send to the given node
        """
        self.maybe_connect(node_id)
        return self.is_ready(node_id, metadata_priority=metadata_priority)

    def connected(self, node_id):
        """Return True iff the node_id is connected."""
        conn = self._conns.get(node_id)
        if conn is None:
            return False
        return conn.connected()

    def _close(self):
        if not self._closed:
            self._closed = True
            self._close_wakeup_socketpair()
            self._selector.close()

    def close(self, node_id=None):
        """Close one or all broker connections.

        Arguments:
            node_id (int, optional): the id of the node to close
        """
        with self._lock:
            if node_id is None:
                self._close()
                conns = list(self._conns.values())
                self._conns.clear()
                for conn in conns:
                    conn.close()
            elif node_id in self._conns:
                self._conns.pop(node_id).close()
            else:
                log.warning("Node %s not found in current connection list; skipping", node_id)
                return

    def __del__(self):
        self._close()

    def is_disconnected(self, node_id):
        """Check whether the node connection has been disconnected or failed.

        A disconnected node has either been closed or has failed. Connection
        failures are usually transient and can be resumed in the next ready()
        call, but there are cases where transient failures need to be caught
        and re-acted upon.

        Arguments:
            node_id (int): the id of the node to check

        Returns:
            bool: True iff the node exists and is disconnected
        """
        conn = self._conns.get(node_id)
        if conn is None:
            return False
        return conn.disconnected()

    def connection_delay(self, node_id):
        """
        Return the number of milliseconds to wait, based on the connection
        state, before attempting to send data. When connecting or disconnected,
        this respects the reconnect backoff time. When connected, returns a very large
        number to handle slow/stalled connections.

        Arguments:
            node_id (int): The id of the node to check

        Returns:
            int: The number of milliseconds to wait.
        """
        conn = self._conns.get(node_id)
        if conn is None:
            return 0
        return conn.connection_delay()

    def throttle_delay(self, node_id):
        """
        Return the number of milliseconds to wait until a broker is no longer throttled.
        When disconnected / connecting, returns 0.
        """
        conn = self._conns.get(node_id)
        if conn is None:
            return 0
        return conn.throttle_delay()

    def is_ready(self, node_id, metadata_priority=True):
        """Check whether a node is ready to send more requests.

        In addition to connection-level checks, this method also is used to
        block additional requests from being sent during a metadata refresh.

        Arguments:
            node_id (int): id of the node to check
            metadata_priority (bool): Mark node as not-ready if a metadata
                refresh is required. Default: True

        Returns:
            bool: True if the node is ready and metadata is not refreshing
        """
        if not self._can_send_request(node_id):
            return False

        # if we need to update our metadata now declare all requests unready to
        # make metadata requests first priority
        if metadata_priority:
            if self._metadata_refresh_in_progress:
                return False
            if self.cluster.ttl() == 0:
                return False
        return True

    def _can_send_request(self, node_id):
        conn = self._conns.get(node_id)
        if not conn:
            return False
        return conn.connected() and conn.can_send_more()

    def send(self, node_id, request, wakeup=True, request_timeout_ms=None):
        """Send a request to a specific node. Bytes are placed on an
        internal per-connection send-queue. Actual network I/O will be
        triggered in a subsequent call to .poll()

        Arguments:
            node_id (int): destination node
            request (Struct): request object (not-encoded)

        Keyword Arguments:
            wakeup (bool, optional): optional flag to disable thread-wakeup.
            request_timeout_ms (int, optional): Provide custom timeout in milliseconds.
                If response is not processed before timeout, client will fail the
                request and close the connection.
                Default: None (uses value from client configuration)

        Raises:
            AssertionError: if node_id is not in current cluster metadata

        Returns:
            Future: resolves to Response struct or Error
        """
        conn = self._conns.get(node_id)
        if not conn or not self._can_send_request(node_id):
            self.maybe_connect(node_id, wakeup=wakeup)
            return Future().failure(Errors.NodeNotReadyError(node_id))

        # conn.send will queue the request internally
        # we will need to call send_pending_requests()
        # to trigger network I/O
        future = conn.send(request, blocking=False, request_timeout_ms=request_timeout_ms)
        if not future.is_done:
            self._sending.add(conn)

        # Wakeup signal is useful in case another thread is
        # blocked waiting for incoming network traffic while holding
        # the client lock in poll().
        if wakeup:
            self.wakeup()

        return future

    def poll(self, timeout_ms=None, future=None):
        """Try to read and write to sockets.

        This method will also attempt to complete node connections, refresh
        stale metadata, and run previously-scheduled tasks.

        Arguments:
            timeout_ms (int, optional): maximum amount of time to wait (in ms)
                for at least one response. Must be non-negative. The actual
                timeout will be the minimum of timeout, request timeout and
                metadata timeout. Default: request_timeout_ms
            future (Future, optional): if provided, blocks until future.is_done

        Returns:
            list: responses received (can be empty)
        """
        if not isinstance(timeout_ms, (int, float, type(None))):
            raise TypeError('Invalid type for timeout: %s' % type(timeout_ms))

        begin = time.time()
        if timeout_ms is not None:
            timeout_at = begin + (timeout_ms / 1000)
        else:
            timeout_at = begin + (self.config['request_timeout_ms'] / 1000)
        # Loop for futures, break after first loop if None
        responses = []
        while True:
            with self._lock:
                if self._closed:
                    break

                # Attempt to complete pending connections
                for node_id in list(self._connecting):
                    # False return means no more connection progress is possible
                    # Connected nodes will update _connecting via state_change callback
                    if not self._init_connect(node_id):
                        # It's possible that the connection attempt triggered a state change
                        # but if not, make sure to remove from _connecting list
                        if node_id in self._connecting:
                            self._connecting.remove(node_id)

                # Send a metadata request if needed (or initiate new connection)
                metadata_timeout_ms = self._maybe_refresh_metadata()

                # If we got a future that is already done, don't block in _poll
                if future is not None and future.is_done:
                    timeout = 0
                else:
                    user_timeout_ms = 1000 * max(0, timeout_at - time.time())
                    idle_connection_timeout_ms = self._idle_expiry_manager.next_check_ms()
                    request_timeout_ms = self._next_ifr_request_timeout_ms()
                    log.debug("Timeouts: user %f, metadata %f, idle connection %f, request %f", user_timeout_ms, metadata_timeout_ms, idle_connection_timeout_ms, request_timeout_ms)
                    timeout = min(
                        user_timeout_ms,
                        metadata_timeout_ms,
                        idle_connection_timeout_ms,
                        request_timeout_ms)
                    timeout = max(0, timeout)  # avoid negative timeouts

                self._poll(timeout / 1000)

            # called without the lock to avoid deadlock potential
            # if handlers need to acquire locks
            responses.extend(self._fire_pending_completed_requests())

            # If all we had was a timeout (future is None) - only do one poll
            # If we do have a future, we keep looping until it is done
            if future is None:
                break
            elif future.is_done:
                break
            elif timeout_ms is not None and time.time() >= timeout_at:
                break

        return responses

    def _register_send_sockets(self):
        while self._sending:
            conn = self._sending.pop()
            if conn._sock is None:
                continue
            try:
                key = self._selector.get_key(conn._sock)
                events = key.events | selectors.EVENT_WRITE
                self._selector.modify(key.fileobj, events, key.data)
            except KeyError:
                self._selector.register(conn._sock, selectors.EVENT_WRITE, conn)

    def _poll(self, timeout):
        # Python throws OverflowError if timeout is > 2147483647 milliseconds
        # (though the param to selector.select is in seconds)
        # so convert any too-large timeout to blocking
        if timeout > 2147483:
            timeout = None
        # This needs to be locked, but since it is only called from within the
        # locked section of poll(), there is no additional lock acquisition here
        processed = set()

        # Send pending requests first, before polling for responses
        self._register_send_sockets()

        start_select = time.time()
        ready = self._selector.select(timeout)
        end_select = time.time()
        if self._sensors:
            self._sensors.select_time.record((end_select - start_select) * 1000000000)

        for key, events in ready:
            if key.fileobj is self._wake_r:
                self._clear_wake_fd()
                continue

            # Send pending requests if socket is ready to write
            if events & selectors.EVENT_WRITE:
                conn = key.data
                if conn.connecting():
                    conn.connect()
                else:
                    if conn.send_pending_requests_v2():
                        # If send is complete, we dont need to track write readiness
                        # for this socket anymore
                        if key.events ^ selectors.EVENT_WRITE:
                            self._selector.modify(
                                key.fileobj,
                                key.events ^ selectors.EVENT_WRITE,
                                key.data)
                        else:
                            self._selector.unregister(key.fileobj)

            if not (events & selectors.EVENT_READ):
                continue
            conn = key.data
            processed.add(conn)

            if not conn.in_flight_requests:
                # if we got an EVENT_READ but there were no in-flight requests, one of
                # two things has happened:
                #
                # 1. The remote end closed the connection (because it died, or because
                #    a firewall timed out, or whatever)
                # 2. The protocol is out of sync.
                #
                # either way, we can no longer safely use this connection
                #
                # Do a 1-byte read to check protocol didnt get out of sync, and then close the conn
                try:
                    unexpected_data = key.fileobj.recv(1)
                    if unexpected_data:  # anything other than a 0-byte read means protocol issues
                        log.warning('Protocol out of sync on %r, closing', conn)
                except socket.error:
                    pass
                conn.close(Errors.KafkaConnectionError('Socket EVENT_READ without in-flight-requests'))
                continue

            self._idle_expiry_manager.update(conn.node_id)
            self._pending_completion.extend(conn.recv())

        # Check for additional pending SSL bytes
        if self.config['security_protocol'] in ('SSL', 'SASL_SSL'):
            # TODO: optimize
            for conn in self._conns.values():
                if conn not in processed and conn.connected() and conn._sock.pending():
                    self._pending_completion.extend(conn.recv())

        for conn in six.itervalues(self._conns):
            if conn.requests_timed_out():
                timed_out = conn.timed_out_ifrs()
                timeout_ms = (timed_out[0][2] - timed_out[0][1]) * 1000
                log.warning('%s timed out after %s ms. Closing connection.',
                            conn, timeout_ms)
                conn.close(error=Errors.RequestTimedOutError(
                    'Request timed out after %s ms' %
                    timeout_ms))

        if self._sensors:
            self._sensors.io_time.record((time.time() - end_select) * 1000000000)

        self._maybe_close_oldest_connection()

    def in_flight_request_count(self, node_id=None):
        """Get the number of in-flight requests for a node or all nodes.

        Arguments:
            node_id (int, optional): a specific node to check. If unspecified,
                return the total for all nodes

        Returns:
            int: pending in-flight requests for the node, or all nodes if None
        """
        if node_id is not None:
            conn = self._conns.get(node_id)
            if conn is None:
                return 0
            return len(conn.in_flight_requests)
        else:
            return sum([len(conn.in_flight_requests)
                        for conn in list(self._conns.values())])

    def _fire_pending_completed_requests(self):
        responses = []
        while True:
            try:
                # We rely on deque.popleft remaining threadsafe
                # to allow both the heartbeat thread and the main thread
                # to process responses
                response, future = self._pending_completion.popleft()
            except IndexError:
                break
            future.success(response)
            responses.append(response)

        return responses

    def least_loaded_node(self):
        """Choose the node with fewest outstanding requests, with fallbacks.

        This method will prefer a node with an existing connection (not throttled)
        with no in-flight-requests. If no such node is found, a node will be chosen
        randomly from all nodes that are not throttled or "blacked out" (i.e.,
        are not subject to a reconnect backoff). If no node metadata has been
        obtained, will return a bootstrap node.

        Returns:
            node_id or None if no suitable node was found
        """
        nodes = [broker.nodeId for broker in self.cluster.brokers()]
        random.shuffle(nodes)

        inflight = float('inf')
        found = None
        for node_id in nodes:
            conn = self._conns.get(node_id)
            connected = conn is not None and conn.connected() and conn.can_send_more()
            blacked_out = conn is not None and (conn.blacked_out() or conn.throttled())
            curr_inflight = len(conn.in_flight_requests) if conn is not None else 0
            if connected and curr_inflight == 0:
                # if we find an established connection (not throttled)
                # with no in-flight requests, we can stop right away
                return node_id
            elif not blacked_out and curr_inflight < inflight:
                # otherwise if this is the best we have found so far, record that
                inflight = curr_inflight
                found = node_id

        return found

    def _refresh_delay_ms(self, node_id):
        conn = self._conns.get(node_id)
        if conn is not None and conn.connected():
            return self.throttle_delay(node_id)
        else:
            return self.connection_delay(node_id)

    def least_loaded_node_refresh_ms(self):
        """Return connection or throttle delay in milliseconds for next available node.

        This method is used primarily for retry/backoff during metadata refresh
        during / after a cluster outage, in which there are no available nodes.

        Returns:
           float: delay_ms
        """
        return min([self._refresh_delay_ms(broker.nodeId) for broker in self.cluster.brokers()])

    def set_topics(self, topics):
        """Set specific topics to track for metadata.

        Arguments:
            topics (list of str): topics to check for metadata

        Returns:
            Future: resolves after metadata request/response
        """
        if set(topics).difference(self._topics):
            future = self.cluster.request_update()
        else:
            future = Future().success(set(topics))
        self._topics = set(topics)
        return future

    def add_topic(self, topic):
        """Add a topic to the list of topics tracked via metadata.

        Arguments:
            topic (str): topic to track

        Returns:
            Future: resolves after metadata request/response
        """
        if topic in self._topics:
            return Future().success(set(self._topics))

        self._topics.add(topic)
        return self.cluster.request_update()

    def _next_ifr_request_timeout_ms(self):
        if self._conns:
            return min([conn.next_ifr_request_timeout_ms() for conn in six.itervalues(self._conns)])
        else:
            return float('inf')

    # This method should be locked when running multi-threaded
    def _maybe_refresh_metadata(self, wakeup=False):
        """Send a metadata request if needed.

        Returns:
            float: milliseconds until next refresh
        """
        ttl = self.cluster.ttl()
        wait_for_in_progress_ms = self.config['request_timeout_ms'] if self._metadata_refresh_in_progress else 0
        metadata_timeout = max(ttl, wait_for_in_progress_ms)

        if metadata_timeout > 0:
            return metadata_timeout

        # Beware that the behavior of this method and the computation of
        # timeouts for poll() are highly dependent on the behavior of
        # least_loaded_node()
        node_id = self.least_loaded_node()
        if node_id is None:
            next_connect_ms = self.least_loaded_node_refresh_ms()
            log.debug("Give up sending metadata request since no node is available. (reconnect delay %d ms)", next_connect_ms)
            return next_connect_ms

        if not self._can_send_request(node_id):
            # If there's any connection establishment underway, wait until it completes. This prevents
            # the client from unnecessarily connecting to additional nodes while a previous connection
            # attempt has not been completed.
            if self._connecting:
                return float('inf')

            elif self._can_connect(node_id):
                log.debug("Initializing connection to node %s for metadata request", node_id)
                self._connecting.add(node_id)
                if not self._init_connect(node_id):
                    if node_id in self._connecting:
                        self._connecting.remove(node_id)
                    # Connection attempt failed immediately, need to retry with a different node
                    return self.config['reconnect_backoff_ms']
            else:
                # Existing connection throttled or max in flight requests.
                return self.throttle_delay(node_id) or self.config['request_timeout_ms']

        # Recheck node_id in case we were able to connect immediately above
        if self._can_send_request(node_id):
            topics = list(self._topics)
            if not topics and self.cluster.is_bootstrap(node_id):
                topics = list(self.config['bootstrap_topics_filter'])

            api_version = self.api_version(MetadataRequest, max_version=7)
            if self.cluster.need_all_topic_metadata or not topics:
                topics = MetadataRequest[api_version].ALL_TOPICS
            if api_version >= 4:
                request = MetadataRequest[api_version](topics, self.config['allow_auto_create_topics'])
            else:
                request = MetadataRequest[api_version](topics)
            log.debug("Sending metadata request %s to node %s", request, node_id)
            future = self.send(node_id, request, wakeup=wakeup)
            future.add_callback(self.cluster.update_metadata)
            future.add_errback(self.cluster.failed_update)

            self._metadata_refresh_in_progress = True
            def refresh_done(val_or_error):
                self._metadata_refresh_in_progress = False
            future.add_callback(refresh_done)
            future.add_errback(refresh_done)
            return self.config['request_timeout_ms']

        # Should only get here if still connecting
        if self._connecting:
            return float('inf')
        else:
            return self.config['reconnect_backoff_ms']

    def get_api_versions(self):
        """Return the ApiVersions map, if available.

        Note: Only available after bootstrap; requires broker version 0.10.0 or later.

        Returns: a map of dict mapping {api_key : (min_version, max_version)},
        or None if ApiVersion is not supported by the kafka cluster.
        """
        return self._api_versions

    def check_version(self, node_id=None, timeout=None, **kwargs):
        """Attempt to guess the version of a Kafka broker.

        Keyword Arguments:
            node_id (str, optional): Broker node id from cluster metadata. If None, attempts
                to connect to any available broker until version is identified.
                Default: None
            timeout (num, optional): Maximum time in seconds to try to check broker version.
                If unable to identify version before timeout, raise error (see below).
                Default: api_version_auto_timeout_ms / 1000

        Returns: version tuple, i.e. (3, 9), (2, 0), (0, 10, 2) etc

        Raises:
            NodeNotReadyError (if node_id is provided)
            NoBrokersAvailable (if node_id is None)
        """
        timeout = timeout or (self.config['api_version_auto_timeout_ms'] / 1000)
        with self._lock:
            end = time.time() + timeout
            while time.time() < end:
                time_remaining = max(end - time.time(), 0)
                if node_id is not None and self.connection_delay(node_id) > 0:
                    sleep_time = min(time_remaining, self.connection_delay(node_id) / 1000.0)
                    if sleep_time > 0:
                        time.sleep(sleep_time)
                    continue
                try_node = node_id or self.least_loaded_node()
                if try_node is None:
                    sleep_time = min(time_remaining,  self.least_loaded_node_refresh_ms() / 1000.0)
                    if sleep_time > 0:
                        log.warning('No node available during check_version; sleeping %.2f secs', sleep_time)
                        time.sleep(sleep_time)
                    continue
                log.debug('Attempting to check version with node %s', try_node)
                if not self._init_connect(try_node):
                    if try_node == node_id:
                        raise Errors.NodeNotReadyError("Connection failed to %s" % node_id)
                    else:
                        continue
                conn = self._conns[try_node]

                while conn.connecting() and time.time() < end:
                    timeout_ms = min((end - time.time()) * 1000, 200)
                    self.poll(timeout_ms=timeout_ms)

                if conn._api_version is not None:
                    return conn._api_version
                else:
                    log.debug('Failed to identify api_version after connection attempt to %s', conn)

            # Timeout
            else:
                if node_id is not None:
                    raise Errors.NodeNotReadyError(node_id)
                else:
                    raise Errors.NoBrokersAvailable()

    def api_version(self, operation, max_version=None):
        """Find the latest version of the protocol operation supported by both
        this library and the broker.

        This resolves to the lesser of either the latest api version this
        library supports, or the max version supported by the broker.

        Arguments:
            operation: A list of protocol operation versions from kafka.protocol.

        Keyword Arguments:
            max_version (int, optional): Provide an alternate maximum api version
                to reflect limitations in user code.

        Returns:
            int: The highest api version number compatible between client and broker.

        Raises: IncompatibleBrokerVersion if no matching version is found
        """
        # Cap max_version at the largest available version in operation list
        max_version = min(len(operation) - 1, max_version if max_version is not None else float('inf'))
        broker_api_versions = self._api_versions
        api_key = operation[0].API_KEY
        if broker_api_versions is None or api_key not in broker_api_versions:
            raise Errors.IncompatibleBrokerVersion(
                "Kafka broker does not support the '{}' Kafka protocol."
                .format(operation[0].__name__))
        broker_min_version, broker_max_version = broker_api_versions[api_key]
        version = min(max_version, broker_max_version)
        if version < broker_min_version:
            # max library version is less than min broker version. Currently,
            # no Kafka versions specify a min msg version. Maybe in the future?
            raise Errors.IncompatibleBrokerVersion(
                "No version of the '{}' Kafka protocol is supported by both the client and broker."
                .format(operation[0].__name__))
        return version

    def wakeup(self):
        if self._waking or self._wake_w is None:
            return
        with self._wake_lock:
            try:
                self._wake_w.sendall(b'x')
                self._waking = True
            except socket.timeout as e:
                log.warning('Timeout to send to wakeup socket!')
                raise Errors.KafkaTimeoutError(e)
            except socket.error as e:
                log.warning('Unable to send to wakeup socket! %s', e)
                raise e

    def _clear_wake_fd(self):
        # reading from wake socket should only happen in a single thread
        with self._wake_lock:
            self._waking = False
            while True:
                try:
                    if not self._wake_r.recv(1024):
                        # Non-blocking socket returns empty on error
                        log.warning("Error reading wakeup socket. Rebuilding socketpair.")
                        self._close_wakeup_socketpair()
                        self._init_wakeup_socketpair()
                        break
                except socket.error:
                    # Non-blocking socket raises when socket is ok but no data available to read
                    break

    def _maybe_close_oldest_connection(self):
        expired_connection = self._idle_expiry_manager.poll_expired_connection()
        if expired_connection:
            conn_id, ts = expired_connection
            idle_ms = (time.time() - ts) * 1000
            log.info('Closing idle connection %s, last active %d ms ago', conn_id, idle_ms)
            self.close(node_id=conn_id)

    def bootstrap_connected(self):
        """Return True if a bootstrap node is connected"""
        for node_id in self._conns:
            if not self.cluster.is_bootstrap(node_id):
                continue
            if self._conns[node_id].connected():
                return True
        else:
            return False


# OrderedDict requires python2.7+
try:
    from collections import OrderedDict
except ImportError:
    # If we dont have OrderedDict, we'll fallback to dict with O(n) priority reads
    OrderedDict = dict


class IdleConnectionManager(object):
    def __init__(self, connections_max_idle_ms):
        if connections_max_idle_ms > 0:
            self.connections_max_idle = connections_max_idle_ms / 1000
        else:
            self.connections_max_idle = float('inf')
        self.next_idle_close_check_time = None
        self.update_next_idle_close_check_time(time.time())
        self.lru_connections = OrderedDict()

    def update(self, conn_id):
        # order should reflect last-update
        if conn_id in self.lru_connections:
            del self.lru_connections[conn_id]
        self.lru_connections[conn_id] = time.time()

    def remove(self, conn_id):
        if conn_id in self.lru_connections:
            del self.lru_connections[conn_id]

    def is_expired(self, conn_id):
        if conn_id not in self.lru_connections:
            return None
        return time.time() >= self.lru_connections[conn_id] + self.connections_max_idle

    def next_check_ms(self):
        now = time.time()
        if not self.lru_connections or self.next_idle_close_check_time == float('inf'):
            return float('inf')
        elif self.next_idle_close_check_time <= now:
            return 0
        else:
            return int((self.next_idle_close_check_time - now) * 1000)

    def update_next_idle_close_check_time(self, ts):
        self.next_idle_close_check_time = ts + self.connections_max_idle

    def poll_expired_connection(self):
        if time.time() < self.next_idle_close_check_time:
            return None

        if not len(self.lru_connections):
            return None

        oldest_conn_id = None
        oldest_ts = None
        if OrderedDict is dict:
            for conn_id, ts in self.lru_connections.items():
                if oldest_conn_id is None or ts < oldest_ts:
                    oldest_conn_id = conn_id
                    oldest_ts = ts
        else:
            (oldest_conn_id, oldest_ts) = next(iter(self.lru_connections.items()))

        self.update_next_idle_close_check_time(oldest_ts)

        if time.time() >= oldest_ts + self.connections_max_idle:
            return (oldest_conn_id, oldest_ts)
        else:
            return None


class KafkaClientMetrics(object):
    def __init__(self, metrics, metric_group_prefix, conns):
        self.metrics = metrics
        self.metric_group_name = metric_group_prefix + '-metrics'

        self.connection_closed = metrics.sensor('connections-closed')
        self.connection_closed.add(metrics.metric_name(
            'connection-close-rate', self.metric_group_name,
            'Connections closed per second in the window.'), Rate())
        self.connection_created = metrics.sensor('connections-created')
        self.connection_created.add(metrics.metric_name(
            'connection-creation-rate', self.metric_group_name,
            'New connections established per second in the window.'), Rate())

        self.select_time = metrics.sensor('select-time')
        self.select_time.add(metrics.metric_name(
            'select-rate', self.metric_group_name,
            'Number of times the I/O layer checked for new I/O to perform per'
            ' second'), Rate(sampled_stat=Count()))
        self.select_time.add(metrics.metric_name(
            'io-wait-time-ns-avg', self.metric_group_name,
            'The average length of time the I/O thread spent waiting for a'
            ' socket ready for reads or writes in nanoseconds.'), Avg())
        self.select_time.add(metrics.metric_name(
            'io-wait-ratio', self.metric_group_name,
            'The fraction of time the I/O thread spent waiting.'),
            Rate(time_unit=TimeUnit.NANOSECONDS))

        self.io_time = metrics.sensor('io-time')
        self.io_time.add(metrics.metric_name(
            'io-time-ns-avg', self.metric_group_name,
            'The average length of time for I/O per select call in nanoseconds.'),
            Avg())
        self.io_time.add(metrics.metric_name(
            'io-ratio', self.metric_group_name,
            'The fraction of time the I/O thread spent doing I/O'),
            Rate(time_unit=TimeUnit.NANOSECONDS))

        metrics.add_metric(metrics.metric_name(
            'connection-count', self.metric_group_name,
            'The current number of active connections.'), AnonMeasurable(
                lambda config, now: len(conns)))
