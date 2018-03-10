from __future__ import absolute_import, division

import collections
import copy
import functools
import logging
import random
import threading
import weakref

# selectors in stdlib as of py3.4
try:
    import selectors  # pylint: disable=import-error
except ImportError:
    # vendored backport module
    from kafka.vendor import selectors34 as selectors

import socket
import time

from kafka.vendor import six

from kafka.cluster import ClusterMetadata
from kafka.conn import BrokerConnection, ConnectionStates, collect_hosts, get_ip_port_afi
from kafka import errors as Errors
from kafka.future import Future
from kafka.metrics import AnonMeasurable
from kafka.metrics.stats import Avg, Count, Rate
from kafka.metrics.stats.rate import TimeUnit
from kafka.protocol.metadata import MetadataRequest
from kafka.util import Dict, WeakMethod
# Although this looks unused, it actually monkey-patches socket.socketpair()
# and should be left in as long as we're using socket.socketpair() in this file
from kafka.vendor import socketpair
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
            strings) that the consumer should contact to bootstrap initial
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
            milliseconds to wait when reconnecting to a broker that has
            repeatedly failed to connect. If provided, the backoff per host
            will increase exponentially for each consecutive connection
            failure, up to this maximum. To avoid connection storms, a
            randomization factor of 0.2 will be applied to the backoff
            resulting in a random range between 20% below and 20% above
            the computed value. Default: 1000.
        request_timeout_ms (int): Client request timeout in milliseconds.
            Default: 40000.
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
        security_protocol (str): Protocol used to communicate with brokers.
            Valid values are: PLAINTEXT, SSL. Default: PLAINTEXT.
        ssl_context (ssl.SSLContext): pre-configured SSLContext for wrapping
            socket connections. If provided, all other ssl_* configurations
            will be ignored. Default: None.
        ssl_check_hostname (bool): flag to configure whether ssl handshake
            should verify that the certificate matches the brokers hostname.
            default: true.
        ssl_cafile (str): optional filename of ca file to use in certificate
            veriication. default: none.
        ssl_certfile (str): optional filename of file in pem format containing
            the client certificate, as well as any ca certificates needed to
            establish the certificate's authenticity. default: none.
        ssl_keyfile (str): optional filename containing the client private key.
            default: none.
        ssl_password (str): optional password to be used when loading the
            certificate chain. default: none.
        ssl_crlfile (str): optional filename containing the CRL to check for
            certificate expiration. By default, no CRL check is done. When
            providing a file, only the leaf certificate will be checked against
            this CRL. The CRL can only be checked with Python 3.4+ or 2.7.9+.
            default: none.
        api_version (tuple): Specify which Kafka API version to use. If set
            to None, KafkaClient will attempt to infer the broker version by
            probing various APIs. Example: (0, 10, 2). Default: None
        api_version_auto_timeout_ms (int): number of milliseconds to throw a
            timeout exception from the constructor when checking the broker
            api version. Only applies if api_version is None
        selector (selectors.BaseSelector): Provide a specific selector
            implementation to use for I/O multiplexing.
            Default: selectors.DefaultSelector
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
        sasl_kerberos_service_name (str): Service name to include in GSSAPI
            sasl mechanism handshake. Default: 'kafka'
    """

    DEFAULT_CONFIG = {
        'bootstrap_servers': 'localhost',
        'client_id': 'kafka-python-' + __version__,
        'request_timeout_ms': 40000,
        'connections_max_idle_ms': 9 * 60 * 1000,
        'reconnect_backoff_ms': 50,
        'reconnect_backoff_max_ms': 1000,
        'max_in_flight_requests_per_connection': 5,
        'receive_buffer_bytes': None,
        'send_buffer_bytes': None,
        'socket_options': [(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)],
        'sock_chunk_bytes': 4096,  # undocumented experimental option
        'sock_chunk_buffer_count': 1000,  # undocumented experimental option
        'retry_backoff_ms': 100,
        'metadata_max_age_ms': 300000,
        'security_protocol': 'PLAINTEXT',
        'ssl_context': None,
        'ssl_check_hostname': True,
        'ssl_cafile': None,
        'ssl_certfile': None,
        'ssl_keyfile': None,
        'ssl_password': None,
        'ssl_crlfile': None,
        'api_version': None,
        'api_version_auto_timeout_ms': 2000,
        'selector': selectors.DefaultSelector,
        'metrics': None,
        'metric_group_prefix': '',
        'sasl_mechanism': None,
        'sasl_plain_username': None,
        'sasl_plain_password': None,
        'sasl_kerberos_service_name': 'kafka',
    }

    def __init__(self, **configs):
        self.config = copy.copy(self.DEFAULT_CONFIG)
        for key in self.config:
            if key in configs:
                self.config[key] = configs[key]

        self.cluster = ClusterMetadata(**self.config)
        self._topics = set()  # empty set will fetch all topic metadata
        self._metadata_refresh_in_progress = False
        self._selector = self.config['selector']()
        self._conns = Dict()  # object to support weakrefs
        self._connecting = set()
        self._refresh_on_disconnects = True
        self._last_bootstrap = 0
        self._bootstrap_fails = 0
        self._wake_r, self._wake_w = socket.socketpair()
        self._wake_r.setblocking(False)
        self._wake_lock = threading.Lock()

        self._lock = threading.RLock()

        # when requests complete, they are transferred to this queue prior to
        # invocation. The purpose is to avoid invoking them while holding the
        # lock above.
        self._pending_completion = collections.deque()

        self._selector.register(self._wake_r, selectors.EVENT_READ)
        self._idle_expiry_manager = IdleConnectionManager(self.config['connections_max_idle_ms'])
        self._closed = False
        self._sensors = None
        if self.config['metrics']:
            self._sensors = KafkaClientMetrics(self.config['metrics'],
                                               self.config['metric_group_prefix'],
                                               weakref.proxy(self._conns))

        self._bootstrap(collect_hosts(self.config['bootstrap_servers']))

        # Check Broker Version if not set explicitly
        if self.config['api_version'] is None:
            check_timeout = self.config['api_version_auto_timeout_ms'] / 1000
            self.config['api_version'] = self.check_version(timeout=check_timeout)

    def _bootstrap(self, hosts):
        log.info('Bootstrapping cluster metadata from %s', hosts)
        # Exponential backoff if bootstrap fails
        backoff_ms = self.config['reconnect_backoff_ms'] * 2 ** self._bootstrap_fails
        next_at = self._last_bootstrap + backoff_ms / 1000.0
        self._refresh_on_disconnects = False
        now = time.time()
        if next_at > now:
            log.debug("Sleeping %0.4f before bootstrapping again", next_at - now)
            time.sleep(next_at - now)
        self._last_bootstrap = time.time()

        if self.config['api_version'] is None or self.config['api_version'] < (0, 10):
            metadata_request = MetadataRequest[0]([])
        else:
            metadata_request = MetadataRequest[1](None)

        for host, port, afi in hosts:
            log.debug("Attempting to bootstrap via node at %s:%s", host, port)
            cb = functools.partial(WeakMethod(self._conn_state_change), 'bootstrap')
            bootstrap = BrokerConnection(host, port, afi,
                                         state_change_callback=cb,
                                         node_id='bootstrap',
                                         **self.config)
            if not bootstrap.connect_blocking():
                bootstrap.close()
                continue
            future = bootstrap.send(metadata_request)
            while not future.is_done:
                self._selector.select(1)
                for r, f in bootstrap.recv():
                    f.success(r)
            if future.failed():
                bootstrap.close()
                continue
            self.cluster.update_metadata(future.value)
            log.info('Bootstrap succeeded: found %d brokers and %d topics.',
                     len(self.cluster.brokers()), len(self.cluster.topics()))

            # A cluster with no topics can return no broker metadata
            # in that case, we should keep the bootstrap connection
            if not len(self.cluster.brokers()):
                self._conns['bootstrap'] = bootstrap
            else:
                bootstrap.close()
            self._bootstrap_fails = 0
            break
        # No bootstrap found...
        else:
            log.error('Unable to bootstrap from %s', hosts)
            # Max exponential backoff is 2^12, x4000 (50ms -> 200s)
            self._bootstrap_fails = min(self._bootstrap_fails + 1, 12)
        self._refresh_on_disconnects = True

    def _can_connect(self, node_id):
        if node_id not in self._conns:
            if self.cluster.broker_metadata(node_id):
                return True
            return False
        conn = self._conns[node_id]
        return conn.disconnected() and not conn.blacked_out()

    def _conn_state_change(self, node_id, conn):
        with self._lock:
            if conn.connecting():
                # SSL connections can enter this state 2x (second during Handshake)
                if node_id not in self._connecting:
                    self._connecting.add(node_id)
                    self._selector.register(conn._sock, selectors.EVENT_WRITE)

            elif conn.connected():
                log.debug("Node %s connected", node_id)
                if node_id in self._connecting:
                    self._connecting.remove(node_id)

                try:
                    self._selector.unregister(conn._sock)
                except KeyError:
                    pass
                self._selector.register(conn._sock, selectors.EVENT_READ, conn)
                if self._sensors:
                    self._sensors.connection_created.record()

                self._idle_expiry_manager.update(node_id)

                if 'bootstrap' in self._conns and node_id != 'bootstrap':
                    bootstrap = self._conns.pop('bootstrap')
                    # XXX: make conn.close() require error to cause refresh
                    self._refresh_on_disconnects = False
                    bootstrap.close()
                    self._refresh_on_disconnects = True

            # Connection failures imply that our metadata is stale, so let's refresh
            elif conn.state is ConnectionStates.DISCONNECTING:
                if node_id in self._connecting:
                    self._connecting.remove(node_id)
                try:
                    self._selector.unregister(conn._sock)
                except KeyError:
                    pass
                if self._sensors:
                    self._sensors.connection_closed.record()

                idle_disconnect = False
                if self._idle_expiry_manager.is_expired(node_id):
                    idle_disconnect = True
                self._idle_expiry_manager.remove(node_id)

                if self._refresh_on_disconnects and not self._closed and not idle_disconnect:
                    log.warning("Node %s connection failed -- refreshing metadata", node_id)
                    self.cluster.request_update()

    def _maybe_connect(self, node_id):
        """Idempotent non-blocking connection attempt to the given node id."""
        with self._lock:
            broker = self.cluster.broker_metadata(node_id)
            conn = self._conns.get(node_id)

            if conn is None:
                assert broker, 'Broker id %s not in current metadata' % node_id

                log.debug("Initiating connection to node %s at %s:%s",
                          node_id, broker.host, broker.port)
                host, port, afi = get_ip_port_afi(broker.host)
                cb = functools.partial(WeakMethod(self._conn_state_change), node_id)
                conn = BrokerConnection(host, broker.port, afi,
                                        state_change_callback=cb,
                                        node_id=node_id,
                                        **self.config)
                self._conns[node_id] = conn

            # Check if existing connection should be recreated because host/port changed
            elif conn.disconnected() and broker is not None:
                host, _, __ = get_ip_port_afi(broker.host)
                if conn.host != host or conn.port != broker.port:
                    log.info("Broker metadata change detected for node %s"
                             " from %s:%s to %s:%s", node_id, conn.host, conn.port,
                             broker.host, broker.port)

                    # Drop old connection object.
                    # It will be recreated on next _maybe_connect
                    self._conns.pop(node_id)
                    return False

            elif conn.connected():
                return True

            conn.connect()
            return conn.connected()

    def ready(self, node_id, metadata_priority=True):
        """Check whether a node is connected and ok to send more requests.

        Arguments:
            node_id (int): the id of the node to check
            metadata_priority (bool): Mark node as not-ready if a metadata
                refresh is required. Default: True

        Returns:
            bool: True if we are ready to send to the given node
        """
        self._maybe_connect(node_id)
        return self.is_ready(node_id, metadata_priority=metadata_priority)

    def connected(self, node_id):
        """Return True iff the node_id is connected."""
        with self._lock:
            if node_id not in self._conns:
                return False
            return self._conns[node_id].connected()

    def _close(self):
        if not self._closed:
            self._closed = True
            self._wake_r.close()
            self._wake_w.close()
            self._selector.close()

    def close(self, node_id=None):
        """Close one or all broker connections.

        Arguments:
            node_id (int, optional): the id of the node to close
        """
        with self._lock:
            if node_id is None:
                self._close()
                for conn in self._conns.values():
                    conn.close()
            elif node_id in self._conns:
                self._conns[node_id].close()
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
        with self._lock:
            if node_id not in self._conns:
                return False
            return self._conns[node_id].disconnected()

    def connection_delay(self, node_id):
        """
        Return the number of milliseconds to wait, based on the connection
        state, before attempting to send data. When disconnected, this respects
        the reconnect backoff time. When connecting, returns 0 to allow
        non-blocking connect to finish. When connected, returns a very large
        number to handle slow/stalled connections.

        Arguments:
            node_id (int): The id of the node to check

        Returns:
            int: The number of milliseconds to wait.
        """
        with self._lock:
            if node_id not in self._conns:
                return 0
            return self._conns[node_id].connection_delay()

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
        with self._lock:
            if node_id not in self._conns:
                return False
            conn = self._conns[node_id]
            return conn.connected() and conn.can_send_more()

    def send(self, node_id, request):
        """Send a request to a specific node.

        Arguments:
            node_id (int): destination node
            request (Struct): request object (not-encoded)

        Raises:
            AssertionError: if node_id is not in current cluster metadata

        Returns:
            Future: resolves to Response struct or Error
        """
        with self._lock:
            if not self._maybe_connect(node_id):
                return Future().failure(Errors.NodeNotReadyError(node_id))

            return self._conns[node_id].send(request)

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
        if future is not None:
            timeout_ms = 100
        elif timeout_ms is None:
            timeout_ms = self.config['request_timeout_ms']
        elif not isinstance(timeout_ms, (int, float)):
            raise RuntimeError('Invalid type for timeout: %s' % type(timeout_ms))

        # Loop for futures, break after first loop if None
        responses = []
        while True:
            with self._lock:

                # Attempt to complete pending connections
                for node_id in list(self._connecting):
                    self._maybe_connect(node_id)

                # Send a metadata request if needed
                metadata_timeout_ms = self._maybe_refresh_metadata()

                # If we got a future that is already done, don't block in _poll
                if future is not None and future.is_done:
                    timeout = 0
                else:
                    idle_connection_timeout_ms = self._idle_expiry_manager.next_check_ms()
                    timeout = min(
                        timeout_ms,
                        metadata_timeout_ms,
                        idle_connection_timeout_ms,
                        self.config['request_timeout_ms'])
                    timeout = max(0, timeout / 1000)  # avoid negative timeouts

                self._poll(timeout)

            # called without the lock to avoid deadlock potential
            # if handlers need to acquire locks
            responses.extend(self._fire_pending_completed_requests())

            # If all we had was a timeout (future is None) - only do one poll
            # If we do have a future, we keep looping until it is done
            if future is None or future.is_done:
                break

        return responses

    def _poll(self, timeout):
        """Returns list of (response, future) tuples"""
        processed = set()

        start_select = time.time()
        ready = self._selector.select(timeout)
        end_select = time.time()
        if self._sensors:
            self._sensors.select_time.record((end_select - start_select) * 1000000000)

        for key, events in ready:
            if key.fileobj is self._wake_r:
                self._clear_wake_fd()
                continue
            elif not (events & selectors.EVENT_READ):
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
                conn.close(Errors.ConnectionError('Socket EVENT_READ without in-flight-requests'))
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
                log.warning('%s timed out after %s ms. Closing connection.',
                            conn, conn.config['request_timeout_ms'])
                conn.close(error=Errors.RequestTimedOutError(
                    'Request timed out after %s ms' %
                    conn.config['request_timeout_ms']))

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
        with self._lock:
            if node_id is not None:
                if node_id not in self._conns:
                    return 0
                return len(self._conns[node_id].in_flight_requests)
            else:
                return sum([len(conn.in_flight_requests) for conn in self._conns.values()])

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

        This method will prefer a node with an existing connection and no
        in-flight-requests. If no such node is found, a node will be chosen
        randomly from disconnected nodes that are not "blacked out" (i.e.,
        are not subject to a reconnect backoff).

        Returns:
            node_id or None if no suitable node was found
        """
        with self._lock:
            nodes = [broker.nodeId for broker in self.cluster.brokers()]
            random.shuffle(nodes)

            inflight = float('inf')
            found = None
            for node_id in nodes:
                conn = self._conns.get(node_id)
                connected = conn is not None and conn.connected()
                blacked_out = conn is not None and conn.blacked_out()
                curr_inflight = len(conn.in_flight_requests) if conn is not None else 0
                if connected and curr_inflight == 0:
                    # if we find an established connection
                    # with no in-flight requests, we can stop right away
                    return node_id
                elif not blacked_out and curr_inflight < inflight:
                    # otherwise if this is the best we have found so far, record that
                    inflight = curr_inflight
                    found = node_id

            if found is not None:
                return found

            # some broker versions return an empty list of broker metadata
            # if there are no topics created yet. the bootstrap process
            # should detect this and keep a 'bootstrap' node alive until
            # a non-bootstrap node is connected and non-empty broker
            # metadata is available
            elif 'bootstrap' in self._conns:
                return 'bootstrap'

            return None

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

    # This method should be locked when running multi-threaded
    def _maybe_refresh_metadata(self):
        """Send a metadata request if needed.

        Returns:
            int: milliseconds until next refresh
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
            log.debug("Give up sending metadata request since no node is available");
            return self.config['reconnect_backoff_ms']

        if self._can_send_request(node_id):
            topics = list(self._topics)
            if self.cluster.need_all_topic_metadata or not topics:
                topics = [] if self.config['api_version'] < (0, 10) else None
            api_version = 0 if self.config['api_version'] < (0, 10) else 1
            request = MetadataRequest[api_version](topics)
            log.debug("Sending metadata request %s to node %s", request, node_id)
            future = self.send(node_id, request)
            future.add_callback(self.cluster.update_metadata)
            future.add_errback(self.cluster.failed_update)

            self._metadata_refresh_in_progress = True
            def refresh_done(val_or_error):
                self._metadata_refresh_in_progress = False
            future.add_callback(refresh_done)
            future.add_errback(refresh_done)
            return self.config['request_timeout_ms']

        # If there's any connection establishment underway, wait until it completes. This prevents
        # the client from unnecessarily connecting to additional nodes while a previous connection
        # attempt has not been completed.
        if self._connecting:
            # Strictly the timeout we should return here is "connect timeout", but as we don't
            # have such application level configuration, using request timeout instead.
            return self.config['request_timeout_ms']

        if self._can_connect(node_id):
            log.debug("Initializing connection to node %s for metadata request", node_id)
            self._maybe_connect(node_id)
            return self.config['reconnect_backoff_ms']

        # connected but can't send more, OR connecting
        # In either case we just need to wait for a network event
        # to let us know the selected connection might be usable again.
        return float('inf')

    def check_version(self, node_id=None, timeout=2, strict=False):
        """Attempt to guess the version of a Kafka broker.

        Note: It is possible that this method blocks longer than the
            specified timeout. This can happen if the entire cluster
            is down and the client enters a bootstrap backoff sleep.
            This is only possible if node_id is None.

        Returns: version tuple, i.e. (0, 10), (0, 9), (0, 8, 2), ...

        Raises:
            NodeNotReadyError (if node_id is provided)
            NoBrokersAvailable (if node_id is None)
            UnrecognizedBrokerVersion: please file bug if seen!
            AssertionError (if strict=True): please file bug if seen!
        """
        end = time.time() + timeout
        while time.time() < end:

            # It is possible that least_loaded_node falls back to bootstrap,
            # which can block for an increasing backoff period
            try_node = node_id or self.least_loaded_node()
            if try_node is None:
                raise Errors.NoBrokersAvailable()
            self._maybe_connect(try_node)
            conn = self._conns[try_node]

            # We will intentionally cause socket failures
            # These should not trigger metadata refresh
            self._refresh_on_disconnects = False
            try:
                remaining = end - time.time()
                version = conn.check_version(timeout=remaining, strict=strict)
                return version
            except Errors.NodeNotReadyError:
                # Only raise to user if this is a node-specific request
                if node_id is not None:
                    raise
            finally:
                self._refresh_on_disconnects = True

        # Timeout
        else:
            raise Errors.NoBrokersAvailable()

    def wakeup(self):
        with self._wake_lock:
            try:
                self._wake_w.sendall(b'x')
            except socket.error:
                log.warning('Unable to send to wakeup socket!')

    def _clear_wake_fd(self):
        # reading from wake socket should only happen in a single thread
        while True:
            try:
                self._wake_r.recv(1024)
            except socket.error:
                break

    def _maybe_close_oldest_connection(self):
        expired_connection = self._idle_expiry_manager.poll_expired_connection()
        if expired_connection:
            conn_id, ts = expired_connection
            idle_ms = (time.time() - ts) * 1000
            log.info('Closing idle connection %s, last active %d ms ago', conn_id, idle_ms)
            self.close(node_id=conn_id)


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
        if not self.lru_connections:
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
