from __future__ import absolute_import, division

import copy
import functools
import heapq
import itertools
import logging
import random
import threading

# selectors in stdlib as of py3.4
try:
    import selectors # pylint: disable=import-error
except ImportError:
    # vendored backport module
    from .vendor import selectors34 as selectors

import socket
import time

from kafka.vendor import six

from .cluster import ClusterMetadata
from .conn import BrokerConnection, ConnectionStates, collect_hosts, get_ip_port_afi
from . import errors as Errors
from .future import Future
from .metrics import AnonMeasurable
from .metrics.stats import Avg, Count, Rate
from .metrics.stats.rate import TimeUnit
from .protocol.metadata import MetadataRequest
from .protocol.produce import ProduceRequest
from .vendor import socketpair
from .version import __version__

if six.PY2:
    ConnectionError = None


log = logging.getLogger('kafka.client')


class KafkaClient(object):
    """
    A network client for asynchronous request/response network i/o.
    This is an internal class used to implement the
    user-facing producer and consumer clients.

    This class is not thread-safe!
    """
    DEFAULT_CONFIG = {
        'bootstrap_servers': 'localhost',
        'client_id': 'kafka-python-' + __version__,
        'request_timeout_ms': 40000,
        'reconnect_backoff_ms': 50,
        'max_in_flight_requests_per_connection': 5,
        'receive_buffer_bytes': None,
        'send_buffer_bytes': None,
        'socket_options': [(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)],
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
    }
    API_VERSIONS = [
        (0, 10),
        (0, 9),
        (0, 8, 2),
        (0, 8, 1),
        (0, 8, 0)
    ]

    def __init__(self, **configs):
        """Initialize an asynchronous kafka client

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
            api_version (tuple): specify which kafka API version to use. Accepted
                values are: (0, 8, 0), (0, 8, 1), (0, 8, 2), (0, 9), (0, 10)
                If None, KafkaClient will attempt to infer the broker
                version by probing various APIs. Default: None
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
        """
        self.config = copy.copy(self.DEFAULT_CONFIG)
        for key in self.config:
            if key in configs:
                self.config[key] = configs[key]

        if self.config['api_version'] is not None:
            assert self.config['api_version'] in self.API_VERSIONS, (
                'api_version [{0}] must be one of: {1}'.format(
                    self.config['api_version'], str(self.API_VERSIONS)))

        self.cluster = ClusterMetadata(**self.config)
        self._topics = set() # empty set will fetch all topic metadata
        self._metadata_refresh_in_progress = False
        self._last_no_node_available_ms = 0
        self._selector = self.config['selector']()
        self._conns = {}
        self._connecting = set()
        self._refresh_on_disconnects = True
        self._delayed_tasks = DelayedTaskQueue()
        self._last_bootstrap = 0
        self._bootstrap_fails = 0
        self._wake_r, self._wake_w = socket.socketpair()
        self._wake_r.setblocking(False)
        self._wake_lock = threading.Lock()
        self._selector.register(self._wake_r, selectors.EVENT_READ)
        self._closed = False
        self._sensors = None
        if self.config['metrics']:
            self._sensors = KafkaClientMetrics(self.config['metrics'],
                                               self.config['metric_group_prefix'],
                                               self._conns)

        self._bootstrap(collect_hosts(self.config['bootstrap_servers']))

        # Check Broker Version if not set explicitly
        if self.config['api_version'] is None:
            check_timeout = self.config['api_version_auto_timeout_ms'] / 1000
            self.config['api_version'] = self.check_version(timeout=check_timeout)

    def _bootstrap(self, hosts):
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
            cb = functools.partial(self._conn_state_change, 'bootstrap')
            bootstrap = BrokerConnection(host, port, afi,
                                         state_change_callback=cb,
                                         node_id='bootstrap',
                                         **self.config)
            bootstrap.connect()
            while bootstrap.connecting():
                bootstrap.connect()
            if bootstrap.state is not ConnectionStates.CONNECTED:
                bootstrap.close()
                continue
            future = bootstrap.send(metadata_request)
            while not future.is_done:
                bootstrap.recv()
            if future.failed():
                bootstrap.close()
                continue
            self.cluster.update_metadata(future.value)

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
        return conn.state is ConnectionStates.DISCONNECTED and not conn.blacked_out()

    def _conn_state_change(self, node_id, conn):
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
            if self._refresh_on_disconnects and not self._closed:
                log.warning("Node %s connection failed -- refreshing metadata", node_id)
                self.cluster.request_update()

    def _maybe_connect(self, node_id):
        """Idempotent non-blocking connection attempt to the given node id."""
        if node_id not in self._conns:
            broker = self.cluster.broker_metadata(node_id)
            assert broker, 'Broker id %s not in current metadata' % node_id

            log.debug("Initiating connection to node %s at %s:%s",
                      node_id, broker.host, broker.port)
            host, port, afi = get_ip_port_afi(broker.host)
            cb = functools.partial(self._conn_state_change, node_id)
            self._conns[node_id] = BrokerConnection(host, broker.port, afi,
                                                    state_change_callback=cb,
                                                    node_id=node_id,
                                                    **self.config)
        conn = self._conns[node_id]
        if conn.connected():
            return True
        conn.connect()
        return conn.connected()

    def ready(self, node_id):
        """Check whether a node is connected and ok to send more requests.

        Arguments:
            node_id (int): the id of the node to check

        Returns:
            bool: True if we are ready to send to the given node
        """
        self._maybe_connect(node_id)
        return self.is_ready(node_id)

    def connected(self, node_id):
        """Return True iff the node_id is connected."""
        if node_id not in self._conns:
            return False
        return self._conns[node_id].connected()

    def close(self, node_id=None):
        """Closes one or all broker connections.

        Arguments:
            node_id (int, optional): the id of the node to close
        """
        if node_id is None:
            self._closed = True
            for conn in self._conns.values():
                conn.close()
            self._wake_r.close()
            self._wake_w.close()
            self._selector.close()
        elif node_id in self._conns:
            self._conns[node_id].close()
        else:
            log.warning("Node %s not found in current connection list; skipping", node_id)
            return

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
        if node_id not in self._conns:
            return False
        return self._conns[node_id].disconnected()

    def connection_delay(self, node_id):
        """
        Returns the number of milliseconds to wait, based on the connection
        state, before attempting to send data. When disconnected, this respects
        the reconnect backoff time. When connecting, returns 0 to allow
        non-blocking connect to finish. When connected, returns a very large
        number to handle slow/stalled connections.

        Arguments:
            node_id (int): The id of the node to check

        Returns:
            int: The number of milliseconds to wait.
        """
        if node_id not in self._conns:
            return 0

        conn = self._conns[node_id]
        time_waited_ms = time.time() - (conn.last_attempt or 0)
        if conn.state is ConnectionStates.DISCONNECTED:
            return max(self.config['reconnect_backoff_ms'] - time_waited_ms, 0)
        elif conn.connecting():
            return 0
        else:
            return 999999999

    def is_ready(self, node_id):
        """Check whether a node is ready to send more requests.

        In addition to connection-level checks, this method also is used to
        block additional requests from being sent during a metadata refresh.

        Arguments:
            node_id (int): id of the node to check

        Returns:
            bool: True if the node is ready and metadata is not refreshing
        """
        # if we need to update our metadata now declare all requests unready to
        # make metadata requests first priority
        if not self._metadata_refresh_in_progress and not self.cluster.ttl() == 0:
            if self._can_send_request(node_id):
                return True
        return False

    def _can_send_request(self, node_id):
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
        if not self._maybe_connect(node_id):
            return Future().failure(Errors.NodeNotReadyError(node_id))

        # Every request gets a response, except one special case:
        expect_response = True
        if isinstance(request, tuple(ProduceRequest)) and request.required_acks == 0:
            expect_response = False

        return self._conns[node_id].send(request, expect_response=expect_response)

    def poll(self, timeout_ms=None, future=None, sleep=True, delayed_tasks=True):
        """Try to read and write to sockets.

        This method will also attempt to complete node connections, refresh
        stale metadata, and run previously-scheduled tasks.

        Arguments:
            timeout_ms (int, optional): maximum amount of time to wait (in ms)
                for at least one response. Must be non-negative. The actual
                timeout will be the minimum of timeout, request timeout and
                metadata timeout. Default: request_timeout_ms
            future (Future, optional): if provided, blocks until future.is_done
            sleep (bool): if True and there is nothing to do (no connections
                or requests in flight), will sleep for duration timeout before
                returning empty results. Default: False.

        Returns:
            list: responses received (can be empty)
        """
        if timeout_ms is None:
            timeout_ms = self.config['request_timeout_ms']

        responses = []

        # Loop for futures, break after first loop if None
        while True:

            # Attempt to complete pending connections
            for node_id in list(self._connecting):
                self._maybe_connect(node_id)

            # Send a metadata request if needed
            metadata_timeout_ms = self._maybe_refresh_metadata()

            # Send scheduled tasks
            if delayed_tasks:
                for task, task_future in self._delayed_tasks.pop_ready():
                    try:
                        result = task()
                    except Exception as e:
                        log.error("Task %s failed: %s", task, e)
                        task_future.failure(e)
                    else:
                        task_future.success(result)

            # If we got a future that is already done, don't block in _poll
            if future and future.is_done:
                timeout = 0
            else:
                timeout = min(
                    timeout_ms,
                    metadata_timeout_ms,
                    self._delayed_tasks.next_at() * 1000,
                    self.config['request_timeout_ms'])
                timeout = max(0, timeout / 1000.0) # avoid negative timeouts

            responses.extend(self._poll(timeout, sleep=sleep))

            # If all we had was a timeout (future is None) - only do one poll
            # If we do have a future, we keep looping until it is done
            if not future or future.is_done:
                break

        return responses

    def _poll(self, timeout, sleep=True):
        # select on reads across all connected sockets, blocking up to timeout
        assert self.in_flight_request_count() > 0 or self._connecting or sleep

        responses = []
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
                conn.close()
                continue

            # Accumulate as many responses as the connection has pending
            while conn.in_flight_requests:
                response = conn.recv() # Note: conn.recv runs callbacks / errbacks

                # Incomplete responses are buffered internally
                # while conn.in_flight_requests retains the request
                if not response:
                    break
                responses.append(response)

        # Check for additional pending SSL bytes
        if self.config['security_protocol'] in ('SSL', 'SASL_SSL'):
            # TODO: optimize
            for conn in self._conns.values():
                if conn not in processed and conn.connected() and conn._sock.pending():
                    response = conn.recv()
                    if response:
                        responses.append(response)

        for conn in six.itervalues(self._conns):
            if conn.requests_timed_out():
                log.warning('%s timed out after %s ms. Closing connection.',
                            conn, conn.config['request_timeout_ms'])
                conn.close(error=Errors.RequestTimedOutError(
                    'Request timed out after %s ms' %
                    conn.config['request_timeout_ms']))

        if self._sensors:
            self._sensors.io_time.record((time.time() - end_select) * 1000000000)
        return responses

    def in_flight_request_count(self, node_id=None):
        """Get the number of in-flight requests for a node or all nodes.

        Arguments:
            node_id (int, optional): a specific node to check. If unspecified,
                return the total for all nodes

        Returns:
            int: pending in-flight requests for the node, or all nodes if None
        """
        if node_id is not None:
            if node_id not in self._conns:
                return 0
            return len(self._conns[node_id].in_flight_requests)
        else:
            return sum([len(conn.in_flight_requests) for conn in self._conns.values()])

    def least_loaded_node(self):
        """Choose the node with fewest outstanding requests, with fallbacks.

        This method will prefer a node with an existing connection, but will
        potentially choose a node for which we don't yet have a connection if
        all existing connections are in use. This method will never choose a
        node that was disconnected within the reconnect backoff period.
        If all else fails, the method will attempt to bootstrap again using the
        bootstrap_servers list.

        Returns:
            node_id or None if no suitable node was found
        """
        nodes = [broker.nodeId for broker in self.cluster.brokers()]
        random.shuffle(nodes)

        inflight = float('inf')
        found = None
        for node_id in nodes:
            conn = self._conns.get(node_id)
            connected = conn is not None and conn.connected()
            blacked_out = conn is not None and conn.blacked_out()
            curr_inflight = len(conn.in_flight_requests) if conn else 0
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

        # Last option: try to bootstrap again
        # this should only happen if no prior bootstrap has been successful
        log.error('No nodes found in metadata -- retrying bootstrap')
        self._bootstrap(collect_hosts(self.config['bootstrap_servers']))
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

    # request metadata update on disconnect and timedout
    def _maybe_refresh_metadata(self):
        """Send a metadata request if needed.

        Returns:
            int: milliseconds until next refresh
        """
        ttl = self.cluster.ttl()
        next_reconnect_ms = self._last_no_node_available_ms + self.cluster.refresh_backoff()
        next_reconnect_ms = max(next_reconnect_ms - time.time() * 1000, 0)
        wait_for_in_progress_ms = 9999999999 if self._metadata_refresh_in_progress else 0
        timeout = max(ttl, next_reconnect_ms, wait_for_in_progress_ms)

        if timeout == 0:
            node_id = self.least_loaded_node()
            if node_id is None:
                log.debug("Give up sending metadata request since no node is available")
                # mark the timestamp for no node available to connect
                self._last_no_node_available_ms = time.time() * 1000
                return timeout

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

            elif self._can_connect(node_id):
                log.debug("Initializing connection to node %s for metadata request", node_id)
                self._maybe_connect(node_id)
                # If initiateConnect failed immediately, this node will be put into blackout and we
                # should allow immediately retrying in case there is another candidate node. If it
                # is still connecting, the worst case is that we end up setting a longer timeout
                # on the next round and then wait for the response.
            else:
                # connected, but can't send more OR connecting
                # In either case, we just need to wait for a network event to let us know the selected
                # connection might be usable again.
                self._last_no_node_available_ms = time.time() * 1000

        return timeout

    def schedule(self, task, at):
        """Schedule a new task to be executed at the given time.

        This is "best-effort" scheduling and should only be used for coarse
        synchronization. A task cannot be scheduled for multiple times
        simultaneously; any previously scheduled instance of the same task
        will be cancelled.

        Arguments:
            task (callable): task to be scheduled
            at (float or int): epoch seconds when task should run

        Returns:
            Future: resolves to result of task call, or exception if raised
        """
        return self._delayed_tasks.add(task, at)

    def unschedule(self, task):
        """Unschedule a task.

        This will remove all instances of the task from the task queue.
        This is a no-op if the task is not scheduled.

        Arguments:
            task (callable): task to be unscheduled
        """
        self._delayed_tasks.remove(task)

    def check_version(self, node_id=None, timeout=2, strict=False):
        """Attempt to guess a broker version

        Note: it is possible that this method blocks longer than the
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
            if self._wake_w.send(b'x') != 1:
                log.warning('Unable to send to wakeup socket!')

    def _clear_wake_fd(self):
        # reading from wake socket should only happen in a single thread
        while True:
            try:
                self._wake_r.recv(1024)
            except:
                break


class DelayedTaskQueue(object):
    # see https://docs.python.org/2/library/heapq.html
    def __init__(self):
        self._tasks = [] # list of entries arranged in a heap
        self._task_map = {} # mapping of tasks to entries
        self._counter = itertools.count() # unique sequence count

    def add(self, task, at):
        """Add a task to run at a later time.

        Arguments:
            task: can be anything, but generally a callable
            at (float or int): epoch seconds to schedule task

        Returns:
            Future: a future that will be returned with the task when ready
        """
        if task in self._task_map:
            self.remove(task)
        count = next(self._counter)
        future = Future()
        entry = [at, count, (task, future)]
        self._task_map[task] = entry
        heapq.heappush(self._tasks, entry)
        return future

    def remove(self, task):
        """Remove a previously scheduled task.

        Raises:
            KeyError: if task is not found
        """
        entry = self._task_map.pop(task)
        task, future = entry[-1]
        future.failure(Errors.Cancelled)
        entry[-1] = 'REMOVED'

    def _drop_removed(self):
        while self._tasks and self._tasks[0][-1] is 'REMOVED':
            at, count, task = heapq.heappop(self._tasks)

    def _pop_next(self):
        self._drop_removed()
        if not self._tasks:
            raise KeyError('pop from an empty DelayedTaskQueue')
        _, _, maybe_task = heapq.heappop(self._tasks)
        if maybe_task is 'REMOVED':
            raise ValueError('popped a removed tasks from queue - bug')
        else:
            task, future = maybe_task
        del self._task_map[task]
        return (task, future)

    def next_at(self):
        """Number of seconds until next task is ready."""
        self._drop_removed()
        if not self._tasks:
            return 9999999999
        else:
            return max(self._tasks[0][0] - time.time(), 0)

    def pop_ready(self):
        """Pop and return a list of all ready (task, future) tuples"""
        ready_tasks = []
        while self._tasks and self._tasks[0][0] < time.time():
            try:
                task = self._pop_next()
            except KeyError:
                break
            ready_tasks.append(task)
        return ready_tasks


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
