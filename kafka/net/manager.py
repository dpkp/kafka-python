import copy
import logging
import inspect
import random
import socket
import ssl
import threading
import time

from .inet import create_connection
from .connection import KafkaConnection
from .metrics import KafkaManagerMetrics
from .transport import KafkaSSLTransport, KafkaTCPTransport
from kafka.cluster import ClusterMetadata
import kafka.errors as Errors
from kafka.protocol.broker_version_data import BrokerVersionData
from kafka.future import Future
from kafka.version import __version__


log = logging.getLogger(__name__)

class KafkaConnectionManager:
    DEFAULT_CONFIG = {
        'bootstrap_servers': 'localhost:9092',
        'client_id': 'kafka-python-' + __version__,
        'client_software_name': 'kafka-python',
        'client_software_version': __version__,
        'reconnect_backoff_ms': 50,
        'reconnect_backoff_max_ms': 30000,
        'request_timeout_ms': 30000,
        'socket_connection_timeout_ms': 5000,
        'socket_options': [
            (socket.IPPROTO_TCP, socket.TCP_NODELAY, 1),
            (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
        ],
        'max_in_flight_requests_per_connection': 5,
        'connections_max_idle_ms': 9 * 60 * 1000,
        'security_protocol': 'PLAINTEXT',
        'ssl_context': None,
        'ssl_check_hostname': True,
        'ssl_cafile': None,
        'ssl_certfile': None,
        'ssl_keyfile': None,
        'ssl_password': None,
        'ssl_crlfile': None,
        'sasl_mechanism': None,
        'sasl_plain_username': None,
        'sasl_plain_password': None,
        'sasl_kerberos_name': None,
        'sasl_kerberos_service_name': 'kafka',
        'sasl_kerberos_domain_name': None,
        'sasl_oauth_token_provider': None,
        'socks5_proxy': None,
        'api_version': None,
        'api_version_auto_timeout_ms': 2000,
        'metrics': None,
        'metric_group_prefix': '',
        'metadata_max_age_ms': 300000,
    }
    def __init__(self, net, **configs):
        self.config = copy.copy(self.DEFAULT_CONFIG)
        for key in self.config:
            if key in configs:
                self.config[key] = configs[key]

        self._net = net
        self.cluster = ClusterMetadata(
            bootstrap_servers=self.config['bootstrap_servers'],
            metadata_max_age_ms=self.config['metadata_max_age_ms'],
        )
        self.cluster.attach(self)
        self._conns = {}
        self._backoff = dict() # node_id => (failures, backoff_until)
        self._idle_check_delay = self.config['connections_max_idle_ms'] / 1000
        self.close_idle_connections()
        self.broker_version_data = None
        self._bootstrap_future = None
        self._io_thread = None
        self._pending_waiters = {}  # event -> state dict, for pending run() waiters
        self._pending_waiters_lock = threading.Lock()
        if self.config['metrics']:
            self._sensors = KafkaManagerMetrics(
                self.config['metrics'], self.config['metric_group_prefix'], self._conns)
        else:
            self._sensors = None
        if self.config['api_version'] is not None:
            self.broker_version_data = BrokerVersionData(self.config['api_version'])

    @property
    def broker_version(self):
        if self.broker_version_data is None:
            return None
        return self.broker_version_data.broker_version

    def least_used_connections(self):
        return sorted(filter(lambda conn: conn.connected, self._conns.values()), key=lambda conn: conn.transport.last_activity)

    async def _do_bootstrap(self, deadline):
        while deadline is None or time.monotonic() < deadline:
            bootstrap_broker = random.choice(self.cluster.bootstrap_brokers())
            log.debug('Attempting bootstrap with %s', bootstrap_broker)
            try:
                conn = self.get_connection(bootstrap_broker.node_id,
                                           pop_on_close=False,
                                           refresh_metadata_on_err=False,
                                           reset_backoff_on_connect=False)
            except Errors.NodeNotReadyError:
                delay = self.connection_delay(bootstrap_broker.node_id)
                if deadline is not None:
                    delay = min(delay, max(0, deadline - time.monotonic()))
                log.debug('Bootstrap %s NodeNotReadyError: backoff %s', bootstrap_broker, delay)
                await self._net.sleep(delay)
                continue

            try:
                await conn
            except Errors.IncompatibleBrokerVersion:
                log.error('Did you attempt to connect to a kafka controller (no metadata support)?')
                raise
            except Exception as exc:
                self._conns.pop(bootstrap_broker.node_id, conn).close(exc)
                continue

            try:
                await self.cluster.refresh_metadata(bootstrap_broker.node_id)
                if not self.cluster.brokers():
                    log.warning('Bootstrap metadata response has no brokers. Retrying.')
                    self.update_backoff(bootstrap_broker.node_id)
                    continue
            except Exception as exc:
                log.error(f'Bootstrap attempt to {bootstrap_broker.node_id} failed: {exc}')
                self.update_backoff(bootstrap_broker.node_id)
                continue
            else:
                self.reset_backoff(bootstrap_broker.node_id)
                self.cluster.start_refresh_loop()
                log.info('Bootstrap complete: %s', self.cluster)
                return True
            finally:
                self._conns.pop(bootstrap_broker.node_id, conn).close()
        else:
            raise Errors.KafkaTimeoutError(
                'Unable to bootstrap from %s' % (self.cluster.config['bootstrap_servers'],))

    def bootstrap_async(self, timeout_ms=None):
        if self._bootstrap_future is not None and not self._bootstrap_future.is_done:
            return self._bootstrap_future
        deadline = None if timeout_ms is None else time.monotonic() + timeout_ms / 1000
        log.debug('Starting new bootstrap')
        self._bootstrap_future = self.call_soon(self._do_bootstrap, deadline)
        self._bootstrap_future.add_errback(lambda exc: log.error('Bootstrap failed: %s', exc))
        return self._bootstrap_future

    def bootstrap(self, timeout_ms=None):
        self.run(self.bootstrap_async, timeout_ms)

    @property
    def bootstrapped(self):
        return self._bootstrap_future is not None and self._bootstrap_future.succeeded()

    def _connection_idle_at(self, conn):
        return conn.transport.last_activity + self._idle_check_delay

    def close_idle_connections(self):
        for conn in self.least_used_connections():
            next_idle_at = self._connection_idle_at(conn)
            if time.monotonic() >= next_idle_at:
                log.info('Closing idle connection to node %s', conn.node_id)
                conn.close()
            else:
                break
        else:
            next_idle_at = time.monotonic() + self._idle_check_delay
        log.debug('Next idle connections check in %d secs', next_idle_at - time.monotonic())
        self._net.call_at(next_idle_at, self.close_idle_connections)

    @property
    def ssl_enabled(self):
        return self.config['security_protocol'] in ('SSL', 'SASL_SSL')

    def _build_ssl_context(self):
        if self.config['ssl_context'] is not None:
            return self.config['ssl_context']
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        ctx.check_hostname = self.config['ssl_check_hostname']
        if self.config['ssl_cafile']:
            ctx.load_verify_locations(self.config['ssl_cafile'])
        else:
            ctx.load_default_certs()
        if self.config['ssl_certfile']:
            ctx.load_cert_chain(
                certfile=self.config['ssl_certfile'],
                keyfile=self.config['ssl_keyfile'],
                password=self.config['ssl_password'],
            )
        if self.config['ssl_crlfile']:
            ctx.load_verify_locations(crl=self.config['ssl_crlfile'])
            ctx.verify_flags |= ssl.VERIFY_CRL_CHECK_LEAF
        return ctx

    async def _build_transport(self, node):
        sock = await create_connection(self._net, node.host, node.port,
                                       self.config['socket_options'],
                                       socks5_proxy=self.config['socks5_proxy'])
        if self.ssl_enabled:
            hostname = node.host if self.config['ssl_check_hostname'] else None
            transport = KafkaSSLTransport(self._net, sock, self._build_ssl_context(), hostname)
        else:
            transport = KafkaTCPTransport(self._net, sock)

        try:
            await transport.handshake()
        except Exception as e:
            raise Errors.KafkaConnectionError('Handshake failed: %s' % e)
        else:
            return transport

    async def _connect(self, node, conn, reset_backoff_on_connect=True):
        try:
            transport = await self._build_transport(node)
            conn.connection_made(transport)
            await conn.init_future
        except Exception as exc:
            log.error('Connection failed: %s', exc)
            conn.connection_lost(exc)
            self.update_backoff(node.node_id)
            return

        if self._sensors:
            self._sensors.connection_created.record()
        if reset_backoff_on_connect:
            self.reset_backoff(node.node_id)
        if conn.broker_version_data is not None:
            if self.cluster.is_bootstrap(node.node_id):
                self.broker_version_data = conn.broker_version_data

    def get_connection(self, node_id, pop_on_close=True, refresh_metadata_on_err=True, reset_backoff_on_connect=True):
        if node_id is None:
            raise Errors.NodeNotReadyError('No node_id provided')
        elif self.connection_delay(node_id) > 0:
            raise Errors.NodeNotReadyError(node_id)
        elif node_id in self._conns:
            return self._conns[node_id]
        node = self.cluster.broker_metadata(node_id)
        if node is None:
            raise Errors.UnknownBrokerIdError(node_id)
        conn = KafkaConnection(self._net, node_id=node_id, broker_version_data=self.broker_version_data, **self.config)
        if pop_on_close:
            conn.close_future.add_both(lambda _: self._conns.pop(node.node_id, None))
        if self._sensors:
            conn.close_future.add_both(lambda _: self._sensors.connection_closed.record())
        if refresh_metadata_on_err:
            conn.close_future.add_errback(lambda _: self.cluster.request_update())
        self._conns[node_id] = conn
        self._net.call_soon(lambda: self._connect(node, conn, reset_backoff_on_connect=reset_backoff_on_connect))
        self._net.call_later(self.config['socket_connection_timeout_ms'] / 1000,
                             lambda: conn.close(Errors.KafkaConnectionError('Connection timed out'))
                                     if not conn.init_future.is_done else None)
        return conn

    def send(self, request, node_id=None):
        node_id = node_id if node_id is not None else self.least_loaded_node()
        try:
            conn = self.get_connection(node_id)
        except Errors.NodeNotReadyError as e:
            return Future().failure(e)
        else:
            return conn.send_request(request)

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
        nodes = [broker.node_id for broker in self.cluster.brokers()]
        random.shuffle(nodes)

        inflight = float('inf')
        found = None
        for node_id in nodes:
            conn = self._conns.get(node_id)
            connected = conn is not None and conn.connected and not conn.paused
            blacked_out = (conn and conn.paused) or self.connection_delay(node_id) > 0
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

    def reset_backoff(self, node_id):
        try:
            del self._backoff[node_id]
        except KeyError:
            pass

    def reconnect_jitter_pct(self):
        return random.uniform(0.8, 1.2)

    def update_backoff(self, node_id):
        failures, _ = self._backoff.get(node_id, (0, 0))
        failures += 1
        backoff_ms = self.config['reconnect_backoff_ms'] * 2 ** (failures - 1)
        backoff_ms = min(backoff_ms, self.config['reconnect_backoff_max_ms'])
        backoff_ms *= self.reconnect_jitter_pct()
        log.debug('%s reconnect backoff %d ms after %s failures', node_id, backoff_ms, failures)
        backoff_until_time = time.monotonic() + (backoff_ms / 1000)
        self._backoff[node_id] = (failures, backoff_until_time)

    def connection_delay(self, node_id):
        """Connection delay in seconds.

        Uses exponential backoff/retry with jitter. See KIP-144.
        """
        if node_id not in self._backoff:
            return 0
        return max(0, self._backoff[node_id][1] - time.monotonic())

    def close(self, node_id=None):
        if node_id is not None:
            conn = self._conns.get(node_id)
            if conn is not None:
                conn.close()
        else:
            for conn in list(self._conns.values()):
                conn.close()
            self.cluster.close()

    def start(self):
        """Spawn a daemon IO thread that owns the event loop. Idempotent."""
        if self._io_thread is not None:
            return
        t = threading.Thread(target=self._net.run_forever,
                             name='kafka-io-%s' % self.config['client_id'],
                             daemon=True)
        self._io_thread = t
        t.start()

    def stop(self, timeout=None):
        """Signal the IO thread to exit and join it. Fails any pending run()
        waiters with KafkaConnectionError. Idempotent."""
        t = self._io_thread
        if t is None:
            return
        self._io_thread = None
        self._net.stop()
        t.join(timeout)
        with self._pending_waiters_lock:
            waiters = list(self._pending_waiters.items())
            self._pending_waiters.clear()
        for event, state in waiters:
            state['exception'] = Errors.KafkaConnectionError('Manager stopped')
            event.set()

    def poll(self, timeout_ms=None, future=None):
        return self._net.poll(timeout_ms=timeout_ms, future=future)

    async def wait_for(self, future, timeout_ms):
        """Await `future` with a timeout in ms. Raises KafkaTimeoutError on timeout.

        Must be awaited from a coroutine running on this loop. The underlying
        future is not cancelled on timeout — it continues to run; the timeout
        only unblocks the awaiter.
        """
        if timeout_ms is None:
            return await future
        wrapper = Future()
        def _on_success(value):
            if not wrapper.is_done:
                wrapper.success(value)
        def _on_failure(exc):
            if not wrapper.is_done:
                wrapper.failure(exc)
        future.add_callback(_on_success)
        future.add_errback(_on_failure)
        def _on_timeout():
            if not wrapper.is_done:
                wrapper.failure(Errors.KafkaTimeoutError(
                    'Timed out after %s ms' % timeout_ms))
        timer = self._net.call_later(timeout_ms / 1000, _on_timeout)
        try:
            return await wrapper
        finally:
            if not timer.is_done:
                try:
                    self._net.unschedule(timer)
                except ValueError:
                    pass

    async def _invoke(self, coro, args):
        """Invoke coro/awaitable/function and fully resolve the result.

        If the result is itself a Future (e.g. send() returning an unresolved
        Future), it is awaited so callers receive the resolved value.
        """
        if inspect.iscoroutinefunction(coro):
            result = await coro(*args)
        elif hasattr(coro, '__await__'):
            result = await coro
        else:
            result = coro(*args)
        if inspect.iscoroutine(result) or hasattr(result, '__await__'):
            result = await result
        while isinstance(result, Future):
            result = await result
        return result

    def call_soon(self, coro, *args):
        """Accepts a coroutine / awaitable / function and schedules it on the event loop.

        Thread-safe.

        Returns: Future
        """
        if hasattr(coro, '__await__'):
            assert not args, 'initiated coroutine does not accept args'
        future = Future()
        async def wrapper():
            try:
                future.success(await self._invoke(coro, args))
            except Exception as exc:
                future.failure(exc)
        self._net.call_soon_threadsafe(wrapper)
        return future

    def run(self, coro, *args):
        """Schedules coro on the event loop, blocks until complete, returns value or raises.

        If an IO thread is running (via start()), the caller thread blocks on
        a cross-thread Event while the coroutine runs on the IO thread. Safe
        to call concurrently from multiple caller threads.

        If no IO thread is running, falls back to driving the loop on the
        caller thread (legacy behavior).
        """
        if self._io_thread is None:
            future = self.call_soon(coro, *args)
            self.poll(future=future)
            if future.exception is not None:
                raise future.exception
            return future.value

        event = threading.Event()
        state = {'value': None, 'exception': None}
        async def waiter():
            try:
                state['value'] = await self._invoke(coro, args)
            except BaseException as exc:
                state['exception'] = exc
            finally:
                with self._pending_waiters_lock:
                    self._pending_waiters.pop(event, None)
                event.set()
        with self._pending_waiters_lock:
            self._pending_waiters[event] = state
        self._net.call_soon_threadsafe(waiter)
        event.wait()
        if state['exception'] is not None:
            raise state['exception']
        return state['value']
