import logging
import random
import threading
import time

import kafka.errors as Errors
from kafka.net.manager import KafkaConnectionManager
from kafka.net.selector import NetworkSelector


log = logging.getLogger(__name__)


class KafkaNetClient:
    """Drop-in replacement for KafkaClient backed by KafkaConnectionManager.

    Provides the KafkaClient API surface that existing consumer/producer/admin
    code depends on. Goal: shrink over time as components transition to using
    KafkaConnectionManager directly (fire-and-forget via _request_buffer).
    """
    def __init__(self, **configs):
        # _lock is still used by the legacy Coordinator (kafka/coordinator/base.py).
        # Remove once Coordinator moves to the IO thread (Phase D).
        self._lock = threading.RLock()
        self._net = NetworkSelector(**configs)
        self._manager = KafkaConnectionManager(self._net, **configs)

    @property
    def cluster(self):
        return self._manager.cluster

    # Connection state queries

    def connected(self, node_id):
        conn = self._manager._conns.get(node_id)
        return conn is not None and conn.connected

    def is_disconnected(self, node_id):
        return not self.connected(node_id)

    def is_ready(self, node_id):
        conn = self._manager._conns.get(node_id)
        return conn is not None and conn.connected and not conn.paused

    def ready(self, node_id, **kwargs):
        if self.is_ready(node_id):
            return True
        try:
            self._manager.get_connection(node_id)
        except Errors.NodeNotReadyError:
            pass
        return False

    def maybe_connect(self, node_id, **kwargs):
        try:
            self._manager.get_connection(node_id)
        except Errors.NodeNotReadyError:
            pass

    def await_ready(self, node_id, timeout_ms=30000):
        if self.is_ready(node_id):
            return True
        self.maybe_connect(node_id)
        conn = self._manager._conns.get(node_id)
        if conn is not None and not conn.init_future.is_done:
            self._manager.poll(timeout_ms=timeout_ms, future=conn.init_future)
        # Connection may be initialized but paused (e.g. max_in_flight reached).
        # Poll briefly to drain in-flight responses and unpause.
        if conn is not None and conn.connected and conn.paused:
            self._manager.poll(timeout_ms=min(timeout_ms, self._manager.config['request_timeout_ms']))
        if not self.is_ready(node_id):
            raise Errors.KafkaConnectionError('Node %s not ready after %s ms' % (node_id, timeout_ms))
        return True

    # In-flight request tracking

    def in_flight_request_count(self, node_id=None):
        if node_id is not None:
            conn = self._manager._conns.get(node_id)
            return len(conn.in_flight_requests) if conn is not None else 0
        return sum(len(c.in_flight_requests) for c in self._manager._conns.values())

    def throttle_delay(self, node_id):
        conn = self._manager._conns.get(node_id)
        if conn is None:
            return 0
        remaining = conn._throttle_time - time.monotonic()
        return max(0, remaining) * 1000

    # Bootstrap / version

    def bootstrap_connected(self):
        bootstrap_future = self._manager._bootstrap_future
        return bootstrap_future is not None and not bootstrap_future.is_done

    def get_broker_version(self, timeout_ms=None):
        if self._manager.broker_version is None:
            self._manager.bootstrap(timeout_ms)
        return self._manager.broker_version

    def check_version(self, node_id=None, timeout_ms=10000):
        if not self._manager.bootstrapped:
            self._manager.bootstrap(timeout_ms)
        if node_id is None:
            return self._manager.broker_version
        async def _check_version(broker_id):
            conn = await self._manager.get_connection(broker_id)
            return conn.broker_version
        return self._manager.run(_check_version, node_id)

    # Request sending

    def send(self, node_id, request, **kwargs):
        return self._manager.send(request, node_id=node_id)

    def send_and_receive(self, node_id, request, timeout_ms=30000):
        self.await_ready(node_id, timeout_ms=timeout_ms)
        f = self.send(node_id, request)
        self._manager.poll(timeout_ms=timeout_ms, future=f)
        if f.succeeded():
            return f.value
        elif f.failed():
            raise f.exception
        raise Errors.KafkaTimeoutError('Request timed out')

    # Delegation

    def poll(self, timeout_ms=None, future=None):
        # _lock serializes with HeartbeatThread, which also drives poll()
        # while holding this lock. Without it, both threads would call
        # _net.poll() concurrently and race on selector / task state.
        # The lock goes away once HeartbeatThread does (Phase D).
        with self._lock:
            return self._manager.poll(timeout_ms=timeout_ms, future=future)

    def close(self, node_id=None):
        if node_id is None:
            self._manager.stop()
        self._manager.close(node_id=node_id)
        if node_id is None:
            self._net.close()

    def least_loaded_node(self, bootstrap_fallback=False):
        node_id = self._manager.least_loaded_node()
        if node_id is None and bootstrap_fallback:
            node_id = random.choice(self._manager.cluster.bootstrap_brokers()).node_id
        return node_id

    def least_loaded_node_refresh_ms(self, bootstrap_fallback=False):
        brokers = self._manager.cluster.brokers()
        if not brokers and bootstrap_fallback:
            brokers = self._manager.cluster.bootstrap_brokers()
        if not brokers:
            return self._manager.config['reconnect_backoff_ms']
        delays = [self._manager.connection_delay(broker.node_id) for broker in brokers]
        return min(delays) * 1000

    def connection_delay(self, node_id):
        return self._manager.connection_delay(node_id)

    def wakeup(self):
        self._net.wakeup()

    def api_version(self, operation, max_version=None):
        assert self._manager.broker_version_data is not None
        return self._manager.broker_version_data.api_version(operation, max_version=max_version)
