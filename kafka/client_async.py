import copy
import heapq
import itertools
import logging
import random
import select
import sys
import time

import six

import kafka.common as Errors # TODO: make Errors a separate class

from .cluster import ClusterMetadata
from .conn import BrokerConnection, ConnectionStates, collect_hosts
from .future import Future
from .protocol.metadata import MetadataRequest
from .protocol.produce import ProduceRequest
from .version import __version__

log = logging.getLogger(__name__)


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
        'receive_buffer_bytes': 32768,
        'send_buffer_bytes': 131072,
    }

    def __init__(self, **configs):
        self.config = copy.copy(self.DEFAULT_CONFIG)
        for key in self.config:
            if key in configs:
                self.config[key] = configs[key]

        self.cluster = ClusterMetadata(**self.config)
        self._topics = set() # empty set will fetch all topic metadata
        self._metadata_refresh_in_progress = False
        self._conns = {}
        self._connecting = set()
        self._delayed_tasks = DelayedTaskQueue()
        self._last_bootstrap = 0
        self._bootstrap_fails = 0
        self._bootstrap(collect_hosts(self.config['bootstrap_servers']))

    def _bootstrap(self, hosts):
        # Exponential backoff if bootstrap fails
        backoff_ms = self.config['reconnect_backoff_ms'] * 2 ** self._bootstrap_fails
        next_at = self._last_bootstrap + backoff_ms / 1000.0
        now = time.time()
        if next_at > now:
            log.debug("Sleeping %0.4f before bootstrapping again", next_at - now)
            time.sleep(next_at - now)
        self._last_bootstrap = time.time()

        metadata_request = MetadataRequest([])
        for host, port in hosts:
            log.debug("Attempting to bootstrap via node at %s:%s", host, port)
            bootstrap = BrokerConnection(host, port, **self.config)
            bootstrap.connect()
            while bootstrap.state is ConnectionStates.CONNECTING:
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
            self._bootstrap_fails = 0
            break
        # No bootstrap found...
        else:
            log.error('Unable to bootstrap from %s', hosts)
            # Max exponential backoff is 2^12, x4000 (50ms -> 200s)
            self._bootstrap_fails = min(self._bootstrap_fails + 1, 12)

    def _can_connect(self, node_id):
        if node_id not in self._conns:
            if self.cluster.broker_metadata(node_id):
                return True
            return False
        conn = self._conns[node_id]
        return conn.state is ConnectionStates.DISCONNECTED and not conn.blacked_out()

    def _initiate_connect(self, node_id):
        """Initiate a connection to the given node"""
        broker = self.cluster.broker_metadata(node_id)
        if not broker:
            raise Errors.IllegalArgumentError('Broker %s not found in current cluster metadata', node_id)

        if node_id not in self._conns:
            log.debug("Initiating connection to node %s at %s:%s",
                      node_id, broker.host, broker.port)
            self._conns[node_id] = BrokerConnection(broker.host, broker.port,
                                                    **self.config)
        return self._finish_connect(node_id)

    def _finish_connect(self, node_id):
        if node_id not in self._conns:
            raise Errors.IllegalArgumentError('Node %s not found in connections', node_id)
        state = self._conns[node_id].connect()
        if state is ConnectionStates.CONNECTING:
            self._connecting.add(node_id)
        elif node_id in self._connecting:
            log.debug("Node %s connection state is %s", node_id, state)
            self._connecting.remove(node_id)
        return state

    def ready(self, node_id):
        """
        Begin connecting to the given node, return true if we are already
        connected and ready to send to that node.

        @param node_id The id of the node to check
        @return True if we are ready to send to the given node
        """
        if self.is_ready(node_id):
            return True

        if self._can_connect(node_id):
            # if we are interested in sending to a node
            # and we don't have a connection to it, initiate one
            self._initiate_connect(node_id)

        if node_id in self._connecting:
            self._finish_connect(node_id)

        return self.is_ready(node_id)

    def close(self, node_id=None):
        """Closes the connection to a particular node (if there is one).

        @param node_id The id of the node
        """
        if node_id is None:
            for conn in self._conns.values():
                conn.close()
        elif node_id in self._conns:
            self._conns[node_id].close()
        else:
            log.warning("Node %s not found in current connection list; skipping", node_id)
            return

    def connection_delay(self, node_id):
        """
        Returns the number of milliseconds to wait, based on the connection
        state, before attempting to send data. When disconnected, this respects
        the reconnect backoff time. When connecting or connected, this handles
        slow/stalled connections.

        @param node_id The id of the node to check
        @return The number of milliseconds to wait.
        """
        if node_id not in self._conns:
            return 0

        conn = self._conns[node_id]
        time_waited_ms = time.time() - (conn.last_attempt or 0)
        if conn.state is ConnectionStates.DISCONNECTED:
            return max(self.config['reconnect_backoff_ms'] - time_waited_ms, 0)
        else:
            return sys.maxint

    def connection_failed(self, node_id):
        """
        Check if the connection of the node has failed, based on the connection
        state. Such connection failures are usually transient and can be resumed
        in the next ready(node) call, but there are cases where transient
        failures need to be caught and re-acted upon.

        @param node_id the id of the node to check
        @return true iff the connection has failed and the node is disconnected
        """
        if node_id not in self._conns:
            return False
        return self._conns[node_id].state is ConnectionStates.DISCONNECTED

    def is_ready(self, node_id):
        """
        Check if the node with the given id is ready to send more requests.

        @param node_id The id of the node
        @return true if the node is ready
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
        """
        Send the given request. Requests can only be sent out to ready nodes.

        @param node destination node
        @param request The request
        @param now The current timestamp
        """
        if not self._can_send_request(node_id):
            raise Errors.IllegalStateError("Attempt to send a request to node %s which is not ready." % node_id)

        # Every request gets a response, except one special case:
        expect_response = True
        if isinstance(request, ProduceRequest) and request.required_acks == 0:
            expect_response = False

        return self._conns[node_id].send(request, expect_response=expect_response)

    def poll(self, timeout_ms=None, future=None):
        """Do actual reads and writes to sockets.

        @param timeout_ms The maximum amount of time to wait (in ms) for
                          responses if there are none available immediately.
                          Must be non-negative. The actual timeout will be the
                          minimum of timeout, request timeout and metadata
                          timeout. If unspecified, default to request_timeout_ms
        @param future Optionally block until the provided future completes.
        @return The list of responses received.
        """
        if timeout_ms is None:
            timeout_ms = self.config['request_timeout_ms']

        responses = []

        # Loop for futures, break after first loop if None
        while True:

            # Attempt to complete pending connections
            for node_id in list(self._connecting):
                self._finish_connect(node_id)

            # Send a metadata request if needed
            metadata_timeout = self._maybe_refresh_metadata()

            # Send scheduled tasks
            for task in self._delayed_tasks.pop_ready():
                try:
                    task()
                except Exception as e:
                    log.error("Task %s failed: %s", task, e)

            timeout = min(timeout_ms, metadata_timeout,
                          self.config['request_timeout_ms'])
            timeout /= 1000.0

            responses.extend(self._poll(timeout))
            if not future or future.is_done:
                break

        return responses

    def _poll(self, timeout):
        # select on reads across all connected sockets, blocking up to timeout
        sockets = [conn._sock for conn in six.itervalues(self._conns)
                   if (conn.state is ConnectionStates.CONNECTED and
                       conn.in_flight_requests)]
        if sockets:
            select.select(sockets, [], [], timeout)

        responses = []
        # list, not iterator, because inline callbacks may add to self._conns
        for conn in list(self._conns.values()):
            if conn.state is ConnectionStates.CONNECTING:
                conn.connect()

            if conn.in_flight_requests:
                response = conn.recv() # This will run callbacks / errbacks
                if response:
                    responses.append(response)
        return responses

    def in_flight_request_count(self, node_id=None):
        """Get the number of in-flight requests"""
        if node_id is not None:
            if node_id not in self._conns:
                return 0
            return len(self._conns[node_id].in_flight_requests)
        else:
            return sum([len(conn.in_flight_requests) for conn in self._conns.values()])

    def least_loaded_node(self):
        """
        Choose the node with the fewest outstanding requests which is at least
        eligible for connection. This method will prefer a node with an
        existing connection, but will potentially choose a node for which we
        don't yet have a connection if all existing connections are in use.
        This method will never choose a node for which there is no existing
        connection and from which we have disconnected within the reconnect
        backoff period.

        @return The node_id with the fewest in-flight requests.
        """
        nodes = list(self._conns.keys())
        random.shuffle(nodes)
        inflight = sys.maxint
        found = None
        for node_id in nodes:
            conn = self._conns[node_id]
            curr_inflight = len(conn.in_flight_requests)
            if curr_inflight == 0 and conn.connected():
                # if we find an established connection with no in-flight requests we can stop right away
                return node_id
            elif not conn.blacked_out() and curr_inflight < inflight:
                # otherwise if this is the best we have found so far, record that
                inflight = curr_inflight
                found = node_id

        if found is not None:
            return found

        # if we found no connected node, return a disconnected one
        log.debug("No connected nodes found. Trying disconnected nodes.")
        for node_id in nodes:
            if not self._conns[node_id].is_blacked_out():
                return node_id

        # if still no luck, look for a node not in self._conns yet
        log.debug("No luck. Trying all broker metadata")
        for broker in self.cluster.brokers():
            if broker.nodeId not in self._conns:
                return broker.nodeId

        # Last option: try to bootstrap again
        log.error('No nodes found in metadata -- retrying bootstrap')
        self._bootstrap(collect_hosts(self.config['bootstrap_servers']))
        return None

    def set_topics(self, topics):
        """
        Set specific topics to track for metadata

        Returns a future that will complete after metadata request/response
        """
        if set(topics).difference(self._topics):
            future = self.cluster.request_update()
        else:
            future = Future().success(set(topics))
        self._topics = set(topics)
        return future

    # request metadata update on disconnect and timedout
    def _maybe_refresh_metadata(self):
        """Send a metadata request if needed"""
        ttl = self.cluster.ttl()
        if ttl > 0:
            return ttl

        if self._metadata_refresh_in_progress:
            return sys.maxint

        node_id = self.least_loaded_node()

        if self._can_send_request(node_id):
            request = MetadataRequest(list(self._topics))
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
            self._initiate_connect(node_id)

        return 0

    def schedule(self, task, at):
        """
        Schedule a new task to be executed at the given time.

        This is "best-effort" scheduling and should only be used for coarse
        synchronization. A task cannot be scheduled for multiple times
        simultaneously; any previously scheduled instance of the same task
        will be cancelled.

        @param task The task to be scheduled -- function or implement __call__
        @param at Epoch seconds when it should run (see time.time())
        @returns Future
        """
        return self._delayed_tasks.add(task, at)

    def unschedule(self, task):
        """
        Unschedule a task. This will remove all instances of the task from the task queue.
        This is a no-op if the task is not scheduled.

        @param task The task to be unscheduled.
        """
        self._delayed_tasks.remove(task)


class DelayedTaskQueue(object):
    # see https://docs.python.org/2/library/heapq.html
    def __init__(self):
        self._tasks = [] # list of entries arranged in a heap
        self._task_map = {} # mapping of tasks to entries
        self._counter = itertools.count() # unique sequence count

    def add(self, task, at):
        """Add a task to run at a later time

        task: anything
        at: seconds from epoch to schedule task (see time.time())
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
        """Remove a previously scheduled task

        Raises KeyError if task is not found
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
        return task

    def next_at(self):
        """Number of seconds until next task is ready"""
        self._drop_removed()
        if not self._tasks:
            return sys.maxint
        else:
            return max(self._tasks[0][0] - time.time(), 0)

    def pop_ready(self):
        """Pop and return a list of all ready (task, future) tuples"""
        self._drop_removed()
        ready_tasks = []
        while self._tasks and self._tasks[0][0] < time.time():
            ready_tasks.append(self._pop_next())
        return ready_tasks
