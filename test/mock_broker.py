"""In-memory mock broker for deterministic protocol-level tests.

Provides MockBroker (scriptable request/response handler) and MockTransport
(in-memory transport that plugs into kafka.net.connection.KafkaConnection)
so tests can exercise the full client stack without a real Kafka broker.

Usage::

    broker = MockBroker(node_id=0, host='localhost', port=9092)
    # Auto-responds to ApiVersionsRequest and MetadataRequest by default.

    # Script additional responses:
    broker.respond(JoinGroupRequest, JoinGroupResponse(version=5, ...))
    broker.respond(SyncGroupRequest, SyncGroupResponse(version=3, ...))

    # Attach to a KafkaConnectionManager so new connections use MockTransport:
    broker.attach(manager)

    # Or use the consumer/client factory fixtures for a one-liner setup.

For multi-broker scenarios (coordinator failover, leader movement), use
MockCluster, which routes connections to the matching broker by (host, port)::

    cluster = MockCluster(num_brokers=2)
    cluster.attach(manager)
    cluster.set_coordinator('my-group', 0)   # brokers answer FindCoordinator
    cluster[0].stop()   # kill broker 0: live connections abort, reconnects fail
    cluster.set_coordinator('my-group', 1)   # "election": broker 1 takes over
"""

import collections
import copy
import logging
import struct
import time
import weakref

import kafka.errors as Errors
from kafka.protocol.admin import DescribeClusterRequest, DescribeClusterResponse
from kafka.protocol.broker_version_data import BrokerVersionData
from kafka.protocol.consumer import (
    HeartbeatRequest, HeartbeatResponse,
    JoinGroupRequest, JoinGroupResponse,
    LeaveGroupRequest, LeaveGroupResponse,
    SyncGroupRequest, SyncGroupResponse,
)
from kafka.protocol.metadata import (
    ApiVersionsRequest, ApiVersionsResponse, CoordinatorType,
    FindCoordinatorRequest, FindCoordinatorResponse,
    MetadataRequest, MetadataResponse,
)


class _MockBrokerFailure:
    """Sentinel returned by MockBroker.handle_request to signal that the
    MockTransport should abort the connection with the given error, simulating
    a transport-level failure (TCP disconnect, broker crash mid-request, etc.).
    """
    __slots__ = ('error',)

    def __init__(self, error):
        self.error = error

log = logging.getLogger(__name__)


class MockTransport:
    """In-memory transport replacing KafkaTCPTransport for tests.

    Implements the transport interface that KafkaConnection expects. On
    ``write(data)``, decodes the framed Kafka request bytes, passes them to
    the attached MockBroker for handling, and delivers the response bytes
    back to the connection's ``data_received()`` via the event loop's
    ``call_soon``.

    No real sockets are created. No ``wait_read`` / ``wait_write`` calls are
    made. The event loop drives the mock the same way it drives real I/O,
    preserving the callback and future semantics that the rest of the stack
    depends on.
    """

    def __init__(self, net, broker, node_id=0, host='localhost', port=9092):
        self._net = net
        self._broker = broker
        self._node_id = node_id
        self._host = host
        # Public mirror -- KafkaTCPTransport / KafkaSSLTransport both expose
        # `host` for the SASL hostname fallback.
        self.host = host
        self._port = port
        self._protocol = None
        self._closed = False
        self._write_buffer = bytearray()
        self.last_read = time.monotonic()
        self.last_write = time.monotonic()
        broker._register_transport(self)

    @property
    def last_activity(self):
        return max(self.last_read, self.last_write)

    # -- Transport protocol (what KafkaConnection expects) --------------------

    def set_protocol(self, protocol):
        self._protocol = protocol
        log.debug('%s: Set protocol %s', self, protocol)

    def get_protocol(self):
        return self._protocol

    def is_closing(self):
        return self._closed

    def resume_reading(self):
        pass

    def pause_reading(self):
        pass

    def is_reading(self):
        return not self._closed

    def write(self, data):
        if self._closed:
            raise RuntimeError('Transport closed for writes')
        if not data:
            raise ValueError('Cant write empty data')
        self._write_buffer.extend(data)
        self.last_write = time.monotonic()
        self._net.call_soon(self._process_requests)

    def writelines(self, data_list):
        for data in data_list:
            self._write_buffer.extend(data)
        self.last_write = time.monotonic()
        self._net.call_soon(self._process_requests)

    def close(self):
        if not self._closed:
            self._closed = True
            if self._protocol:
                self._protocol.connection_lost(None)
                self._protocol = None

    def abort(self, error=None):
        if not self._closed:
            self._closed = True
            if self._protocol:
                self._protocol.connection_lost(error)
                self._protocol = None

    def can_write_eof(self):
        return False

    def write_eof(self):
        pass

    def getHost(self):
        return (self._host, 0)

    def getPeer(self):
        return (self._host, self._port)

    def host_port(self):
        return '%s:%d<-mock' % (self._host, self._port)

    async def handshake(self):
        pass

    def __str__(self):
        state = ' (closed)' if self._closed else ''
        return '<MockTransport [%s]%s>' % (self.host_port(), state)

    # -- Internal: request processing ----------------------------------------

    async def _process_requests(self):
        """Parse framed requests from the write buffer and deliver responses."""
        while len(self._write_buffer) >= 4:
            frame_size = struct.unpack('>i', self._write_buffer[:4])[0]
            total = 4 + frame_size
            if len(self._write_buffer) < total:
                break

            request_bytes = bytes(self._write_buffer[4:total])
            del self._write_buffer[:total]

            # Parse the request header to extract routing info
            api_key = struct.unpack('>h', request_bytes[0:2])[0]
            api_version = struct.unpack('>h', request_bytes[2:4])[0]
            correlation_id = struct.unpack('>i', request_bytes[4:8])[0]

            log.debug('%s: Request api_key=%d version=%d correlation_id=%d',
                      self, api_key, api_version, correlation_id)

            result = await self._broker.handle_request(
                api_key, api_version, correlation_id, request_bytes)

            if isinstance(result, _MockBrokerFailure):
                log.debug('%s: simulating transport failure: %s', self, result.error)
                self.abort(result.error)
                return

            if result is not None and self._protocol and not self._closed:
                self.last_read = time.monotonic()
                self._protocol.data_received(result)


class MockBroker:
    """Scriptable in-memory Kafka broker for testing.

    Automatically handles ``ApiVersionsRequest`` and ``MetadataRequest`` with
    configurable responses. Additional request types can be scripted via
    :meth:`respond` (matched by API key, consumed in order).

    Arguments:
        node_id (int): Broker node ID. Default 0.
        host (str): Broker hostname. Default 'localhost'.
        port (int): Broker port. Default 9092.
        broker_version (tuple): Broker version for auto-generating
            ``ApiVersionsResponse``. Default ``(4, 2)``.
    """

    def __init__(self, node_id=0, host='localhost', port=9092, broker_version=(4, 2)):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.set_broker_version(broker_version)
        self.set_metadata() # Default metadata: just this broker, no topics

        # Scripted response queue: list of (api_key, response_object) pairs
        self._response_queue = collections.deque()

        # Persistent responses checked after the scripted queue: api_key -> response/fn
        self._always_responses = {}

        # Broker lifecycle: stop() flips this and aborts live transports
        self.online = True
        self._transports = weakref.WeakSet()

        # Counters for debugging
        self.requests_received = 0
        self.responses_sent = 0

    def _register_transport(self, transport):
        self._transports.add(transport)

    def set_broker_version(self, broker_version):
        self.broker_version = broker_version
        # Build the auto-response for ApiVersionsRequest
        self._broker_version_data = BrokerVersionData(broker_version)
        ApiVersion = ApiVersionsResponse.ApiVersion
        api_keys = [ApiVersion(api_key=k, min_version=v[0], max_version=v[1])
                     for k, v in self._broker_version_data.api_versions.items()]
        self._api_versions_response = ApiVersionsResponse(
            version=min(4, ApiVersionsResponse.max_version),
            error_code=0,
            api_keys=api_keys,
            throttle_time_ms=0,
        )
        # Max version this broker supports for ApiVersionsRequest itself
        av_range = self._broker_version_data.api_versions.get(ApiVersionsRequest.API_KEY, (0, 0))
        self._api_versions_max = av_range[1]

    def set_metadata(self, topics=None, brokers=None):
        """Configure the auto-response for MetadataRequest.

        Arguments:
            topics: List of MetadataResponseTopic objects to include.
            brokers: List of MetadataResponseBroker objects. If None, defaults
                to this broker only.
        """
        Broker = MetadataResponse.MetadataResponseBroker
        if brokers is None:
            brokers = [Broker(node_id=self.node_id, host=self.host, port=self.port, rack=None)]
        self._metadata_response = MetadataResponse(
            version=self._broker_version_data.api_version(MetadataRequest, max_version=8),
            throttle_time_ms=0,
            brokers=brokers,
            cluster_id='mock-cluster',
            controller_id=self.node_id,
            topics=topics or [],
        )
        try:
            DCBroker = DescribeClusterResponse.DescribeClusterBroker
            self._describe_cluster_response = DescribeClusterResponse(
                version=self._broker_version_data.api_version(DescribeClusterRequest),
                throttle_time_ms=0,
                error_code=0,
                error_message=None,
                endpoint_type=1,
                cluster_id='mock-cluster',
                controller_id=self.node_id,
                brokers=[DCBroker(broker_id=b.node_id, host=b.host, port=b.port, rack=b.rack, is_fenced=False)
                         for b in brokers],
                authorized_operations=set(),
            )
        except Errors.IncompatibleBrokerVersion:
            self._describe_cluster_response = None

    def respond(self, request_class, response):
        """Enqueue a scripted response for the next request of the given type.

        Responses are matched by API key and consumed in FIFO order. If
        multiple responses are queued for the same API key, they are returned
        in the order they were enqueued.

        Arguments:
            request_class: The request class (e.g., ``JoinGroupRequest``).
                Used to extract the API key.
            response: A response object to return when the matching request
                arrives. The response's ``correlation_id`` and ``version``
                will be set automatically to match the incoming request.
        """
        self._response_queue.append((request_class.API_KEY, response))

    def respond_fn(self, request_class, fn):
        """Enqueue a callable that generates a response dynamically.

        The callable may be a regular function or an ``async def`` coroutine.
        Coroutines are awaited by the mock transport's event loop, which means
        they can use ``await net.sleep(delay)`` to simulate broker processing
        latency or ``await some_future`` to synchronize with test code.

        Arguments:
            request_class: The request class for API key matching.
            fn: Callable or coroutine function
                ``(api_key, api_version, correlation_id, request_bytes) -> response_object``.
                The returned response's correlation_id and version will be
                set automatically.
        """
        self._response_queue.append((request_class.API_KEY, fn))

    def respond_always(self, request_class, response):
        """Set a persistent response for all requests of the given type.

        Unlike :meth:`respond`, the response is not consumed -- every matching
        request gets it. Scripted queue entries (:meth:`respond` /
        :meth:`respond_fn` / :meth:`fail_next`) take precedence; built-in
        auto-responses (ApiVersions / Metadata / DescribeCluster) are checked
        after, so a persistent response can override them. Useful for periodic
        traffic like heartbeats.

        Arguments:
            request_class: The request class for API key matching.
            response: A response object, or a callable / coroutine function
                with the same signature as :meth:`respond_fn`.
        """
        self._always_responses[request_class.API_KEY] = response

    def stop(self, error=None):
        """Take the broker down, as if the process was killed.

        Aborts all live transports with ``error`` and refuses new connections
        (an attached manager's ``net.create_connection`` raises
        ``KafkaConnectionError``) until :meth:`start` is called. This
        exercises the client's real connection-lost, connect-failure, and
        reconnect-backoff paths.

        Arguments:
            error: Exception delivered to live transports' ``abort()``.
                Defaults to ``Errors.KafkaConnectionError``.
        """
        if error is None:
            error = Errors.KafkaConnectionError('MockBroker stopped')
        self.online = False
        for transport in list(self._transports):
            if transport.is_closing():
                continue
            # abort() must run on the event loop: connection_lost mutates
            # state the loop owns. call_soon_threadsafe works both when the
            # loop runs on an IO thread and when a test drives poll() inline.
            try:
                transport._net.call_soon_threadsafe(lambda t=transport: t.abort(error))
            except RuntimeError:
                pass  # selector already closed; nothing left to abort

    def start(self):
        """Bring a stopped broker back online. New connections succeed again."""
        self.online = True

    def fail_next(self, request_class, error=None):
        """Enqueue a transport failure for the next request of the given type.

        When the matching request arrives, the MockTransport aborts the
        connection with ``error`` instead of returning a response, simulating
        a transport-level send failure (TCP disconnect, broker crash
        mid-request, etc.). The pending request's Future then fails via the
        connection's ``connection_lost`` -> ``fail_in_flight_requests`` path.

        Arguments:
            request_class: The request class for API key matching.
            error: Exception delivered to ``transport.abort()``. Defaults to
                ``Errors.KafkaConnectionError``.
        """
        if error is None:
            error = Errors.KafkaConnectionError('MockBroker.fail_next')
        self._response_queue.append((request_class.API_KEY, _MockBrokerFailure(error)))

    async def handle_request(self, api_key, api_version, correlation_id, request_bytes):
        """Process a request and return framed response bytes.

        This is a coroutine so that ``respond_fn`` callables can themselves
        be coroutines (e.g. to inject delays via ``await net.sleep(...)``).

        Called by MockTransport for each decoded request frame.

        Returns:
            bytes: Framed response ready for ``protocol.data_received()``,
            or None if the request expects no response, or
            ``_MockBrokerFailure`` to signal that the transport should abort.
        """
        self.requests_received += 1

        # Check the scripted queue first (match by api_key, FIFO)
        for i, (queued_key, queued_response) in enumerate(self._response_queue):
            if queued_key == api_key:
                del self._response_queue[i]
                if isinstance(queued_response, _MockBrokerFailure):
                    return queued_response
                response = await self._resolve_response(
                    queued_response, api_key, api_version, correlation_id, request_bytes)
                # A resolved response of None means "request handled, send
                # nothing back" -- the real broker behavior for acks=0
                # ProduceRequests, which the client does not expect a response to.
                if response is None:
                    return None
                return self._encode_response(response, api_version, correlation_id)

        # Then persistent responses set via respond_always
        if api_key in self._always_responses:
            response = await self._resolve_response(
                self._always_responses[api_key], api_key, api_version,
                correlation_id, request_bytes)
            if response is None:
                return None
            return self._encode_response(response, api_version, correlation_id)

        # Fall back to auto-responses
        if api_key == ApiVersionsRequest.API_KEY:
            if api_version > self._api_versions_max:
                # Client requested a newer version than the broker supports.
                # Return UnsupportedVersionError so the client can downgrade
                # (mirrors real broker behavior; see KafkaConnection._check_version
                # lines 355-362).
                #
                # Real broker behavior (KIP-511, Kafka 2.4+):
                #   v0-v2: error response with api_keys=[] (empty)
                #   v3+:   error response with api_keys containing only the
                #          ApiVersions entry so the client can jump directly
                #          to the right version without falling all the way to v0.
                #
                # Error is always encoded at v0 for backwards-compatibility.
                ApiVersion = ApiVersionsResponse.ApiVersion
                api_key = ApiVersion(api_key=18, min_version=0, max_version=self._api_versions_max)
                error_response = ApiVersionsResponse(
                    version=0,
                    error_code=35,  # UnsupportedVersionError
                    api_keys=[api_key] if api_version >= 3 else [],
                    throttle_time_ms=0,
                )
                return self._encode_response(error_response, 0, correlation_id)
            return self._encode_response(
                self._api_versions_response, api_version, correlation_id)

        if api_key == MetadataRequest.API_KEY:
            return self._encode_response(
                self._metadata_response, api_version, correlation_id)

        if api_key == DescribeClusterRequest.API_KEY and self._describe_cluster_response is not None:
            return self._encode_response(
                self._describe_cluster_response, api_version, correlation_id)

        raise ValueError(
            'MockBroker: no handler for api_key=%d version=%d '
            '(correlation_id=%d). Scripted queue has %d entries: %s'
            % (api_key, api_version, correlation_id,
               len(self._response_queue),
               [(k, type(r).__name__) for k, r in self._response_queue]))

    @staticmethod
    async def _resolve_response(response, api_key, api_version, correlation_id, request_bytes):
        """Resolve a scripted entry to a response object, calling/awaiting
        respond_fn-style callables as needed."""
        if callable(response):
            response = response(api_key, api_version, correlation_id, request_bytes)
            # Support both sync and async respond_fn callables
            if hasattr(response, '__await__'):
                response = await response
        return response

    def attach(self, manager):
        """Monkey-patch a KafkaConnectionManager to route all connections
        through this MockBroker.

        After calling this, any ``manager.get_connection(node_id)`` call will
        create a KafkaConnection backed by a MockTransport connected to this
        broker, instead of opening a real TCP socket.

        Arguments:
            manager: A ``KafkaConnectionManager`` instance.
        """
        broker = self

        async def _mock_create_connection(protocol, host, port, **kwargs):
            if not broker.online:
                raise Errors.KafkaConnectionError(
                    'connect to %s:%s refused (MockBroker stopped)'
                    % (host, port))
            return MockTransport(
                manager._net, broker,
                node_id=protocol.node_id, host=host, port=port)

        manager._net.create_connection = _mock_create_connection

    def client_factory(self):
        """Return a callable suitable for passing as ``kafka_client=...``
        to ``KafkaConsumer``, ``KafkaProducer``, or ``KafkaAdminClient``.

        The factory creates a real ``KafkaNetClient`` but patches its
        connection manager to use this MockBroker before any connections
        are established (including bootstrap).

        Usage::

            broker = MockBroker()
            consumer = KafkaConsumer(kafka_client=broker.client_factory(), ...)
            admin = KafkaAdminClient(kafka_client=broker.client_factory(), ...)
        """
        broker = self

        def factory(**kwargs):
            from kafka.net.compat import KafkaNetClient
            client = KafkaNetClient(**kwargs)
            broker.attach(client._manager)
            return client

        return factory

    @staticmethod
    def _encode_response(response, api_version, correlation_id):
        """Encode a response object to framed wire bytes with the correct
        correlation_id and version.

        Makes a shallow copy so the original response object can be reused
        across multiple requests (important for auto-responses like
        ApiVersions and Metadata which are shared across all connections).
        """
        version = min(api_version, response.max_version)
        r = copy.copy(response)
        r._header = None
        r.API_VERSION = version
        r.with_header(correlation_id=correlation_id)
        return r.encode(header=True, framed=True)


class MockCluster:
    """A set of MockBrokers routed by (host, port) for multi-broker tests.

    Broker ``i`` gets ``node_id=i`` and ``port=base_port + i``. All brokers
    advertise the same broker list via :meth:`set_metadata`; individual
    brokers can still be given divergent metadata with
    ``cluster[i].set_metadata(...)`` to simulate stale views.

    Scripting is per-broker via the usual MockBroker API, and brokers can be
    taken down and brought back with ``cluster[i].stop()`` / ``start()``.

    Arguments:
        num_brokers (int): Number of brokers. Default 3.
        broker_version (tuple): Version for all brokers. Default ``(4, 2)``.
        host (str): Hostname shared by all brokers. Default 'localhost'.
        base_port (int): Port of broker 0. Default 9092.
    """

    def __init__(self, num_brokers=3, broker_version=(4, 2), host='localhost', base_port=9092):
        self.broker_version = broker_version
        self.brokers = [
            MockBroker(node_id=i, host=host, port=base_port + i,
                       broker_version=broker_version)
            for i in range(num_brokers)
        ]
        self._by_addr = {(b.host, b.port): b for b in self.brokers}
        self._coordinators = {}
        self._groups = {}
        self.set_metadata()

    def __getitem__(self, node_id):
        return self.brokers[node_id]

    def __iter__(self):
        return iter(self.brokers)

    def __len__(self):
        return len(self.brokers)

    def bootstrap_servers(self):
        """Comma-separated host:port list covering all brokers, for client config."""
        return ','.join('%s:%d' % (b.host, b.port) for b in self.brokers)

    def set_metadata(self, topics=None):
        """Configure a consistent MetadataRequest auto-response on every broker.

        Arguments:
            topics: List of MetadataResponseTopic objects to include.
        """
        Broker = MetadataResponse.MetadataResponseBroker
        brokers = [Broker(node_id=b.node_id, host=b.host, port=b.port, rack=None)
                   for b in self.brokers]
        for b in self.brokers:
            b.set_metadata(topics=topics, brokers=brokers)

    def set_coordinator(self, key, broker_id, key_type=CoordinatorType.GROUP):
        """Name a broker as coordinator for a group / transactional id.

        After the first call, every broker auto-answers
        ``FindCoordinatorRequest`` from the cluster's coordinator map
        (scripted ``respond()`` / ``respond_fn()`` entries still take
        precedence, and a per-broker ``respond_always()`` for
        FindCoordinator overrides the cluster handler on that broker).
        Keys without a mapping are answered with COORDINATOR_NOT_AVAILABLE,
        so tests can model election gaps.

        The map does not react to :meth:`MockBroker.stop`: like a real
        cluster mid-failover, surviving brokers keep naming the dead
        coordinator until the test re-points the key.

        Arguments:
            key (str): group_id or transactional_id.
            broker_id (int or None): Index of the coordinator broker. None
                clears the mapping (subsequent lookups get
                COORDINATOR_NOT_AVAILABLE).
            key_type (CoordinatorType or int): GROUP (0), TRANSACTION (1),
                or SHARE (2). Default: GROUP.
        """
        if broker_id is None:
            self._coordinators.pop((int(key_type), key), None)
        else:
            self._coordinators[(int(key_type), key)] = self.brokers[broker_id].node_id
        for b in self.brokers:
            b.respond_always(FindCoordinatorRequest, self._handle_find_coordinator)

    def _handle_find_coordinator(self, api_key, api_version, correlation_id, request_bytes):
        request = FindCoordinatorRequest.decode(
            request_bytes, version=api_version, header=True)
        # v4+ (KIP-699) carry the keys in an array; v0-v3 a single key field.
        keys = list(request.coordinator_keys) or [request.key]
        Coordinator = FindCoordinatorResponse.Coordinator
        coordinators = []
        for key in keys:
            broker_id = self._coordinators.get((request.key_type, key))
            if broker_id is None:
                coordinators.append(Coordinator(
                    key=key, node_id=-1, host='', port=-1,
                    error_code=Errors.CoordinatorNotAvailableError.errno,
                    error_message=None))
            else:
                broker = self.brokers[broker_id]
                coordinators.append(Coordinator(
                    key=key, node_id=broker.node_id, host=broker.host,
                    port=broker.port, error_code=0, error_message=None))
        # Top-level fields serve v0-v3 (single key); the array serves v4+.
        first = coordinators[0]
        return FindCoordinatorResponse(
            throttle_time_ms=0,
            error_code=first.error_code, error_message=first.error_message,
            node_id=first.node_id, host=first.host, port=first.port,
            coordinators=coordinators)

    def add_group(self, group_id, coordinator=0):
        """Install a minimal classic-protocol group coordinator on a broker.

        Points ``FindCoordinator`` for ``group_id`` at the ``coordinator``
        broker (via :meth:`set_coordinator`) and registers a :class:`MockGroup`
        there that auto-answers JoinGroup / SyncGroup / Heartbeat / LeaveGroup,
        so a real consumer can complete a rebalance without the test scripting
        each response. Returns the :class:`MockGroup` for inspection and
        rebalance control (e.g. ``group.trigger_rebalance()``).

        Arguments:
            group_id (str): consumer group id.
            coordinator (int): index of the coordinator broker. Default 0.
        """
        self.set_coordinator(group_id, coordinator)
        group = MockGroup(self.brokers[coordinator], group_id)
        self._groups[group_id] = group
        return group

    def attach(self, manager):
        """Monkey-patch a KafkaConnectionManager to route each new connection
        to the cluster member matching the target node's (host, port).

        Routing by address (rather than node_id) also covers bootstrap
        connections, which use synthetic node ids like 'bootstrap-0', and
        synthesized coordinator ids like 'coordinator-1'. Connection attempts
        to an unknown address or a stopped broker raise
        ``KafkaConnectionError``, like a real connect to a dead host.

        Arguments:
            manager: A ``KafkaConnectionManager`` instance.
        """
        cluster = self

        async def _mock_create_connection(protocol, host, port, **kwargs):
            broker = cluster._by_addr.get((host, port))
            if broker is None or not broker.online:
                raise Errors.KafkaConnectionError(
                    'connect to %s:%s refused' % (host, port))
            return MockTransport(
                manager._net, broker,
                node_id=protocol.node_id, host=host, port=port)

        manager._net.create_connection = _mock_create_connection

    def client_factory(self):
        """Return a callable suitable for passing as ``kafka_client=...`` to
        ``KafkaConsumer``, ``KafkaProducer``, or ``KafkaAdminClient``.

        Pass ``bootstrap_servers=cluster.bootstrap_servers()`` alongside so
        the client's bootstrap addresses match the cluster's brokers.
        """
        cluster = self

        def factory(**kwargs):
            from kafka.net.compat import KafkaNetClient
            client = KafkaNetClient(**kwargs)
            cluster.attach(client._manager)
            return client

        return factory


def _as_bytes(value):
    """Coerce a decoded metadata/assignment field (bytes or memoryview) to bytes."""
    if isinstance(value, bytes):
        return value
    return bytes(value)


class MockGroup:
    """Minimal classic-protocol consumer-group coordinator for a MockBroker.

    Auto-answers JoinGroup / SyncGroup / Heartbeat / LeaveGroup so a real
    consumer (or ConsumerCoordinator) can complete a rebalance against a mock
    without the test scripting each response. Models the common case:

    - The first member to join (or whoever sent a non-empty member_id) is
      elected leader; only the leader receives the full member list so it can
      run the assignor.
    - SyncGroup routes the leader-computed assignment back to each member --
      the broker just echoes ``assignments[member_id]`` from the request.
    - Heartbeat returns NoError, unless :meth:`trigger_rebalance` armed a
      one-shot RebalanceInProgress to drive a broker-initiated rejoin.

    This is deliberately not a full broker: no session-timeout eviction, no
    generation fencing beyond what the handlers below do, classic
    JoinGroup/SyncGroup only (not KIP-848). Use :attr:`members` /
    :attr:`generation` / :attr:`leader` for assertions.

    Typically created via :meth:`MockCluster.add_group`.
    """

    def __init__(self, broker, group_id):
        self.broker = broker
        self.group_id = group_id
        self.generation = 0
        self.members = {}          # member_id -> subscription metadata bytes
        self.leader = None
        self._next_member_id = 0
        self._force_rebalance = False
        broker.respond_always(JoinGroupRequest, self._on_join)
        broker.respond_always(SyncGroupRequest, self._on_sync)
        broker.respond_always(HeartbeatRequest, self._on_heartbeat)
        broker.respond_always(LeaveGroupRequest, self._on_leave)

    def trigger_rebalance(self):
        """Arm a one-shot RebalanceInProgress on the next Heartbeat, forcing
        the consumer to rejoin (a broker-initiated rebalance)."""
        self._force_rebalance = True

    def _on_join(self, api_key, api_version, correlation_id, request_bytes):
        request = JoinGroupRequest.decode(request_bytes, version=api_version, header=True)
        member_id = request.member_id
        if not member_id:
            member_id = '%s-member-%d' % (self.group_id, self._next_member_id)
            self._next_member_id += 1
        # Record/refresh this member's subscription (first supported protocol).
        protocol = request.protocols[0]
        self.members[member_id] = _as_bytes(protocol.metadata)
        if self.leader is None or self.leader not in self.members:
            self.leader = member_id
        self.generation += 1

        Member = JoinGroupResponse.JoinGroupResponseMember
        # Only the leader needs the member list (to compute the assignment).
        members = []
        if member_id == self.leader:
            members = [Member(member_id=m, group_instance_id=None, metadata=md)
                       for m, md in self.members.items()]
        return JoinGroupResponse(
            throttle_time_ms=0, error_code=0,
            generation_id=self.generation,
            protocol_type='consumer',
            protocol_name=protocol.name,
            leader=self.leader,
            member_id=member_id,
            members=members)

    def _on_sync(self, api_key, api_version, correlation_id, request_bytes):
        request = SyncGroupRequest.decode(request_bytes, version=api_version, header=True)
        # The leader carries every member's assignment; a follower sends none.
        # Route this member's slice back (empty if not found).
        assignment = b''
        for entry in request.assignments:
            if entry.member_id == request.member_id:
                assignment = _as_bytes(entry.assignment)
                break
        return SyncGroupResponse(
            throttle_time_ms=0, error_code=0,
            protocol_type='consumer',
            protocol_name=request.protocol_name,
            assignment=assignment)

    def _on_heartbeat(self, api_key, api_version, correlation_id, request_bytes):
        if self._force_rebalance:
            self._force_rebalance = False
            return HeartbeatResponse(
                throttle_time_ms=0,
                error_code=Errors.RebalanceInProgressError.errno)
        return HeartbeatResponse(throttle_time_ms=0, error_code=0)

    def _on_leave(self, api_key, api_version, correlation_id, request_bytes):
        request = LeaveGroupRequest.decode(request_bytes, version=api_version, header=True)
        Member = LeaveGroupResponse.MemberResponse
        members = []
        for m in getattr(request, 'members', None) or []:
            members.append(Member(member_id=m.member_id, error_code=0))
            self.members.pop(m.member_id, None)
        return LeaveGroupResponse(throttle_time_ms=0, error_code=0, members=members)
