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
"""

import collections
import copy
import logging
import struct
import time

from kafka.protocol.broker_version_data import BrokerVersionData
from kafka.protocol.metadata import ApiVersionsRequest, ApiVersionsResponse, MetadataRequest, MetadataResponse

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
        self._port = port
        self._protocol = None
        self._closed = False
        self._write_buffer = bytearray()
        self.last_read = time.monotonic()
        self.last_write = time.monotonic()

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

            response_bytes = await self._broker.handle_request(
                api_key, api_version, correlation_id, request_bytes)

            if response_bytes is not None and self._protocol and not self._closed:
                self.last_read = time.monotonic()
                self._protocol.data_received(response_bytes)


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

        # Default metadata: just this broker, no topics
        Broker = MetadataResponse.MetadataResponseBroker
        self._metadata_response = MetadataResponse(
            version=min(8, MetadataResponse.max_version),
            throttle_time_ms=0,
            brokers=[Broker(node_id=node_id, host=host, port=port, rack=None)],
            cluster_id='mock-cluster',
            controller_id=node_id,
            topics=[],
        )

        # Scripted response queue: list of (api_key, response_object) pairs
        self._response_queue = collections.deque()

        # Counters for debugging
        self.requests_received = 0
        self.responses_sent = 0

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
            version=min(8, MetadataResponse.max_version),
            throttle_time_ms=0,
            brokers=brokers,
            cluster_id='mock-cluster',
            controller_id=self.node_id,
            topics=topics or [],
        )

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

    async def handle_request(self, api_key, api_version, correlation_id, request_bytes):
        """Process a request and return framed response bytes.

        This is a coroutine so that ``respond_fn`` callables can themselves
        be coroutines (e.g. to inject delays via ``await net.sleep(...)``).

        Called by MockTransport for each decoded request frame.

        Returns:
            bytes: Framed response ready for ``protocol.data_received()``,
            or None if the request expects no response.
        """
        self.requests_received += 1

        # Check the scripted queue first (match by api_key, FIFO)
        for i, (queued_key, queued_response) in enumerate(self._response_queue):
            if queued_key == api_key:
                del self._response_queue[i]
                if callable(queued_response):
                    response = queued_response(api_key, api_version, correlation_id, request_bytes)
                    # Support both sync and async respond_fn callables
                    if hasattr(response, '__await__'):
                        response = await response
                else:
                    response = queued_response
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

        raise ValueError(
            'MockBroker: no handler for api_key=%d version=%d '
            '(correlation_id=%d). Scripted queue has %d entries: %s'
            % (api_key, api_version, correlation_id,
               len(self._response_queue),
               [(k, type(r).__name__) for k, r in self._response_queue]))

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

        async def _mock_build_transport(node):
            return MockTransport(
                manager._net, broker,
                node_id=node.node_id, host=node.host, port=node.port)

        manager._build_transport = _mock_build_transport

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
