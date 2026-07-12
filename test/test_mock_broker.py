"""Tests for the MockBroker / MockTransport infrastructure.

These tests validate that the mock infrastructure correctly bridges into
kafka.net's KafkaConnection -> KafkaProtocol -> callback chain, so that
higher-level tests can rely on MockBroker for deterministic protocol-level
testing without a real Kafka broker.
"""

import time

import pytest

from kafka.admin.client import KafkaAdminClient
from kafka.net.compat import KafkaNetClient
from kafka.protocol.admin import CreateTopicsRequest
from kafka.protocol.metadata import MetadataRequest, MetadataResponse
from kafka.structs import TopicPartition

from test.mock_broker import MockBroker, MockCluster


def _poll_for_future(client, future, timeout_ms=5000):
    """Poll in a loop until the future completes.

    A single ``client.poll(future=f)`` call may return early because
    ``compat.poll`` caps the inner timeout to ``cluster.ttl()`` (the metadata
    retry backoff). Looping mimics what real callers like ``_wait_for_futures``
    do - each iteration re-enters ``compat.poll``, re-checks ttl, and
    eventually triggers the metadata refresh when ttl drops to 0.
    """
    deadline = time.monotonic() + timeout_ms / 1000
    while not future.is_done and time.monotonic() < deadline:
        client.poll(future=future, timeout_ms=min(200, timeout_ms))
    return future


def _run_coroutine(coro):
    """Drive a coroutine that is expected to complete without suspending,
    returning its value. Used to unit-test MockBroker.handle_request for
    synchronous (non-awaiting) respond_fns."""
    try:
        coro.send(None)
    except StopIteration as stop:
        return stop.value
    raise AssertionError('coroutine suspended unexpectedly')


# ---------------------------------------------------------------------------
# MockBroker unit tests (no event loop, no connections)
# ---------------------------------------------------------------------------


class TestMockBrokerUnit:
    """Test MockBroker's request handling and response encoding without
    any networking."""

    def test_default_construction(self):
        broker = MockBroker()
        assert broker.node_id == 0
        assert broker.host == 'localhost'
        assert broker.port == 9092
        assert broker.broker_version == (4, 2)
        assert broker.requests_received == 0

    def test_set_metadata_with_topics(self):
        broker = MockBroker()
        Topic = MetadataResponse.MetadataResponseTopic
        Partition = Topic.MetadataResponsePartition
        broker.set_metadata(topics=[
            Topic(version=8, error_code=0, name='test-topic', is_internal=False,
                  partitions=[
                      Partition(version=8, error_code=0, partition_index=0,
                                leader_id=0, leader_epoch=0,
                                replica_nodes=[0], isr_nodes=[0], offline_replicas=[]),
                  ]),
        ])
        # Verify the metadata response is updated
        assert broker._metadata_response is not None

    def test_respond_queues_in_order(self):
        broker = MockBroker()
        r1 = MetadataResponse(version=8, throttle_time_ms=0, brokers=[], cluster_id='a',
                               controller_id=0, topics=[])
        r2 = MetadataResponse(version=8, throttle_time_ms=0, brokers=[], cluster_id='b',
                               controller_id=0, topics=[])
        broker.respond(MetadataRequest, r1)
        broker.respond(MetadataRequest, r2)
        assert len(broker._response_queue) == 2
        assert broker._response_queue[0][1] is r1
        assert broker._response_queue[1][1] is r2

    def test_respond_fn_returning_none_sends_no_response(self):
        """A respond_fn that resolves to None means 'request handled, send
        nothing back' -- the real broker behavior for acks=0 ProduceRequests.
        handle_request still consumes the queue entry and returns None so
        MockTransport delivers no bytes."""
        broker = MockBroker()
        seen = []

        def fn(api_key, api_version, correlation_id, request_bytes):
            seen.append(correlation_id)
            return None

        broker.respond_fn(MetadataRequest, fn)
        result = _run_coroutine(broker.handle_request(
            MetadataRequest.API_KEY, 0, correlation_id=99, request_bytes=b''))

        assert result is None
        assert seen == [99]
        # The queue entry was consumed even though no response was produced.
        assert len(broker._response_queue) == 0
        assert broker.requests_received == 1


# ---------------------------------------------------------------------------
# Integration tests: MockBroker + KafkaNetClient
# ---------------------------------------------------------------------------


class TestMockBrokerWithClient:
    """Test MockBroker wired into a real KafkaNetClient via attach()."""

    def _make_client(self, broker):
        """Create a KafkaNetClient and attach the mock broker."""
        client = KafkaNetClient(
            bootstrap_servers='%s:%d' % (broker.host, broker.port),
            api_version=broker.broker_version,
            request_timeout_ms=5000,
            metadata_max_age_ms=300000,
        )
        broker.attach(client._net)
        return client

    def test_bootstrap_through_mock(self):
        """Client bootstraps (ApiVersions + Metadata) through MockBroker."""
        broker = MockBroker()
        client = self._make_client(broker)
        try:
            version = client.check_version(timeout_ms=5000)
            assert version == broker.broker_version
            assert client.cluster.brokers()
            assert broker.requests_received > 0
        finally:
            client.close()

    def test_metadata_with_topics(self):
        """Client can fetch topic metadata from MockBroker."""
        broker = MockBroker()
        Topic = MetadataResponse.MetadataResponseTopic
        Partition = Topic.MetadataResponsePartition
        broker.set_metadata(topics=[
            Topic(version=8, error_code=0, name='test-topic', is_internal=False,
                  partitions=[
                      Partition(version=8, error_code=0, partition_index=0,
                                leader_id=0, leader_epoch=0,
                                replica_nodes=[0], isr_nodes=[0], offline_replicas=[]),
                      Partition(version=8, error_code=0, partition_index=1,
                                leader_id=0, leader_epoch=0,
                                replica_nodes=[0], isr_nodes=[0], offline_replicas=[]),
                  ]),
        ])
        client = self._make_client(broker)
        try:
            client.check_version(timeout_ms=5000)
            # Force a metadata update for the topic
            cluster = client.cluster
            cluster.add_topic('test-topic')
            future = cluster.request_update()
            _poll_for_future(client, future)
            assert 'test-topic' in cluster.topics()
            partitions = cluster.partitions_for_topic('test-topic')
            assert partitions == {0, 1}
        finally:
            client.close()

    def test_scripted_response(self):
        """Scripted responses are matched by API key and consumed in order."""
        broker = MockBroker()
        client = self._make_client(broker)
        try:
            client.check_version(timeout_ms=5000)

            # Script a custom metadata response with a specific topic
            Topic = MetadataResponse.MetadataResponseTopic
            Partition = Topic.MetadataResponsePartition
            Broker = MetadataResponse.MetadataResponseBroker
            custom_response = MetadataResponse(
                version=8,
                throttle_time_ms=0,
                brokers=[Broker(node_id=0, host='localhost', port=9092, rack=None)],
                cluster_id='custom-cluster',
                controller_id=0,
                topics=[
                    Topic(version=8, error_code=0, name='scripted-topic', is_internal=False,
                          partitions=[
                              Partition(version=8, error_code=0, partition_index=0,
                                        leader_id=0, leader_epoch=0,
                                        replica_nodes=[0], isr_nodes=[0], offline_replicas=[]),
                          ]),
                ],
            )
            broker.respond(MetadataRequest, custom_response)

            # Next metadata fetch should use the scripted response
            cluster = client.cluster
            cluster.add_topic('scripted-topic')
            future = cluster.request_update()
            _poll_for_future(client, future)
            assert cluster.cluster_id == 'custom-cluster'
            assert 'scripted-topic' in cluster.topics()

            # Subsequent metadata fetches fall back to the auto-response
            future = cluster.request_update()
            _poll_for_future(client, future)
            assert cluster.cluster_id == 'mock-cluster'
        finally:
            client.close()

    def test_multiple_metadata_updates(self):
        """Multiple metadata requests each get the correct response."""
        broker = MockBroker()
        client = self._make_client(broker)
        try:
            client.check_version(timeout_ms=5000)

            # First update: no topics
            cluster = client.cluster
            future = cluster.request_update()
            _poll_for_future(client, future)
            assert len(cluster.topics()) == 0

            # Update broker to have a topic
            Topic = MetadataResponse.MetadataResponseTopic
            Partition = Topic.MetadataResponsePartition
            broker.set_metadata(topics=[
                Topic(version=8, error_code=0, name='new-topic', is_internal=False,
                      partitions=[
                          Partition(version=8, error_code=0, partition_index=0,
                                    leader_id=0, leader_epoch=0,
                                    replica_nodes=[0], isr_nodes=[0], offline_replicas=[]),
                      ]),
            ])

            # Second update: should see the new topic
            cluster.add_topic('new-topic')
            future = cluster.request_update()
            _poll_for_future(client, future)
            assert 'new-topic' in cluster.topics()
        finally:
            client.close()

    def test_send_and_receive(self):
        """A request/response round-trip via send + poll works."""
        broker = MockBroker()
        client = self._make_client(broker)
        try:
            client.check_version(timeout_ms=5000)
            node_id = client.least_loaded_node(bootstrap_fallback=True)
            client.await_ready(node_id, timeout_ms=5000)

            # Send a MetadataRequest directly via client.send
            request = MetadataRequest(max_version=9)
            future = client.send(node_id, request)
            _poll_for_future(client, future)

            assert future.succeeded()
            response = future.value
            assert response.cluster_id == 'mock-cluster'
        finally:
            client.close()

    def test_fail_next_aborts_request(self):
        """fail_next aborts the connection and fails the in-flight request."""
        import kafka.errors as Errors

        broker = MockBroker()
        client = self._make_client(broker)
        try:
            client.check_version(timeout_ms=5000)
            node_id = client.least_loaded_node(bootstrap_fallback=True)
            client.await_ready(node_id, timeout_ms=5000)

            error = Errors.KafkaConnectionError('simulated')
            broker.fail_next(MetadataRequest, error=error)

            future = client.send(node_id, MetadataRequest(max_version=9))
            _poll_for_future(client, future)

            assert future.failed()
            assert future.exception is error
            # Connection was aborted, so it should no longer be ready.
            assert not client.is_ready(node_id)
        finally:
            client.close()

    def test_api_version_negotiation(self):
        """Client negotiates ApiVersions when api_version is not pre-set."""
        broker = MockBroker()
        client = KafkaNetClient(
            bootstrap_servers='%s:%d' % (broker.host, broker.port),
            # Deliberately omit api_version so the client must negotiate
            # via ApiVersionsRequest round-trips with the broker.
            request_timeout_ms=5000,
            metadata_max_age_ms=300000,
        )
        broker.attach(client._net)
        try:
            version = client.check_version(timeout_ms=5000)
            assert version == broker.broker_version
            assert client.cluster.brokers()
            # The broker should have received at least one ApiVersionsRequest.
            # If the client started above the broker's max supported version,
            # it would have gotten UnsupportedVersionError and retried at a
            # lower version - both paths exercise the mock's version check.
            assert broker.requests_received >= 2  # ApiVersions + Metadata at minimum
        finally:
            client.close()

    def test_respond_always(self):
        """respond_always serves every matching request; scripted queue wins."""
        broker = MockBroker()
        Broker = MetadataResponse.MetadataResponseBroker
        always = MetadataResponse(
            version=8, throttle_time_ms=0,
            brokers=[Broker(node_id=0, host='localhost', port=9092, rack=None)],
            cluster_id='always-cluster', controller_id=0, topics=[])
        scripted = MetadataResponse(
            version=8, throttle_time_ms=0,
            brokers=[Broker(node_id=0, host='localhost', port=9092, rack=None)],
            cluster_id='scripted-cluster', controller_id=0, topics=[])
        broker.respond_always(MetadataRequest, always)
        client = self._make_client(broker)
        try:
            client.check_version(timeout_ms=5000)
            cluster = client.cluster
            broker.respond(MetadataRequest, scripted)
            future = cluster.request_update()
            _poll_for_future(client, future)
            assert cluster.cluster_id == 'scripted-cluster'
            # Persistent response is served on every subsequent request
            for _ in range(2):
                future = cluster.request_update()
                _poll_for_future(client, future)
                assert cluster.cluster_id == 'always-cluster'
        finally:
            client.close()

    def test_admin_client_with_mock(self):
        """KafkaAdminClient works through MockBroker for basic operations."""
        broker = MockBroker()
        Topic = MetadataResponse.MetadataResponseTopic
        Partition = Topic.MetadataResponsePartition
        broker.set_metadata(topics=[
            Topic(version=8, error_code=0, name='admin-topic', is_internal=False,
                  partitions=[
                      Partition(version=8, error_code=0, partition_index=0,
                                leader_id=0, leader_epoch=0,
                                replica_nodes=[0], isr_nodes=[0], offline_replicas=[]),
                  ]),
        ])

        admin = KafkaAdminClient(
            kafka_client=broker.client_factory(),
            bootstrap_servers='%s:%d' % (broker.host, broker.port),
            api_version=broker.broker_version,
            request_timeout_ms=5000,
        )
        try:
            # list_topics / describe_topics use MetadataRequest
            topics = admin.list_topics()
            assert 'admin-topic' in topics

            described = admin.describe_topics(['admin-topic'])
            assert len(described) == 1
            assert described[0]['name'] == 'admin-topic'
        finally:
            admin.close()


# ---------------------------------------------------------------------------
# MockCluster: multi-broker routing and lifecycle
# ---------------------------------------------------------------------------


class TestMockCluster:

    def _make_client(self, cluster):
        client = KafkaNetClient(
            bootstrap_servers=cluster.bootstrap_servers(),
            api_version=cluster.broker_version,
            request_timeout_ms=5000,
            metadata_max_age_ms=300000,
        )
        cluster.attach(client._manager)
        return client

    def test_default_construction(self):
        cluster = MockCluster(num_brokers=3)
        assert len(cluster) == 3
        assert [b.node_id for b in cluster] == [0, 1, 2]
        assert cluster[1].port == 9093
        assert cluster.bootstrap_servers() == 'localhost:9092,localhost:9093,localhost:9094'
        # All brokers advertise the full broker list
        for b in cluster:
            assert len(b._metadata_response.brokers) == 3

    def test_bootstrap_and_per_node_routing(self):
        """Requests to node N land on broker N, not on a shared broker."""
        cluster = MockCluster(num_brokers=2)
        client = self._make_client(cluster)
        try:
            client.check_version(timeout_ms=5000)
            assert {b.node_id for b in client.cluster.brokers()} == {0, 1}

            for node_id in (0, 1):
                before = cluster[node_id].requests_received
                client.await_ready(node_id, timeout_ms=5000)
                future = client.send(node_id, MetadataRequest(max_version=9))
                _poll_for_future(client, future)
                assert future.succeeded()
                assert cluster[node_id].requests_received > before
        finally:
            client.close()

    def test_set_coordinator(self):
        """set_coordinator answers FindCoordinator per (key_type, key), with
        COORDINATOR_NOT_AVAILABLE for unmapped keys, and re-points live."""
        import kafka.errors as Errors
        from kafka.protocol.metadata import CoordinatorType, FindCoordinatorRequest

        cluster = MockCluster(num_brokers=2)
        cluster.set_coordinator('my-group', 1)
        cluster.set_coordinator('my-txn', 0, key_type=CoordinatorType.TRANSACTION)
        client = self._make_client(cluster)
        try:
            client.check_version(timeout_ms=5000)
            node_id = client.least_loaded_node(bootstrap_fallback=True)
            client.await_ready(node_id, timeout_ms=5000)

            def find(key, key_type):
                future = client.send(node_id, FindCoordinatorRequest(
                    key=key, key_type=key_type, coordinator_keys=[key]))
                _poll_for_future(client, future)
                assert future.succeeded()
                response = future.value
                return response.coordinators[0] if response.coordinators else response

            result = find('my-group', CoordinatorType.GROUP)
            assert result.error_code == 0
            assert (result.node_id, result.port) == (1, 9093)

            # The same key maps independently per key_type.
            result = find('my-txn', CoordinatorType.TRANSACTION)
            assert (result.node_id, result.port) == (0, 9092)
            result = find('my-txn', CoordinatorType.GROUP)
            assert result.error_code == Errors.CoordinatorNotAvailableError.errno

            # Re-point after a simulated election.
            cluster.set_coordinator('my-group', 0)
            result = find('my-group', CoordinatorType.GROUP)
            assert (result.node_id, result.port) == (0, 9092)

            # Clearing the mapping models an election gap.
            cluster.set_coordinator('my-group', None)
            result = find('my-group', CoordinatorType.GROUP)
            assert result.error_code == Errors.CoordinatorNotAvailableError.errno
        finally:
            client.close()

    def test_stop_and_start(self):
        """stop() aborts live connections and refuses new ones; start() recovers."""
        import kafka.errors as Errors

        cluster = MockCluster(num_brokers=2)
        client = self._make_client(cluster)
        try:
            client.check_version(timeout_ms=5000)
            client.await_ready(0, timeout_ms=5000)
            client.await_ready(1, timeout_ms=5000)

            cluster[0].stop()
            # In-flight request on the existing connection fails when the
            # scheduled abort lands.
            future = client.send(0, MetadataRequest(max_version=9))
            _poll_for_future(client, future)
            assert future.failed()
            assert not client.is_ready(0)

            # New connection attempts are refused while stopped.
            with pytest.raises(Errors.KafkaConnectionError):
                client.await_ready(0, timeout_ms=200)

            # Broker 1 is unaffected.
            future = client.send(1, MetadataRequest(max_version=9))
            _poll_for_future(client, future)
            assert future.succeeded()

            # After start(), the client reconnects once backoff expires.
            cluster[0].start()
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and not client.is_ready(0):
                try:
                    client.await_ready(0, timeout_ms=500)
                except Errors.KafkaConnectionError:
                    client.poll(timeout_ms=50)
            assert client.is_ready(0)
        finally:
            client.close()


class TestMockGroup:
    """The MockCluster.add_group auto-coordinator (JoinGroup/SyncGroup/
    Heartbeat/LeaveGroup), tested at the protocol-handler level.

    The handlers return response objects directly (the broker encodes them
    on the way out), so the tests can feed an encoded request in and assert
    on the response object's fields.
    """

    def _join(self, group, member_id='', topics=('t',), api_version=7):
        from kafka.coordinator.assignors.abstract import ConsumerProtocolSubscription
        from kafka.protocol.consumer import JoinGroupRequest
        request = JoinGroupRequest(
            group_id=group.group_id, session_timeout_ms=30000,
            rebalance_timeout_ms=300000, member_id=member_id,
            group_instance_id=None, protocol_type='consumer',
            protocols=[('range', ConsumerProtocolSubscription(0, list(topics), b'').encode())],
            max_version=api_version)
        request.API_VERSION = api_version
        request.with_header(correlation_id=1)
        return group._on_join(JoinGroupRequest.API_KEY, api_version, 1,
                              request.encode(header=True))

    def test_add_group_elects_first_member_leader(self):
        cluster = MockCluster(num_brokers=1)
        group = cluster.add_group('g', coordinator=0)
        # First join with an empty member_id: broker assigns one, elects it leader.
        resp = self._join(group, member_id='')
        assert resp.error_code == 0
        assert resp.leader == resp.member_id        # this member is leader
        assert resp.member_id                       # a member_id was assigned
        assert resp.generation_id == 1
        assert len(resp.members) == 1               # leader gets the member list
        assert group.leader == resp.member_id
        assert list(group.members) == [resp.member_id]

        # A rejoin reusing the member_id keeps it leader and bumps generation.
        resp2 = self._join(group, member_id=resp.member_id)
        assert resp2.member_id == resp.member_id
        assert resp2.leader == resp.member_id
        assert resp2.generation_id == 2
        assert list(group.members) == [resp.member_id]   # no duplicate member

    def test_trigger_rebalance_is_one_shot(self):
        import kafka.errors as Errors
        from kafka.protocol.consumer import HeartbeatRequest
        cluster = MockCluster(num_brokers=1)
        group = cluster.add_group('g', coordinator=0)

        def heartbeat():
            req = HeartbeatRequest(group_id='g', generation_id=1, member_id='m',
                                   group_instance_id=None)
            req.API_VERSION = 3
            req.with_header(correlation_id=1)
            return group._on_heartbeat(HeartbeatRequest.API_KEY, 3, 1, req.encode(header=True))

        # Default: healthy heartbeats.
        assert heartbeat().error_code == 0
        # Armed: next heartbeat reports a rebalance, then heals.
        group.trigger_rebalance()
        assert heartbeat().error_code == Errors.RebalanceInProgressError.errno
        assert heartbeat().error_code == 0
