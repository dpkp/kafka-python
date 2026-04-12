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

from test.mock_broker import MockBroker


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
        broker.attach(client._manager)
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
            version = client.api_version(MetadataRequest, max_version=8)
            request = MetadataRequest[version]()
            future = client.send(node_id, request)
            _poll_for_future(client, future)

            assert future.succeeded()
            response = future.value
            assert response.cluster_id == 'mock-cluster'
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
        broker.attach(client._manager)
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
