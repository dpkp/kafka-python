"""Curated both-backends end-to-end coverage.

A small, high-signal set of full-stack round-trips (bootstrap, send, metadata
refresh, consumer-group join) run against BOTH the selector and asyncio backends
via the parametrized ``both_net`` fixture. This is the cross-backend counterpart
to the per-backend suites in this package: those exercise each backend in
isolation; these prove the manager / coordinator async bridge (run / wait_for /
send / bootstrap) behaves identically on both.

Driven through MockCluster (stateful), not a scripted MockBroker: a started loop
runs background heartbeat / metadata-refresh coroutines that fire unscripted
requests, which MockCluster answers and a scripted MockBroker would not.
"""
from kafka.consumer.subscription_state import SubscriptionState
from kafka.coordinator.consumer import ConsumerCoordinator
from kafka.net.compat import KafkaNetClient
from kafka.net.manager import KafkaConnectionManager
from kafka.protocol.metadata import MetadataRequest, MetadataResponse
from kafka.structs import TopicPartition
from test.mock_broker import MockCluster


def _metadata_topic(name, num_partitions, node_id=0):
    """A MetadataResponseTopic for MockCluster.set_metadata(topics=[...])."""
    Topic = MetadataResponse.MetadataResponseTopic
    Partition = Topic.MetadataResponsePartition
    return Topic(version=8, error_code=0, name=name, is_internal=False,
                 partitions=[
                     Partition(version=8, error_code=0, partition_index=p,
                               leader_id=node_id, leader_epoch=0,
                               replica_nodes=[node_id], isr_nodes=[node_id],
                               offline_replicas=[])
                     for p in range(num_partitions)])


def _manager(both_net, mock_cluster):
    manager = KafkaConnectionManager(
        both_net, bootstrap_servers=mock_cluster.bootstrap_servers(),
        api_version=mock_cluster.broker_version, request_timeout_ms=5000)
    mock_cluster.attach(manager)
    return manager


def test_bootstrap_and_metadata_send(both_net):
    """Bootstrap + a MetadataRequest round-trip through the manager."""
    cluster = MockCluster(num_brokers=1)
    cluster.set_metadata(topics=[_metadata_topic('t', num_partitions=2)])
    manager = _manager(both_net, cluster)
    try:
        manager.bootstrap(timeout_ms=5000)
        assert manager.bootstrapped
        assert manager.cluster.brokers()

        async def do_send():
            return await manager.send(MetadataRequest[0]([]))
        resp = both_net.run(do_send)
        assert resp is not None
    finally:
        manager.close()


def test_metadata_refresh(both_net):
    """cluster.refresh_metadata() drives a real MetadataRequest and applies it."""
    cluster = MockCluster(num_brokers=1)
    cluster.set_metadata(topics=[_metadata_topic('t', num_partitions=3)])
    manager = _manager(both_net, cluster)
    try:
        manager.bootstrap(timeout_ms=5000)
        both_net.run(manager.cluster.refresh_metadata)
        assert 't' in manager.cluster.topics()
        assert manager.cluster.partitions_for_topic('t') == {0, 1, 2}
    finally:
        manager.close()


def test_consumer_group_join_assigns_partitions(both_net, metrics):
    """A consumer joins a MockCluster group, is elected leader, runs the
    assignor, and is assigned every partition -- exercising find-coordinator +
    join + sync + heartbeat on both backends."""
    cluster = MockCluster(num_brokers=1)
    cluster.set_metadata(topics=[_metadata_topic('t', num_partitions=3)])
    group = cluster.add_group('grp', coordinator=0)
    manager = _manager(both_net, cluster)
    client = KafkaNetClient(net=both_net, manager=manager)
    coordinator = ConsumerCoordinator(
        client, SubscriptionState(), metrics=metrics,
        api_version=cluster.broker_version,
        heartbeat_interval_ms=20, retry_backoff_ms=20, group_id='grp')
    manager.bootstrap(timeout_ms=5000)
    try:
        coordinator._subscription.subscribe(topics=['t'])
        client.cluster.request_update()

        assert coordinator.ensure_active_group(timeout_ms=5000)
        assert coordinator._subscription.assigned_partitions() == {
            TopicPartition('t', 0), TopicPartition('t', 1), TopicPartition('t', 2)}
        assert coordinator._is_leader
        assert group.generation == 1
    finally:
        coordinator._close_heartbeat()
        coordinator.reset_generation()
        coordinator.close(timeout_ms=0)
        client.close()
        manager.close()
