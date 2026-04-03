# pylint: skip-file

import socket

from kafka.cluster import ClusterMetadata, collect_hosts
from kafka.protocol.metadata import MetadataResponse

Broker = MetadataResponse.MetadataResponseBroker
Topic = MetadataResponse.MetadataResponseTopic
Partition = Topic.MetadataResponsePartition


def test_empty_broker_list():
    cluster = ClusterMetadata()
    assert len(cluster.brokers()) == 0

    cluster.update_metadata(MetadataResponse[0](
        [Broker(0, 'foo', 12, version=0), Broker(1, 'bar', 34, version=0)], []))
    assert len(cluster.brokers()) == 2

    # empty broker list response should be ignored
    cluster.update_metadata(MetadataResponse[0](
        brokers=[],  # empty brokers
        topics=[Topic(17, 'foo', [], version=0), Topic(17, 'bar', [], version=0)]))  # topics w/ error
    assert len(cluster.brokers()) == 2


def test_metadata_v0():
    cluster = ClusterMetadata()
    cluster.update_metadata(MetadataResponse[0](
        brokers=[Broker(0, 'foo', 12, version=0), Broker(1, 'bar', 34, version=0)],
        topics=[Topic(0, 'topic-1', [Partition(0, 0, 0, [0], [0], version=0)], version=0)]))
    assert len(cluster.topics()) == 1
    assert cluster.controller is None
    assert cluster.cluster_id is None
    assert cluster._partitions['topic-1'][0].offline_replicas == []
    assert cluster._partitions['topic-1'][0].leader_epoch == -1


def test_metadata_v1():
    cluster = ClusterMetadata()
    cluster.update_metadata(MetadataResponse[1](
        brokers=[Broker(0, 'foo', 12, 'rack-1', version=1), Broker(1, 'bar', 34, 'rack-2', version=1)],
        controller_id=0,
        topics=[Topic(0, 'topic-1', False, [Partition(0, 0, 0, [0], [0], version=1)], version=1)]))
    assert len(cluster.topics()) == 1
    assert cluster.controller == cluster.broker_metadata(0)
    assert cluster.cluster_id is None
    assert cluster._partitions['topic-1'][0].offline_replicas == []
    assert cluster._partitions['topic-1'][0].leader_epoch == -1


def test_metadata_v2():
    cluster = ClusterMetadata()
    cluster.update_metadata(MetadataResponse[2](
        brokers=[Broker(0, 'foo', 12, 'rack-1', version=2), Broker(1, 'bar', 34, 'rack-2', version=2)],
        cluster_id='cluster-foo',
        controller_id=0,
        topics=[Topic(0, 'topic-1', False, [Partition(0, 0, 0, [0], [0], version=2)], version=2)]))
    assert len(cluster.topics()) == 1
    assert cluster.controller == cluster.broker_metadata(0)
    assert cluster.cluster_id == 'cluster-foo'
    assert cluster._partitions['topic-1'][0].offline_replicas == []
    assert cluster._partitions['topic-1'][0].leader_epoch == -1


def test_metadata_v3():
    cluster = ClusterMetadata()
    cluster.update_metadata(MetadataResponse[3](
        throttle_time_ms=0,
        brokers=[Broker(0, 'foo', 12, 'rack-1', version=3), Broker(1, 'bar', 34, 'rack-2', version=3)],
        cluster_id='cluster-foo',
        controller_id=0,
        topics=[Topic(0, 'topic-1', False, [Partition(0, 0, 0, [0], [0], version=3)], version=3)]))
    assert len(cluster.topics()) == 1
    assert cluster.controller == cluster.broker_metadata(0)
    assert cluster.cluster_id == 'cluster-foo'
    assert cluster._partitions['topic-1'][0].offline_replicas == []
    assert cluster._partitions['topic-1'][0].leader_epoch == -1


def test_metadata_v4():
    cluster = ClusterMetadata()
    cluster.update_metadata(MetadataResponse[4](
        throttle_time_ms=0,
        brokers=[Broker(0, 'foo', 12, 'rack-1', version=4), Broker(1, 'bar', 34, 'rack-2', version=4)],
        cluster_id='cluster-foo',
        controller_id=0,
        topics=[Topic(0, 'topic-1', False, [Partition(0, 0, 0, [0], [0], version=4)], version=4)]))
    assert len(cluster.topics()) == 1
    assert cluster.controller == cluster.broker_metadata(0)
    assert cluster.cluster_id == 'cluster-foo'
    assert cluster._partitions['topic-1'][0].offline_replicas == []
    assert cluster._partitions['topic-1'][0].leader_epoch == -1


def test_metadata_v5():
    cluster = ClusterMetadata()
    cluster.update_metadata(MetadataResponse[5](
        throttle_time_ms=0,
        brokers=[Broker(0, 'foo', 12, 'rack-1', version=5), Broker(1, 'bar', 34, 'rack-2', version=5)],
        cluster_id='cluster-foo',
        controller_id=0,
        topics=[Topic(0, 'topic-1', False, [Partition(0, 0, 0, [0], [0], [12], version=5)], version=5)]))
    assert len(cluster.topics()) == 1
    assert cluster.controller == cluster.broker_metadata(0)
    assert cluster.cluster_id == 'cluster-foo'
    assert cluster._partitions['topic-1'][0].offline_replicas == [12]
    assert cluster._partitions['topic-1'][0].leader_epoch == -1


def test_metadata_v6():
    cluster = ClusterMetadata()
    cluster.update_metadata(MetadataResponse[6](
        throttle_time_ms=0,
        brokers=[Broker(0, 'foo', 12, 'rack-1', version=6), Broker(1, 'bar', 34, 'rack-2', version=6)],
        cluster_id='cluster-foo',
        controller_id=0,
        topics=[Topic(0, 'topic-1', False, [Partition(0, 0, 0, [0], [0], [12], version=6)], version=6)]))
    assert len(cluster.topics()) == 1
    assert cluster.controller == cluster.broker_metadata(0)
    assert cluster.cluster_id == 'cluster-foo'
    assert cluster._partitions['topic-1'][0].offline_replicas == [12]
    assert cluster._partitions['topic-1'][0].leader_epoch == -1


def test_metadata_v7():
    cluster = ClusterMetadata()
    cluster.update_metadata(MetadataResponse[7](
        throttle_time_ms=0,
        brokers=[Broker(0, 'foo', 12, 'rack-1', version=7), Broker(1, 'bar', 34, 'rack-2', version=7)],
        cluster_id='cluster-foo',
        controller_id=0,
        topics=[Topic(0, 'topic-1', False, [Partition(0, 0, 0, 0, [0], [0], [12], version=7)], version=7)]))
    assert len(cluster.topics()) == 1
    assert cluster.controller == cluster.broker_metadata(0)
    assert cluster.cluster_id == 'cluster-foo'
    assert cluster._partitions['topic-1'][0].offline_replicas == [12]
    assert cluster._partitions['topic-1'][0].leader_epoch == 0


def test_unauthorized_topic():
    cluster = ClusterMetadata()
    assert len(cluster.brokers()) == 0

    cluster.update_metadata(MetadataResponse[0](
        brokers=[Broker(0, 'foo', 12, version=0), Broker(1, 'bar', 34, version=0)],
        topics=[Topic(29, 'unauthorized-topic', [], version=0)]))  # single topic w/ unauthorized error

    # broker metadata should get updated
    assert len(cluster.brokers()) == 2

    # topic should be added to unauthorized list
    assert 'unauthorized-topic' in cluster.unauthorized_topics


def test_set_topics():
    cluster = ClusterMetadata()
    cluster._need_update = False

    fut = cluster.set_topics(['t1', 't2'])
    assert not fut.is_done
    assert cluster._need_update is True

    fut.success(cluster)
    cluster._need_update = False

    fut = cluster.set_topics(['t1', 't2'])
    assert fut.is_done
    assert fut.value == cluster
    assert cluster._need_update is False

    fut = cluster.set_topics([])
    assert fut.is_done
    assert fut.value == cluster
    assert cluster._need_update is False


def test_collect_hosts__happy_path():
    hosts = "127.0.0.1:1234,127.0.0.1"
    results = collect_hosts(hosts)
    assert set(results) == set([
        ('127.0.0.1', 1234, socket.AF_INET),
        ('127.0.0.1', 9092, socket.AF_INET),
    ])


def test_collect_hosts__ipv6():
    hosts = "[localhost]:1234,[2001:1000:2000::1],[2001:1000:2000::1]:1234"
    results = collect_hosts(hosts)
    assert set(results) == set([
        ('localhost', 1234, socket.AF_INET6),
        ('2001:1000:2000::1', 9092, socket.AF_INET6),
        ('2001:1000:2000::1', 1234, socket.AF_INET6),
    ])


def test_collect_hosts__string_list():
    hosts = [
        'localhost:1234',
        'localhost',
        '[localhost]',
        '2001::1',
        '[2001::1]',
        '[2001::1]:1234',
    ]
    results = collect_hosts(hosts)
    assert set(results) == set([
        ('localhost', 1234, socket.AF_UNSPEC),
        ('localhost', 9092, socket.AF_UNSPEC),
        ('localhost', 9092, socket.AF_INET6),
        ('2001::1', 9092, socket.AF_INET6),
        ('2001::1', 9092, socket.AF_INET6),
        ('2001::1', 1234, socket.AF_INET6),
    ])


def test_collect_hosts__with_spaces():
    hosts = "localhost:1234, localhost"
    results = collect_hosts(hosts)
    assert set(results) == set([
        ('localhost', 1234, socket.AF_UNSPEC),
        ('localhost', 9092, socket.AF_UNSPEC),
    ])


def test_collect_hosts__protocol():
    hosts = "SASL_SSL://foo.bar:1234,SASL_SSL://fizz.buzz:5678"
    results = collect_hosts(hosts)
    assert set(results) == set([
        ('foo.bar', 1234, socket.AF_UNSPEC),
        ('fizz.buzz', 5678, socket.AF_UNSPEC),
    ])
