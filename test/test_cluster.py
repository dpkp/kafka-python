# pylint: skip-file
from __future__ import absolute_import

import socket

from kafka.cluster import ClusterMetadata, collect_hosts
from kafka.protocol.metadata import MetadataResponse


def test_empty_broker_list():
    cluster = ClusterMetadata()
    assert len(cluster.brokers()) == 0

    cluster.update_metadata(MetadataResponse[0](
        [(0, 'foo', 12), (1, 'bar', 34)], []))
    assert len(cluster.brokers()) == 2

    # empty broker list response should be ignored
    cluster.update_metadata(MetadataResponse[0](
        [],  # empty brokers
        [(17, 'foo', []), (17, 'bar', [])]))  # topics w/ error
    assert len(cluster.brokers()) == 2


def test_metadata_v0():
    cluster = ClusterMetadata()
    cluster.update_metadata(MetadataResponse[0](
        [(0, 'foo', 12), (1, 'bar', 34)],
        [(0, 'topic-1', [(0, 0, 0, [0], [0])])]))
    assert len(cluster.topics()) == 1
    assert cluster.controller is None
    assert cluster.cluster_id is None
    assert cluster._partitions['topic-1'][0].offline_replicas == []
    assert cluster._partitions['topic-1'][0].leader_epoch == -1


def test_metadata_v1():
    cluster = ClusterMetadata()
    cluster.update_metadata(MetadataResponse[1](
        [(0, 'foo', 12, 'rack-1'), (1, 'bar', 34, 'rack-2')],
        0, # controller_id
        [(0, 'topic-1', False, [(0, 0, 0, [0], [0])])]))
    assert len(cluster.topics()) == 1
    assert cluster.controller == cluster.broker_metadata(0)
    assert cluster.cluster_id is None
    assert cluster._partitions['topic-1'][0].offline_replicas == []
    assert cluster._partitions['topic-1'][0].leader_epoch == -1


def test_metadata_v2():
    cluster = ClusterMetadata()
    cluster.update_metadata(MetadataResponse[2](
        [(0, 'foo', 12, 'rack-1'), (1, 'bar', 34, 'rack-2')],
        'cluster-foo', # cluster_id
        0, # controller_id
        [(0, 'topic-1', False, [(0, 0, 0, [0], [0])])]))
    assert len(cluster.topics()) == 1
    assert cluster.controller == cluster.broker_metadata(0)
    assert cluster.cluster_id == 'cluster-foo'
    assert cluster._partitions['topic-1'][0].offline_replicas == []
    assert cluster._partitions['topic-1'][0].leader_epoch == -1


def test_metadata_v3():
    cluster = ClusterMetadata()
    cluster.update_metadata(MetadataResponse[3](
        0, # throttle_time_ms
        [(0, 'foo', 12, 'rack-1'), (1, 'bar', 34, 'rack-2')],
        'cluster-foo', # cluster_id
        0, # controller_id
        [(0, 'topic-1', False, [(0, 0, 0, [0], [0])])]))
    assert len(cluster.topics()) == 1
    assert cluster.controller == cluster.broker_metadata(0)
    assert cluster.cluster_id == 'cluster-foo'
    assert cluster._partitions['topic-1'][0].offline_replicas == []
    assert cluster._partitions['topic-1'][0].leader_epoch == -1


def test_metadata_v4():
    cluster = ClusterMetadata()
    cluster.update_metadata(MetadataResponse[4](
        0, # throttle_time_ms
        [(0, 'foo', 12, 'rack-1'), (1, 'bar', 34, 'rack-2')],
        'cluster-foo', # cluster_id
        0, # controller_id
        [(0, 'topic-1', False, [(0, 0, 0, [0], [0])])]))
    assert len(cluster.topics()) == 1
    assert cluster.controller == cluster.broker_metadata(0)
    assert cluster.cluster_id == 'cluster-foo'
    assert cluster._partitions['topic-1'][0].offline_replicas == []
    assert cluster._partitions['topic-1'][0].leader_epoch == -1


def test_metadata_v5():
    cluster = ClusterMetadata()
    cluster.update_metadata(MetadataResponse[5](
        0, # throttle_time_ms
        [(0, 'foo', 12, 'rack-1'), (1, 'bar', 34, 'rack-2')],
        'cluster-foo', # cluster_id
        0, # controller_id
        [(0, 'topic-1', False, [(0, 0, 0, [0], [0], [12])])]))
    assert len(cluster.topics()) == 1
    assert cluster.controller == cluster.broker_metadata(0)
    assert cluster.cluster_id == 'cluster-foo'
    assert cluster._partitions['topic-1'][0].offline_replicas == [12]
    assert cluster._partitions['topic-1'][0].leader_epoch == -1


def test_metadata_v6():
    cluster = ClusterMetadata()
    cluster.update_metadata(MetadataResponse[6](
        0, # throttle_time_ms
        [(0, 'foo', 12, 'rack-1'), (1, 'bar', 34, 'rack-2')],
        'cluster-foo', # cluster_id
        0, # controller_id
        [(0, 'topic-1', False, [(0, 0, 0, [0], [0], [12])])]))
    assert len(cluster.topics()) == 1
    assert cluster.controller == cluster.broker_metadata(0)
    assert cluster.cluster_id == 'cluster-foo'
    assert cluster._partitions['topic-1'][0].offline_replicas == [12]
    assert cluster._partitions['topic-1'][0].leader_epoch == -1


def test_metadata_v7():
    cluster = ClusterMetadata()
    cluster.update_metadata(MetadataResponse[7](
        0, # throttle_time_ms
        [(0, 'foo', 12, 'rack-1'), (1, 'bar', 34, 'rack-2')],
        'cluster-foo', # cluster_id
        0, # controller_id
        [(0, 'topic-1', False, [(0, 0, 0, 0, [0], [0], [12])])]))
    assert len(cluster.topics()) == 1
    assert cluster.controller == cluster.broker_metadata(0)
    assert cluster.cluster_id == 'cluster-foo'
    assert cluster._partitions['topic-1'][0].offline_replicas == [12]
    assert cluster._partitions['topic-1'][0].leader_epoch == 0


def test_unauthorized_topic():
    cluster = ClusterMetadata()
    assert len(cluster.brokers()) == 0

    cluster.update_metadata(MetadataResponse[0](
        [(0, 'foo', 12), (1, 'bar', 34)],
        [(29, 'unauthorized-topic', [])]))  # single topic w/ unauthorized error

    # broker metadata should get updated
    assert len(cluster.brokers()) == 2

    # topic should be added to unauthorized list
    assert 'unauthorized-topic' in cluster.unauthorized_topics


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
