# pylint: skip-file

import socket

import pytest

from kafka.cluster import ClusterMetadata, collect_hosts
from kafka.protocol.metadata import MetadataResponse

Broker = MetadataResponse.MetadataResponseBroker
Topic = MetadataResponse.MetadataResponseTopic
Partition = Topic.MetadataResponsePartition


class TestClusterMetadataUpdateMetadata:
    def test_empty_broker_list(self):
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

    @pytest.mark.parametrize('version', range(0, MetadataResponse.max_version + 1))
    def test_metadata(self, version):
        cluster = ClusterMetadata()
        topic = Topic(
            version=version,
            error_code=0,
            name='topic-1',
            is_internal=False,
            partitions=[
                Partition(
                    version=version,
                    error_code=0,
                    partition_index=0,
                    leader_id=0,
                    leader_epoch=0,
                    replica_nodes=[0],
                    isr_nodes=[0],
                    offline_replicas=[12],
                ),
            ],
        )
        response = MetadataResponse(
            version=version,
            throttle_time_ms=0,
            brokers=[
                Broker(node_id=0, host='foo', port=12, rack='rack-1', version=version),
                Broker(node_id=1, host='bar', port=34, rack='rack-2', version=version),
            ],
            cluster_id='cluster-foo',
            controller_id=0,
            topics=[topic])
        response = MetadataResponse.decode(response.encode(), version=version)
        cluster.update_metadata(response)
        assert len(cluster.topics()) == 1
        if version >= 1:
            assert cluster.controller == cluster.broker_metadata(0)
        else:
            assert cluster.controller is None
        if version >= 2:
            assert cluster.cluster_id == 'cluster-foo'
        else:
            assert cluster.cluster_id is None
        if version >= 5:
            assert cluster._partitions['topic-1'][0].offline_replicas == [12]
        else:
            assert cluster._partitions['topic-1'][0].offline_replicas == []
        if version >= 7:
            assert cluster._partitions['topic-1'][0].leader_epoch == 0
        else:
            assert cluster._partitions['topic-1'][0].leader_epoch == -1

    def test_unauthorized_topic(self):
        cluster = ClusterMetadata()
        cluster.set_topics(['unauthorized-topic'])
        assert len(cluster.brokers()) == 0

        cluster.update_metadata(MetadataResponse[0](
            brokers=[Broker(0, 'foo', 12, version=0), Broker(1, 'bar', 34, version=0)],
            topics=[Topic(29, 'unauthorized-topic', [], version=0)]))  # single topic w/ unauthorized error

        # broker metadata should get updated
        assert len(cluster.brokers()) == 2

        # topic should be added to unauthorized list
        assert 'unauthorized-topic' in cluster.unauthorized_topics


class TestClusterMetadataTopics:
    def test_set_topics(self):
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


class TestClusterMetadataCollectHosts:
    def test_collect_hosts__happy_path(self):
        hosts = "127.0.0.1:1234,127.0.0.1"
        results = collect_hosts(hosts)
        assert set(results) == set([
            ('127.0.0.1', 1234, socket.AF_INET),
            ('127.0.0.1', 9092, socket.AF_INET),
        ])

    def test_collect_hosts__ipv6(self):
        hosts = "[localhost]:1234,[2001:1000:2000::1],[2001:1000:2000::1]:1234"
        results = collect_hosts(hosts)
        assert set(results) == set([
            ('localhost', 1234, socket.AF_INET6),
            ('2001:1000:2000::1', 9092, socket.AF_INET6),
            ('2001:1000:2000::1', 1234, socket.AF_INET6),
        ])

    def test_collect_hosts__string_list(self):
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

    def test_collect_hosts__with_spaces(self):
        hosts = "localhost:1234, localhost"
        results = collect_hosts(hosts)
        assert set(results) == set([
            ('localhost', 1234, socket.AF_UNSPEC),
            ('localhost', 9092, socket.AF_UNSPEC),
        ])

    def test_collect_hosts__protocol(self):
        hosts = "SASL_SSL://foo.bar:1234,SASL_SSL://fizz.buzz:5678"
        results = collect_hosts(hosts)
        assert set(results) == set([
            ('foo.bar', 1234, socket.AF_UNSPEC),
            ('fizz.buzz', 5678, socket.AF_UNSPEC),
        ])
