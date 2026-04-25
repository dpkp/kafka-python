# pylint: skip-file

import socket
from unittest.mock import MagicMock, patch

import pytest

from kafka.cluster import ClusterMetadata, collect_hosts
from kafka.future import Future
from kafka.protocol.metadata import MetadataResponse

Broker = MetadataResponse.MetadataResponseBroker
Topic = MetadataResponse.MetadataResponseTopic
Partition = Topic.MetadataResponsePartition


@pytest.fixture
def cluster():
    return ClusterMetadata(MagicMock())


class TestClusterMetadataUpdateMetadata:
    def test_empty_broker_list(self, cluster):
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
    def test_metadata(self, cluster, version):
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

    def test_unauthorized_topic(self, cluster):
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
    def test_set_topics(self, cluster):
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


class TestClusterMetadataRefresh:
    def test_request_update_returns_future(self, cluster):
        f = cluster.request_update()
        assert isinstance(f, Future)
        assert not f.is_done

    def test_request_update_deduplicates(self, cluster):
        f1 = cluster.request_update()
        f2 = cluster.request_update()
        assert f1 is f2

    def test_request_update_new_future_after_done(self, cluster):
        f1 = cluster.request_update()
        f1.success(True)
        f2 = cluster.request_update()
        assert f2 is not f1

    def test_request_update_sets_cluster_need_update(self, cluster):
        f = cluster.request_update()
        assert cluster._need_update

    def test_request_update_sends_metadata_request(self, manager):
        manager.bootstrap()
        with patch.object(manager, 'send', return_value=MagicMock()):
            f = manager.cluster.request_update()
            # Drive the cluster refresh loop
            manager.poll(timeout_ms=100, future=f)
            assert manager.send.called

    def test_refresh_metadata_retries_no_node(self, manager):
        # No connected nodes, empty cluster
        cluster = manager.cluster
        with patch.object(cluster, 'brokers', return_value=[]):
            cluster.start_refresh_loop()
            f = cluster.request_update()
            manager.poll(timeout_ms=0)
            # Should not have resolved yet (retry scheduled)
            assert not f.is_done
            # Should have a scheduled retry
            assert len(manager._net._scheduled) > 0

    def test_bootstrap_triggers_refresh_loop(self, manager, mocker):
        """bootstrap() schedules the periodic metadata refresh loop on the
        cluster, so refresh fires without anyone calling it from compat.poll()."""
        cluster = manager.cluster
        assert cluster._refresh_loop_future is None
        spy = mocker.spy(cluster, 'refresh_metadata')
        manager.bootstrap(timeout_ms=100)
        assert cluster._refresh_loop_future is not None
        assert spy.call_count >= 1

    def test_refresh_loop_spawned_once(self, manager):
        """Calling bootstrap() multiple times must not spawn multiple refresh
        loop tasks."""
        cluster = manager.cluster
        manager.bootstrap(timeout_ms=100)
        future = cluster._refresh_loop_future
        assert future is not None
        manager.bootstrap(timeout_ms=100)
        assert cluster._refresh_loop_future is future
