import logging
import random

from .conn import BrokerConnection, collect_hosts
from .protocol.metadata import MetadataRequest

logger = logging.getLogger(__name__)


class Cluster(object):
    def __init__(self, **kwargs):
        if 'bootstrap_servers' not in kwargs:
            kargs['bootstrap_servers'] = 'localhost'

        self._brokers = {}
        self._topics = {}
        self._groups = {}

        self._bootstrap(collect_hosts(kwargs['bootstrap_servers']),
                        timeout=kwargs.get('bootstrap_timeout', 2))

    def brokers(self):
        brokers = list(self._brokers.values())
        return random.sample(brokers, len(brokers))

    def random_broker(self):
        for broker in self.brokers():
            if broker.connected() or broker.connect():
                return broker
        return None

    def broker_by_id(self, broker_id):
        return self._brokers.get(broker_id)

    def topics(self):
        return list(self._topics.keys())

    def partitions_for_topic(self, topic):
        if topic not in self._topics:
            return None
        return list(self._topics[topic].keys())

    def broker_for_partition(self, topic, partition):
        if topic not in self._topics or partition not in self._topics[topic]:
            return None
        broker_id = self._topics[topic][partition]
        return self.broker_by_id(broker_id)

    def refresh_metadata(self):
        broker = self.random_broker()
        if not broker.send(MetadataRequest([])):
            return None
        metadata = broker.recv()
        if not metadata:
            return None
        self._update_metadata(metadata)
        return metadata

    def _update_metadata(self, metadata):
        self._brokers.update({
            node_id: BrokerConnection(host, port)
            for node_id, host, port in metadata.brokers
            if node_id not in self._brokers
        })

        self._topics = {
            topic: {
                partition: leader
                for _, partition, leader, _, _ in partitions
            }
            for _, topic, partitions in metadata.topics
        }

    def _bootstrap(self, hosts, timeout=2):
        for host, port in hosts:
            conn = BrokerConnection(host, port, timeout)
            if not conn.connect():
                continue
            self._brokers['bootstrap'] = conn
            if self.refresh_metadata():
                break
        else:
            raise ValueError("Could not bootstrap kafka cluster from %s" % hosts)

        if len(self._brokers) > 1:
            self._brokers.pop('bootstrap')
            conn.close()

    def __str__(self):
        return 'Cluster(brokers: %d, topics: %d, groups: %d)' % \
               (len(self._brokers), len(self._topics), len(self._groups))
