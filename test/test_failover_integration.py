import os
import time

from kafka import *  # noqa
from kafka.common import *  # noqa
from fixtures import ZookeeperFixture, KafkaFixture
from testutil import *

class TestFailover(KafkaIntegrationTestCase):
    create_client = False

    @classmethod
    def setUpClass(cls):  # noqa
        if not os.environ.get('KAFKA_VERSION'):
            return

        zk_chroot = random_string(10)
        replicas = 2
        partitions = 2

        # mini zookeeper, 2 kafka brokers
        cls.zk = ZookeeperFixture.instance()
        kk_args = [cls.zk.host, cls.zk.port, zk_chroot, replicas, partitions]
        cls.brokers = [KafkaFixture.instance(i, *kk_args) for i in range(replicas)]

        hosts = ['%s:%d' % (b.host, b.port) for b in cls.brokers]
        cls.client = KafkaClient(hosts)

    @classmethod
    def tearDownClass(cls):
        if not os.environ.get('KAFKA_VERSION'):
            return

        cls.client.close()
        for broker in cls.brokers:
            broker.close()
        cls.zk.close()

    @kafka_versions("all")
    def test_switch_leader(self):
        key, topic, partition = random_string(5), self.topic, 0
        producer = SimpleProducer(self.client)

        for i in range(1, 4):

            # XXX unfortunately, the conns dict needs to be warmed for this to work
            # XXX unfortunately, for warming to work, we need at least as many partitions as brokers
            self._send_random_messages(producer, self.topic, 10)

            # kil leader for partition 0
            broker = self._kill_leader(topic, partition)

            # expect failure, reload meta data
            with self.assertRaises(FailedPayloadsError):
                producer.send_messages(self.topic, 'part 1')
                producer.send_messages(self.topic, 'part 2')
            time.sleep(1)

            # send to new leader
            self._send_random_messages(producer, self.topic, 10)

            broker.open()
            time.sleep(3)

            # count number of messages
            count = self._count_messages('test_switch_leader group %s' % i, topic)
            self.assertIn(count, range(20 * i, 22 * i + 1))

        producer.stop()

    @kafka_versions("all")
    def test_switch_leader_async(self):
        key, topic, partition = random_string(5), self.topic, 0
        producer = SimpleProducer(self.client, async=True)

        for i in range(1, 4):

            self._send_random_messages(producer, self.topic, 10)

            # kil leader for partition 0
            broker = self._kill_leader(topic, partition)

            # expect failure, reload meta data
            producer.send_messages(self.topic, 'part 1')
            producer.send_messages(self.topic, 'part 2')
            time.sleep(1)

            # send to new leader
            self._send_random_messages(producer, self.topic, 10)

            broker.open()
            time.sleep(3)

            # count number of messages
            count = self._count_messages('test_switch_leader_async group %s' % i, topic)
            self.assertIn(count, range(20 * i, 22 * i + 1))

        producer.stop()

    def _send_random_messages(self, producer, topic, n):
        for j in range(n):
            resp = producer.send_messages(topic, random_string(10))
            if len(resp) > 0:
                self.assertEquals(resp[0].error, 0)
        time.sleep(1)  # give it some time

    def _kill_leader(self, topic, partition):
        leader = self.client.topics_to_brokers[TopicAndPartition(topic, partition)]
        broker = self.brokers[leader.nodeId]
        broker.close()
        time.sleep(1)  # give it some time
        return broker

    def _count_messages(self, group, topic):
        hosts = '%s:%d' % (self.brokers[0].host, self.brokers[0].port)
        client = KafkaClient(hosts)
        consumer = SimpleConsumer(client, group, topic, auto_commit=False, iter_timeout=0)
        all_messages = []
        for message in consumer:
            all_messages.append(message)
        consumer.stop()
        client.close()
        return len(all_messages)
