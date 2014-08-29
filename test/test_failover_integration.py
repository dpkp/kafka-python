import logging
import os
import time
from . import unittest

from kafka import *  # noqa
from kafka.common import *  # noqa
from kafka.producer import Producer
from .fixtures import ZookeeperFixture, KafkaFixture
from .testutil import *


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

        # Test the base class Producer -- send_messages to a specific partition
        producer = Producer(self.client, async=False)

        # Send 10 random messages
        self._send_random_messages(producer, topic, partition, 10)

        # kill leader for partition
        broker = self._kill_leader(topic, partition)

        # expect failure, but dont wait more than 60 secs to recover
        recovered = False
        started = time.time()
        timeout = 60
        while not recovered and (time.time() - started) < timeout:
            try:
                logging.debug("attempting to send 'success' message after leader killed")
                producer.send_messages(topic, partition, b'success')
                logging.debug("success!")
                recovered = True
            except (FailedPayloadsError, ConnectionError):
                logging.debug("caught exception sending message -- will retry")
                continue

        # Verify we successfully sent the message
        self.assertTrue(recovered)

        # send some more messages to new leader
        self._send_random_messages(producer, topic, partition, 10)

        # count number of messages
        count = self._count_messages('test_switch_leader group', topic,
                                     partitions=(partition,))

        # Should be equal to 10 before + 1 recovery + 10 after
        self.assertEquals(count, 21)


    #@kafka_versions("all")
    @unittest.skip("async producer does not support reliable failover yet")
    def test_switch_leader_async(self):
        key, topic, partition = random_string(5), self.topic, 0

        # Test the base class Producer -- send_messages to a specific partition
        producer = Producer(self.client, async=True)

        # Send 10 random messages
        self._send_random_messages(producer, topic, partition, 10)

        # kill leader for partition
        broker = self._kill_leader(topic, partition)

        logging.debug("attempting to send 'success' message after leader killed")

        # in async mode, this should return immediately
        producer.send_messages(topic, partition, 'success')

        # send to new leader
        self._send_random_messages(producer, topic, partition, 10)

        # wait until producer queue is empty
        while not producer.queue.empty():
            time.sleep(0.1)
        producer.stop()

        # count number of messages
        count = self._count_messages('test_switch_leader_async group', topic,
                                     partitions=(partition,))

        # Should be equal to 10 before + 1 recovery + 10 after
        self.assertEquals(count, 21)


    def _send_random_messages(self, producer, topic, partition, n):
        for j in range(n):
            logging.debug('_send_random_message to %s:%d -- try %d', topic, partition, j)
            resp = producer.send_messages(topic, partition, random_string(10))
            if len(resp) > 0:
                self.assertEquals(resp[0].error, 0)
            logging.debug('_send_random_message to %s:%d -- try %d success', topic, partition, j)

    def _kill_leader(self, topic, partition):
        leader = self.client.topics_to_brokers[TopicAndPartition(topic, partition)]
        broker = self.brokers[leader.nodeId]
        broker.close()
        return broker

    def _count_messages(self, group, topic, timeout=1, partitions=None):
        hosts = ','.join(['%s:%d' % (broker.host, broker.port)
                          for broker in self.brokers])

        client = KafkaClient(hosts)
        consumer = SimpleConsumer(client, group, topic,
                                  partitions=partitions,
                                  auto_commit=False,
                                  iter_timeout=timeout)

        count = consumer.pending(partitions)
        consumer.stop()
        client.close()
        return count
