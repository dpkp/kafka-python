import logging
import os
import time

from . import unittest

from kafka import KafkaClient, SimpleConsumer
from kafka.common import TopicAndPartition, FailedPayloadsError, ConnectionError
from kafka.producer.base import Producer

from test.fixtures import ZookeeperFixture, KafkaFixture
from test.testutil import (
    KafkaIntegrationTestCase, kafka_versions, random_string
)


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
        topic = self.topic
        partition = 0

        # Test the base class Producer -- send_messages to a specific partition
        producer = Producer(self.client, async=False,
                            req_acks=Producer.ACK_AFTER_CLUSTER_COMMIT)

        # Send 10 random messages
        self._send_random_messages(producer, topic, partition, 100)

        # kill leader for partition
        self._kill_leader(topic, partition)

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
        self._send_random_messages(producer, topic, partition, 100)

        # count number of messages
        # Should be equal to 10 before + 1 recovery + 10 after
        self.assert_message_count(topic, 201, partitions=(partition,))


    #@kafka_versions("all")
    @unittest.skip("async producer does not support reliable failover yet")
    def test_switch_leader_async(self):
        topic = self.topic
        partition = 0

        # Test the base class Producer -- send_messages to a specific partition
        producer = Producer(self.client, async=True)

        # Send 10 random messages
        self._send_random_messages(producer, topic, partition, 10)

        # kill leader for partition
        self._kill_leader(topic, partition)

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
        # Should be equal to 10 before + 1 recovery + 10 after
        self.assert_message_count(topic, 21, partitions=(partition,))

    def _send_random_messages(self, producer, topic, partition, n):
        for j in range(n):
            logging.debug('_send_random_message to %s:%d -- try %d', topic, partition, j)
            resp = producer.send_messages(topic, partition, random_string(10))
            if len(resp) > 0:
                self.assertEqual(resp[0].error, 0)
            logging.debug('_send_random_message to %s:%d -- try %d success', topic, partition, j)

    def _kill_leader(self, topic, partition):
        leader = self.client.topics_to_brokers[TopicAndPartition(topic, partition)]
        broker = self.brokers[leader.nodeId]
        broker.close()
        return broker

    def assert_message_count(self, topic, check_count, timeout=10, partitions=None):
        hosts = ','.join(['%s:%d' % (broker.host, broker.port)
                          for broker in self.brokers])

        client = KafkaClient(hosts)
        group = random_string(10)
        consumer = SimpleConsumer(client, group, topic,
                                  partitions=partitions,
                                  auto_commit=False,
                                  iter_timeout=timeout)

        started_at = time.time()
        pending = consumer.pending(partitions)

        # Keep checking if it isn't immediately correct, subject to timeout
        while pending != check_count and (time.time() - started_at < timeout):
            pending = consumer.pending(partitions)

        consumer.stop()
        client.close()

        self.assertEqual(pending, check_count)
