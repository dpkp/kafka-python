import logging
import os
import time

from kafka import SimpleClient, SimpleConsumer, KeyedProducer
from kafka.errors import (
    FailedPayloadsError, ConnectionError, RequestTimedOutError,
    NotLeaderForPartitionError)
from kafka.producer.base import Producer
from kafka.structs import TopicPartition

from test.fixtures import ZookeeperFixture, KafkaFixture
from test.testutil import KafkaIntegrationTestCase, random_string


log = logging.getLogger(__name__)


class TestFailover(KafkaIntegrationTestCase):
    create_client = False

    def setUp(self):
        if not os.environ.get('KAFKA_VERSION'):
            self.skipTest('integration test requires KAFKA_VERSION')

        zk_chroot = random_string(10)
        replicas = 3
        partitions = 3

        # mini zookeeper, 3 kafka brokers
        self.zk = ZookeeperFixture.instance()
        kk_args = [self.zk.host, self.zk.port]
        kk_kwargs = {'zk_chroot': zk_chroot, 'replicas': replicas,
                     'partitions': partitions}
        self.brokers = [KafkaFixture.instance(i, *kk_args, **kk_kwargs)
                        for i in range(replicas)]

        hosts = ['%s:%d' % (b.host, b.port) for b in self.brokers]
        self.client = SimpleClient(hosts, timeout=2)
        super(TestFailover, self).setUp()

    def tearDown(self):
        super(TestFailover, self).tearDown()
        if not os.environ.get('KAFKA_VERSION'):
            return

        self.client.close()
        for broker in self.brokers:
            broker.close()
        self.zk.close()

    def test_switch_leader(self):
        topic = self.topic
        partition = 0

        # Testing the base Producer class here so that we can easily send
        # messages to a specific partition, kill the leader for that partition
        # and check that after another broker takes leadership the producer
        # is able to resume sending messages

        # require that the server commit messages to all in-sync replicas
        # so that failover doesn't lose any messages on server-side
        # and we can assert that server-side message count equals client-side
        producer = Producer(self.client, async=False,
                            req_acks=Producer.ACK_AFTER_CLUSTER_COMMIT)

        # Send 100 random messages to a specific partition
        self._send_random_messages(producer, topic, partition, 100)

        # kill leader for partition
        self._kill_leader(topic, partition)

        # expect failure, but don't wait more than 60 secs to recover
        recovered = False
        started = time.time()
        timeout = 60
        while not recovered and (time.time() - started) < timeout:
            try:
                log.debug("attempting to send 'success' message after leader killed")
                producer.send_messages(topic, partition, b'success')
                log.debug("success!")
                recovered = True
            except (FailedPayloadsError, ConnectionError, RequestTimedOutError,
                    NotLeaderForPartitionError):
                log.debug("caught exception sending message -- will retry")
                continue

        # Verify we successfully sent the message
        self.assertTrue(recovered)

        # send some more messages to new leader
        self._send_random_messages(producer, topic, partition, 100)

        # count number of messages
        # Should be equal to 100 before + 1 recovery + 100 after
        # at_least=True because exactly once delivery isn't really a thing
        self.assert_message_count(topic, 201, partitions=(partition,),
                                  at_least=True)

    def test_switch_leader_async(self):
        topic = self.topic
        partition = 0

        # Test the base class Producer -- send_messages to a specific partition
        producer = Producer(self.client, async=True,
                            batch_send_every_n=15,
                            batch_send_every_t=3,
                            req_acks=Producer.ACK_AFTER_CLUSTER_COMMIT,
                            async_log_messages_on_error=False)

        # Send 10 random messages
        self._send_random_messages(producer, topic, partition, 10)
        self._send_random_messages(producer, topic, partition + 1, 10)

        # kill leader for partition
        self._kill_leader(topic, partition)

        log.debug("attempting to send 'success' message after leader killed")

        # in async mode, this should return immediately
        producer.send_messages(topic, partition, b'success')
        producer.send_messages(topic, partition + 1, b'success')

        # send to new leader
        self._send_random_messages(producer, topic, partition, 10)
        self._send_random_messages(producer, topic, partition + 1, 10)

        # Stop the producer and wait for it to shutdown
        producer.stop()
        started = time.time()
        timeout = 60
        while (time.time() - started) < timeout:
            if not producer.thread.is_alive():
                break
            time.sleep(0.1)
        else:
            self.fail('timeout waiting for producer queue to empty')

        # count number of messages
        # Should be equal to 10 before + 1 recovery + 10 after
        # at_least=True because exactly once delivery isn't really a thing
        self.assert_message_count(topic, 21, partitions=(partition,),
                                  at_least=True)
        self.assert_message_count(topic, 21, partitions=(partition + 1,),
                                  at_least=True)

    def test_switch_leader_keyed_producer(self):
        topic = self.topic

        producer = KeyedProducer(self.client, async=False)

        # Send 10 random messages
        for _ in range(10):
            key = random_string(3).encode('utf-8')
            msg = random_string(10).encode('utf-8')
            producer.send_messages(topic, key, msg)

        # kill leader for partition 0
        self._kill_leader(topic, 0)

        recovered = False
        started = time.time()
        timeout = 60
        while not recovered and (time.time() - started) < timeout:
            try:
                key = random_string(3).encode('utf-8')
                msg = random_string(10).encode('utf-8')
                producer.send_messages(topic, key, msg)
                if producer.partitioners[topic].partition(key) == 0:
                    recovered = True
            except (FailedPayloadsError, ConnectionError, RequestTimedOutError,
                    NotLeaderForPartitionError):
                log.debug("caught exception sending message -- will retry")
                continue

        # Verify we successfully sent the message
        self.assertTrue(recovered)

        # send some more messages just to make sure no more exceptions
        for _ in range(10):
            key = random_string(3).encode('utf-8')
            msg = random_string(10).encode('utf-8')
            producer.send_messages(topic, key, msg)

    def test_switch_leader_simple_consumer(self):
        producer = Producer(self.client, async=False)
        consumer = SimpleConsumer(self.client, None, self.topic, partitions=None, auto_commit=False, iter_timeout=10)
        self._send_random_messages(producer, self.topic, 0, 2)
        consumer.get_messages()
        self._kill_leader(self.topic, 0)
        consumer.get_messages()

    def _send_random_messages(self, producer, topic, partition, n):
        for j in range(n):
            msg = 'msg {0}: {1}'.format(j, random_string(10))
            log.debug('_send_random_message %s to %s:%d', msg, topic, partition)
            while True:
                try:
                    producer.send_messages(topic, partition, msg.encode('utf-8'))
                except:
                    log.exception('failure in _send_random_messages - retrying')
                    continue
                else:
                    break

    def _kill_leader(self, topic, partition):
        leader = self.client.topics_to_brokers[TopicPartition(topic, partition)]
        broker = self.brokers[leader.nodeId]
        broker.close()
        return broker

    def assert_message_count(self, topic, check_count, timeout=10,
                             partitions=None, at_least=False):
        hosts = ','.join(['%s:%d' % (broker.host, broker.port)
                          for broker in self.brokers])

        client = SimpleClient(hosts, timeout=2)
        consumer = SimpleConsumer(client, None, topic,
                                  partitions=partitions,
                                  auto_commit=False,
                                  iter_timeout=timeout)

        started_at = time.time()
        pending = -1
        while pending < check_count and (time.time() - started_at < timeout):
            try:
                pending = consumer.pending(partitions)
            except FailedPayloadsError:
                pass
            time.sleep(0.5)

        consumer.stop()
        client.close()

        if pending < check_count:
            self.fail('Too few pending messages: found %d, expected %d' %
                      (pending, check_count))
        elif pending > check_count and not at_least:
            self.fail('Too many pending messages: found %d, expected %d' %
                      (pending, check_count))
        return True
