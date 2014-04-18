import unittest
from datetime import datetime

from kafka import *  # noqa
from kafka.common import *  # noqa
from kafka.consumer import MAX_FETCH_BUFFER_SIZE_BYTES
from .fixtures import ZookeeperFixture, KafkaFixture
from .testutil import *

@unittest.skipIf(skip_integration(), 'Skipping Integration')
class TestConsumerIntegration(KafkaIntegrationTestCase):
    @classmethod
    def setUpClass(cls):
        cls.zk = ZookeeperFixture.instance()
        cls.server1 = KafkaFixture.instance(0, cls.zk.host, cls.zk.port)
        cls.server2 = KafkaFixture.instance(1, cls.zk.host, cls.zk.port)

        cls.server = cls.server1 # Bootstrapping server

    @classmethod
    def tearDownClass(cls):  # noqa
        cls.server1.close()
        cls.server2.close()
        cls.zk.close()

    def send_messages(self, partition, messages):
        messages = [ create_message(self.msg(str(msg))) for msg in messages ]
        produce = ProduceRequest(self.topic, partition, messages = messages)
        resp, = self.client.send_produce_request([produce])
        self.assertEquals(resp.error, 0)

        return [ x.value for x in messages ]

    def assert_message_count(self, messages, num_messages):
        # Make sure we got them all
        self.assertEquals(len(messages), num_messages)

        # Make sure there are no duplicates
        self.assertEquals(len(set(messages)), num_messages)

    def test_simple_consumer(self):
        self.send_messages(0, range(0, 100))
        self.send_messages(1, range(100, 200))

        # Start a consumer
        consumer = SimpleConsumer(self.client, "group1",
                                  self.topic, auto_commit=False,
                                  iter_timeout=0)

        self.assert_message_count([ message for message in consumer ], 200)

        consumer.stop()

    def test_simple_consumer__seek(self):
        self.send_messages(0, range(0, 100))
        self.send_messages(1, range(100, 200))

        consumer = SimpleConsumer(self.client, "group1",
                                  self.topic, auto_commit=False,
                                  iter_timeout=0)

        # Rewind 10 messages from the end
        consumer.seek(-10, 2)
        self.assert_message_count([ message for message in consumer ], 10)

        # Rewind 13 messages from the end
        consumer.seek(-13, 2)
        self.assert_message_count([ message for message in consumer ], 13)

        consumer.stop()

    def test_simple_consumer_blocking(self):
        consumer = SimpleConsumer(self.client, "group1",
                                  self.topic,
                                  auto_commit=False, iter_timeout=0)

        # Ask for 5 messages, nothing in queue, block 5 seconds
        with Timer() as t:
            messages = consumer.get_messages(block=True, timeout=5)
            self.assert_message_count(messages, 0)
        self.assertGreaterEqual(t.interval, 5)

        self.send_messages(0, range(0, 10))

        # Ask for 5 messages, 10 in queue. Get 5 back, no blocking
        with Timer() as t:
            messages = consumer.get_messages(count=5, block=True, timeout=5)
            self.assert_message_count(messages, 5)
        self.assertLessEqual(t.interval, 1)

        # Ask for 10 messages, get 5 back, block 5 seconds
        with Timer() as t:
            messages = consumer.get_messages(count=10, block=True, timeout=5)
            self.assert_message_count(messages, 5)
        self.assertGreaterEqual(t.interval, 5)

        consumer.stop()

    def test_simple_consumer_pending(self):
        # Produce 10 messages to partitions 0 and 1
        self.send_messages(0, range(0, 10))
        self.send_messages(1, range(10, 20))

        consumer = SimpleConsumer(self.client, "group1", self.topic,
                                  auto_commit=False, iter_timeout=0)

        self.assertEquals(consumer.pending(), 20)
        self.assertEquals(consumer.pending(partitions=[0]), 10)
        self.assertEquals(consumer.pending(partitions=[1]), 10)

        consumer.stop()

    def test_multi_process_consumer(self):
        # Produce 100 messages to partitions 0 and 1
        self.send_messages(0, range(0, 100))
        self.send_messages(1, range(100, 200))

        consumer = MultiProcessConsumer(self.client, "grp1", self.topic, auto_commit=False)

        self.assert_message_count([ message for message in consumer ], 200)

        consumer.stop()

    def test_multi_process_consumer_blocking(self):
        consumer = MultiProcessConsumer(self.client, "grp1", self.topic, auto_commit=False)

        # Ask for 5 messages, No messages in queue, block 5 seconds
        with Timer() as t:
            messages = consumer.get_messages(block=True, timeout=5)
            self.assert_message_count(messages, 0)

        self.assertGreaterEqual(t.interval, 5)

        # Send 10 messages
        self.send_messages(0, range(0, 10))

        # Ask for 5 messages, 10 messages in queue, block 0 seconds
        with Timer() as t:
            messages = consumer.get_messages(count=5, block=True, timeout=5)
            self.assert_message_count(messages, 5)
        self.assertLessEqual(t.interval, 1)

        # Ask for 10 messages, 5 in queue, block 5 seconds
        with Timer() as t:
            messages = consumer.get_messages(count=10, block=True, timeout=5)
            self.assert_message_count(messages, 5)
        self.assertGreaterEqual(t.interval, 5)

        consumer.stop()

    def test_multi_proc_pending(self):
        self.send_messages(0, range(0, 10))
        self.send_messages(1, range(10, 20))

        consumer = MultiProcessConsumer(self.client, "group1", self.topic, auto_commit=False)

        self.assertEquals(consumer.pending(), 20)
        self.assertEquals(consumer.pending(partitions=[0]), 10)
        self.assertEquals(consumer.pending(partitions=[1]), 10)

        consumer.stop()

    def test_large_messages(self):
        # Produce 10 "normal" size messages
        small_messages = self.send_messages(0, [ str(x) for x in range(10) ])

        # Produce 10 messages that are large (bigger than default fetch size)
        large_messages = self.send_messages(0, [ random_string(5000) for x in range(10) ])

        # Consumer should still get all of them
        consumer = SimpleConsumer(self.client, "group1", self.topic,
                                  auto_commit=False, iter_timeout=0)

        expected_messages = set(small_messages + large_messages)
        actual_messages = set([ x.message.value for x in consumer ])
        self.assertEqual(expected_messages, actual_messages)

        consumer.stop()

    def test_huge_messages(self):
        huge_message, = self.send_messages(0, [
            create_message(random_string(MAX_FETCH_BUFFER_SIZE_BYTES + 10)),
        ])

        # Create a consumer with the default buffer size
        consumer = SimpleConsumer(self.client, "group1", self.topic,
                                  auto_commit=False, iter_timeout=0)

        # This consumer failes to get the message
        with self.assertRaises(ConsumerFetchSizeTooSmall):
            consumer.get_message(False, 0.1)

        consumer.stop()

        # Create a consumer with no fetch size limit
        big_consumer = SimpleConsumer(self.client, "group1", self.topic,
                                      max_buffer_size=None, partitions=[0],
                                      auto_commit=False, iter_timeout=0)

        # Seek to the last message
        big_consumer.seek(-1, 2)

        # Consume giant message successfully
        message = big_consumer.get_message(block=False, timeout=10)
        self.assertIsNotNone(message)
        self.assertEquals(message.message.value, huge_message)

        big_consumer.stop()
