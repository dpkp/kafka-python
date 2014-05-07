import os
from datetime import datetime

from kafka import *  # noqa
from kafka.common import *  # noqa
from kafka.consumer import MAX_FETCH_BUFFER_SIZE_BYTES
from fixtures import ZookeeperFixture, KafkaFixture
from testutil import *

class TestConsumerIntegration(KafkaIntegrationTestCase):
    @classmethod
    def setUpClass(cls):
        if not os.environ.get('KAFKA_VERSION'):
            return

        cls.zk = ZookeeperFixture.instance()
        cls.server1 = KafkaFixture.instance(0, cls.zk.host, cls.zk.port)
        cls.server2 = KafkaFixture.instance(1, cls.zk.host, cls.zk.port)

        cls.server = cls.server1 # Bootstrapping server

    @classmethod
    def tearDownClass(cls):
        if not os.environ.get('KAFKA_VERSION'):
            return

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

    @kafka_versions("all")
    def test_simple_consumer(self):
        self.send_messages(0, range(0, 100))
        self.send_messages(1, range(100, 200))

        # Start a consumer
        consumer = self.consumer()

        self.assert_message_count([ message for message in consumer ], 200)

        consumer.stop()

    @kafka_versions("all")
    def test_simple_consumer__seek(self):
        self.send_messages(0, range(0, 100))
        self.send_messages(1, range(100, 200))

        consumer = self.consumer()

        # Rewind 10 messages from the end
        consumer.seek(-10, 2)
        self.assert_message_count([ message for message in consumer ], 10)

        # Rewind 13 messages from the end
        consumer.seek(-13, 2)
        self.assert_message_count([ message for message in consumer ], 13)

        consumer.stop()

    @kafka_versions("all")
    def test_simple_consumer_blocking(self):
        consumer = self.consumer()

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

    @kafka_versions("all")
    def test_simple_consumer_pending(self):
        # Produce 10 messages to partitions 0 and 1
        self.send_messages(0, range(0, 10))
        self.send_messages(1, range(10, 20))

        consumer = self.consumer()

        self.assertEquals(consumer.pending(), 20)
        self.assertEquals(consumer.pending(partitions=[0]), 10)
        self.assertEquals(consumer.pending(partitions=[1]), 10)

        consumer.stop()

    @kafka_versions("all")
    def test_multi_process_consumer(self):
        # Produce 100 messages to partitions 0 and 1
        self.send_messages(0, range(0, 100))
        self.send_messages(1, range(100, 200))

        consumer = self.consumer(consumer = MultiProcessConsumer)

        self.assert_message_count([ message for message in consumer ], 200)

        consumer.stop()

    @kafka_versions("all")
    def test_multi_process_consumer_blocking(self):
        consumer = self.consumer(consumer = MultiProcessConsumer)

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

    @kafka_versions("all")
    def test_multi_proc_pending(self):
        self.send_messages(0, range(0, 10))
        self.send_messages(1, range(10, 20))

        consumer = MultiProcessConsumer(self.client, "group1", self.topic, auto_commit=False)

        self.assertEquals(consumer.pending(), 20)
        self.assertEquals(consumer.pending(partitions=[0]), 10)
        self.assertEquals(consumer.pending(partitions=[1]), 10)

        consumer.stop()

    @kafka_versions("all")
    def test_large_messages(self):
        # Produce 10 "normal" size messages
        small_messages = self.send_messages(0, [ str(x) for x in range(10) ])

        # Produce 10 messages that are large (bigger than default fetch size)
        large_messages = self.send_messages(0, [ random_string(5000) for x in range(10) ])

        # Consumer should still get all of them
        consumer = self.consumer()

        expected_messages = set(small_messages + large_messages)
        actual_messages = set([ x.message.value for x in consumer ])
        self.assertEqual(expected_messages, actual_messages)

        consumer.stop()

    @kafka_versions("all")
    def test_huge_messages(self):
        huge_message, = self.send_messages(0, [
            create_message(random_string(MAX_FETCH_BUFFER_SIZE_BYTES + 10)),
        ])

        # Create a consumer with the default buffer size
        consumer = self.consumer()

        # This consumer failes to get the message
        with self.assertRaises(ConsumerFetchSizeTooSmall):
            consumer.get_message(False, 0.1)

        consumer.stop()

        # Create a consumer with no fetch size limit
        big_consumer = self.consumer(
            max_buffer_size = None,
            partitions = [0],
        )

        # Seek to the last message
        big_consumer.seek(-1, 2)

        # Consume giant message successfully
        message = big_consumer.get_message(block=False, timeout=10)
        self.assertIsNotNone(message)
        self.assertEquals(message.message.value, huge_message)

        big_consumer.stop()

    @kafka_versions("0.8.1")
    def test_offset_behavior__resuming_behavior(self):
        msgs1 = self.send_messages(0, range(0, 100))
        msgs2 = self.send_messages(1, range(100, 200))

        # Start a consumer
        consumer1 = self.consumer(
            auto_commit_every_t = None,
            auto_commit_every_n = 20,
        )

        # Grab the first 195 messages
        output_msgs1 = [ consumer1.get_message().message.value for _ in xrange(195) ]
        self.assert_message_count(output_msgs1, 195)

        # The total offset across both partitions should be at 180
        consumer2 = self.consumer(
            auto_commit_every_t = None,
            auto_commit_every_n = 20,
        )

        # 181-200
        self.assert_message_count([ message for message in consumer2 ], 20)

        consumer1.stop()
        consumer2.stop()

    def consumer(self, **kwargs):
        if os.environ['KAFKA_VERSION'] == "0.8.0":
            # Kafka 0.8.0 simply doesn't support offset requests, so hard code it being off
            kwargs['auto_commit'] = False
        else:
            kwargs.setdefault('auto_commit', True)

        consumer_class = kwargs.pop('consumer', SimpleConsumer)
        group = kwargs.pop('group', self.id())
        topic = kwargs.pop('topic', self.topic)

        if consumer_class == SimpleConsumer:
            kwargs.setdefault('iter_timeout', 0)

        return consumer_class(self.client, group, topic, **kwargs)
