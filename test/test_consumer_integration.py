import logging
import os

from six.moves import xrange

from . import unittest
from kafka import (
    KafkaConsumer, MultiProcessConsumer, SimpleConsumer, create_message, create_gzip_message
)
from kafka.consumer.base import MAX_FETCH_BUFFER_SIZE_BYTES
from kafka.errors import ConsumerFetchSizeTooSmall, OffsetOutOfRangeError
from kafka.structs import ProduceRequestPayload, TopicPartition

from test.fixtures import ZookeeperFixture, KafkaFixture
from test.testutil import (
    KafkaIntegrationTestCase, kafka_versions, random_string, Timer
)


class TestConsumerIntegration(KafkaIntegrationTestCase):
    @classmethod
    def setUpClass(cls):
        if not os.environ.get('KAFKA_VERSION'):
            return

        cls.zk = ZookeeperFixture.instance()
        chroot = random_string(10)
        cls.server1 = KafkaFixture.instance(0, cls.zk.host, cls.zk.port,
                                            zk_chroot=chroot)
        cls.server2 = KafkaFixture.instance(1, cls.zk.host, cls.zk.port,
                                            zk_chroot=chroot)

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
        produce = ProduceRequestPayload(self.topic, partition, messages = messages)
        resp, = self.client.send_produce_request([produce])
        self.assertEqual(resp.error, 0)

        return [ x.value for x in messages ]

    def send_gzip_message(self, partition, messages):
        message = create_gzip_message([(self.msg(str(msg)), None) for msg in messages])
        produce = ProduceRequestPayload(self.topic, partition, messages = [message])
        resp, = self.client.send_produce_request([produce])
        self.assertEqual(resp.error, 0)

    def assert_message_count(self, messages, num_messages):
        # Make sure we got them all
        self.assertEqual(len(messages), num_messages)

        # Make sure there are no duplicates
        self.assertEqual(len(set(messages)), num_messages)

    def consumer(self, **kwargs):
        if os.environ['KAFKA_VERSION'] == "0.8.0":
            # Kafka 0.8.0 simply doesn't support offset requests, so hard code it being off
            kwargs['group'] = None
            kwargs['auto_commit'] = False
        else:
            kwargs.setdefault('group', None)
            kwargs.setdefault('auto_commit', False)

        consumer_class = kwargs.pop('consumer', SimpleConsumer)
        group = kwargs.pop('group', None)
        topic = kwargs.pop('topic', self.topic)

        if consumer_class in [SimpleConsumer, MultiProcessConsumer]:
            kwargs.setdefault('iter_timeout', 0)

        return consumer_class(self.client, group, topic, **kwargs)

    def kafka_consumer(self, **configs):
        brokers = '%s:%d' % (self.server.host, self.server.port)
        consumer = KafkaConsumer(self.topic,
                                 bootstrap_servers=brokers,
                                 **configs)
        return consumer

    def test_simple_consumer(self):
        self.send_messages(0, range(0, 100))
        self.send_messages(1, range(100, 200))

        # Start a consumer
        consumer = self.consumer()

        self.assert_message_count([ message for message in consumer ], 200)

        consumer.stop()

    def test_simple_consumer_gzip(self):
        self.send_gzip_message(0, range(0, 100))
        self.send_gzip_message(1, range(100, 200))

        # Start a consumer
        consumer = self.consumer()

        self.assert_message_count([ message for message in consumer ], 200)

        consumer.stop()

    def test_simple_consumer_smallest_offset_reset(self):
        self.send_messages(0, range(0, 100))
        self.send_messages(1, range(100, 200))

        consumer = self.consumer(auto_offset_reset='smallest')
        # Move fetch offset ahead of 300 message (out of range)
        consumer.seek(300, 2)
        # Since auto_offset_reset is set to smallest we should read all 200
        # messages from beginning.
        self.assert_message_count([message for message in consumer], 200)

    def test_simple_consumer_largest_offset_reset(self):
        self.send_messages(0, range(0, 100))
        self.send_messages(1, range(100, 200))

        # Default largest
        consumer = self.consumer()
        # Move fetch offset ahead of 300 message (out of range)
        consumer.seek(300, 2)
        # Since auto_offset_reset is set to largest we should not read any
        # messages.
        self.assert_message_count([message for message in consumer], 0)
        # Send 200 new messages to the queue
        self.send_messages(0, range(200, 300))
        self.send_messages(1, range(300, 400))
        # Since the offset is set to largest we should read all the new messages.
        self.assert_message_count([message for message in consumer], 200)

    def test_simple_consumer_no_reset(self):
        self.send_messages(0, range(0, 100))
        self.send_messages(1, range(100, 200))

        # Default largest
        consumer = self.consumer(auto_offset_reset=None)
        # Move fetch offset ahead of 300 message (out of range)
        consumer.seek(300, 2)
        with self.assertRaises(OffsetOutOfRangeError):
            consumer.get_message()

    @kafka_versions('>=0.8.1')
    def test_simple_consumer_load_initial_offsets(self):
        self.send_messages(0, range(0, 100))
        self.send_messages(1, range(100, 200))

        # Create 1st consumer and change offsets
        consumer = self.consumer(group='test_simple_consumer_load_initial_offsets')
        self.assertEqual(consumer.offsets, {0: 0, 1: 0})
        consumer.offsets.update({0:51, 1:101})
        # Update counter after manual offsets update
        consumer.count_since_commit += 1
        consumer.commit()

        # Create 2nd consumer and check initial offsets
        consumer = self.consumer(group='test_simple_consumer_load_initial_offsets',
                                 auto_commit=False)
        self.assertEqual(consumer.offsets, {0: 51, 1: 101})

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

        # Set absolute offset
        consumer.seek(100)
        self.assert_message_count([ message for message in consumer ], 0)
        consumer.seek(100, partition=0)
        self.assert_message_count([ message for message in consumer ], 0)
        consumer.seek(101, partition=1)
        self.assert_message_count([ message for message in consumer ], 0)
        consumer.seek(90, partition=0)
        self.assert_message_count([ message for message in consumer ], 10)
        consumer.seek(20, partition=1)
        self.assert_message_count([ message for message in consumer ], 80)
        consumer.seek(0, partition=1)
        self.assert_message_count([ message for message in consumer ], 100)

        consumer.stop()

    def test_simple_consumer_blocking(self):
        consumer = self.consumer()

        # Ask for 5 messages, nothing in queue, block 1 second
        with Timer() as t:
            messages = consumer.get_messages(block=True, timeout=1)
            self.assert_message_count(messages, 0)
        self.assertGreaterEqual(t.interval, 1)

        self.send_messages(0, range(0, 5))
        self.send_messages(1, range(5, 10))

        # Ask for 5 messages, 10 in queue. Get 5 back, no blocking
        with Timer() as t:
            messages = consumer.get_messages(count=5, block=True, timeout=3)
            self.assert_message_count(messages, 5)
        self.assertLess(t.interval, 3)

        # Ask for 10 messages, get 5 back, block 1 second
        with Timer() as t:
            messages = consumer.get_messages(count=10, block=True, timeout=1)
            self.assert_message_count(messages, 5)
        self.assertGreaterEqual(t.interval, 1)

        # Ask for 10 messages, 5 in queue, ask to block for 1 message or 1
        # second, get 5 back, no blocking
        self.send_messages(0, range(0, 3))
        self.send_messages(1, range(3, 5))
        with Timer() as t:
            messages = consumer.get_messages(count=10, block=1, timeout=1)
            self.assert_message_count(messages, 5)
        self.assertLessEqual(t.interval, 1)

        consumer.stop()

    def test_simple_consumer_pending(self):
        # make sure that we start with no pending messages
        consumer = self.consumer()
        self.assertEquals(consumer.pending(), 0)
        self.assertEquals(consumer.pending(partitions=[0]), 0)
        self.assertEquals(consumer.pending(partitions=[1]), 0)

        # Produce 10 messages to partitions 0 and 1
        self.send_messages(0, range(0, 10))
        self.send_messages(1, range(10, 20))

        consumer = self.consumer()

        self.assertEqual(consumer.pending(), 20)
        self.assertEqual(consumer.pending(partitions=[0]), 10)
        self.assertEqual(consumer.pending(partitions=[1]), 10)

        # move to last message, so one partition should have 1 pending
        # message and other 0
        consumer.seek(-1, 2)
        self.assertEqual(consumer.pending(), 1)

        pending_part1 = consumer.pending(partitions=[0])
        pending_part2 = consumer.pending(partitions=[1])
        self.assertEquals(set([0, 1]), set([pending_part1, pending_part2]))
        consumer.stop()

    @unittest.skip('MultiProcessConsumer deprecated and these tests are flaky')
    def test_multi_process_consumer(self):
        # Produce 100 messages to partitions 0 and 1
        self.send_messages(0, range(0, 100))
        self.send_messages(1, range(100, 200))

        consumer = self.consumer(consumer = MultiProcessConsumer)

        self.assert_message_count([ message for message in consumer ], 200)

        consumer.stop()

    @unittest.skip('MultiProcessConsumer deprecated and these tests are flaky')
    def test_multi_process_consumer_blocking(self):
        consumer = self.consumer(consumer = MultiProcessConsumer)

        # Ask for 5 messages, No messages in queue, block 1 second
        with Timer() as t:
            messages = consumer.get_messages(block=True, timeout=1)
            self.assert_message_count(messages, 0)

        self.assertGreaterEqual(t.interval, 1)

        # Send 10 messages
        self.send_messages(0, range(0, 10))

        # Ask for 5 messages, 10 messages in queue, block 0 seconds
        with Timer() as t:
            messages = consumer.get_messages(count=5, block=True, timeout=5)
            self.assert_message_count(messages, 5)
        self.assertLessEqual(t.interval, 1)

        # Ask for 10 messages, 5 in queue, block 1 second
        with Timer() as t:
            messages = consumer.get_messages(count=10, block=True, timeout=1)
            self.assert_message_count(messages, 5)
        self.assertGreaterEqual(t.interval, 1)

        # Ask for 10 messages, 5 in queue, ask to block for 1 message or 1
        # second, get at least one back, no blocking
        self.send_messages(0, range(0, 5))
        with Timer() as t:
            messages = consumer.get_messages(count=10, block=1, timeout=1)
            received_message_count = len(messages)
            self.assertGreaterEqual(received_message_count, 1)
            self.assert_message_count(messages, received_message_count)
        self.assertLessEqual(t.interval, 1)

        consumer.stop()

    @unittest.skip('MultiProcessConsumer deprecated and these tests are flaky')
    def test_multi_proc_pending(self):
        self.send_messages(0, range(0, 10))
        self.send_messages(1, range(10, 20))

        # set group to None and auto_commit to False to avoid interactions w/
        # offset commit/fetch apis
        consumer = MultiProcessConsumer(self.client, None, self.topic,
                                        auto_commit=False, iter_timeout=0)

        self.assertEqual(consumer.pending(), 20)
        self.assertEqual(consumer.pending(partitions=[0]), 10)
        self.assertEqual(consumer.pending(partitions=[1]), 10)

        consumer.stop()

    @unittest.skip('MultiProcessConsumer deprecated and these tests are flaky')
    @kafka_versions('>=0.8.1')
    def test_multi_process_consumer_load_initial_offsets(self):
        self.send_messages(0, range(0, 10))
        self.send_messages(1, range(10, 20))

        # Create 1st consumer and change offsets
        consumer = self.consumer(group='test_multi_process_consumer_load_initial_offsets')
        self.assertEqual(consumer.offsets, {0: 0, 1: 0})
        consumer.offsets.update({0:5, 1:15})
        # Update counter after manual offsets update
        consumer.count_since_commit += 1
        consumer.commit()

        # Create 2nd consumer and check initial offsets
        consumer = self.consumer(consumer = MultiProcessConsumer,
                                 group='test_multi_process_consumer_load_initial_offsets',
                                 auto_commit=False)
        self.assertEqual(consumer.offsets, {0: 5, 1: 15})

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
        self.assertEqual(message.message.value, huge_message)

        big_consumer.stop()

    @kafka_versions('>=0.8.1')
    def test_offset_behavior__resuming_behavior(self):
        self.send_messages(0, range(0, 100))
        self.send_messages(1, range(100, 200))

        # Start a consumer
        consumer1 = self.consumer(
            group='test_offset_behavior__resuming_behavior',
            auto_commit=True,
            auto_commit_every_t = None,
            auto_commit_every_n = 20,
        )

        # Grab the first 195 messages
        output_msgs1 = [ consumer1.get_message().message.value for _ in xrange(195) ]
        self.assert_message_count(output_msgs1, 195)

        # The total offset across both partitions should be at 180
        consumer2 = self.consumer(
            group='test_offset_behavior__resuming_behavior',
            auto_commit=True,
            auto_commit_every_t = None,
            auto_commit_every_n = 20,
        )

        # 181-200
        self.assert_message_count([ message for message in consumer2 ], 20)

        consumer1.stop()
        consumer2.stop()

    @unittest.skip('MultiProcessConsumer deprecated and these tests are flaky')
    @kafka_versions('>=0.8.1')
    def test_multi_process_offset_behavior__resuming_behavior(self):
        self.send_messages(0, range(0, 100))
        self.send_messages(1, range(100, 200))

        # Start a consumer
        consumer1 = self.consumer(
            consumer=MultiProcessConsumer,
            group='test_multi_process_offset_behavior__resuming_behavior',
            auto_commit=True,
            auto_commit_every_t = None,
            auto_commit_every_n = 20,
            )

        # Grab the first 195 messages
        output_msgs1 = []
        idx = 0
        for message in consumer1:
            output_msgs1.append(message.message.value)
            idx += 1
            if idx >= 195:
                break
        self.assert_message_count(output_msgs1, 195)

        # The total offset across both partitions should be at 180
        consumer2 = self.consumer(
            consumer=MultiProcessConsumer,
            group='test_multi_process_offset_behavior__resuming_behavior',
            auto_commit=True,
            auto_commit_every_t = None,
            auto_commit_every_n = 20,
            )

        # 181-200
        self.assert_message_count([ message for message in consumer2 ], 20)

        consumer1.stop()
        consumer2.stop()

    # TODO: Make this a unit test -- should not require integration
    def test_fetch_buffer_size(self):

        # Test parameters (see issue 135 / PR 136)
        TEST_MESSAGE_SIZE=1048
        INIT_BUFFER_SIZE=1024
        MAX_BUFFER_SIZE=2048
        assert TEST_MESSAGE_SIZE > INIT_BUFFER_SIZE
        assert TEST_MESSAGE_SIZE < MAX_BUFFER_SIZE
        assert MAX_BUFFER_SIZE == 2 * INIT_BUFFER_SIZE

        self.send_messages(0, [ "x" * 1048 ])
        self.send_messages(1, [ "x" * 1048 ])

        consumer = self.consumer(buffer_size=1024, max_buffer_size=2048)
        messages = [ message for message in consumer ]
        self.assertEqual(len(messages), 2)

    def test_kafka_consumer(self):
        self.send_messages(0, range(0, 100))
        self.send_messages(1, range(100, 200))

        # Start a consumer
        consumer = self.kafka_consumer(auto_offset_reset='earliest')
        n = 0
        messages = {0: set(), 1: set()}
        for m in consumer:
            logging.debug("Consumed message %s" % repr(m))
            n += 1
            messages[m.partition].add(m.offset)
            if n >= 200:
                break

        self.assertEqual(len(messages[0]), 100)
        self.assertEqual(len(messages[1]), 100)

    def test_kafka_consumer__blocking(self):
        TIMEOUT_MS = 500
        consumer = self.kafka_consumer(auto_offset_reset='earliest',
                                       consumer_timeout_ms=TIMEOUT_MS)

        # Manual assignment avoids overhead of consumer group mgmt
        consumer.unsubscribe()
        consumer.assign([TopicPartition(self.topic, 0)])

        # Ask for 5 messages, nothing in queue, block 500ms
        with Timer() as t:
            with self.assertRaises(StopIteration):
                msg = next(consumer)
        self.assertGreaterEqual(t.interval, TIMEOUT_MS / 1000.0 )

        self.send_messages(0, range(0, 10))

        # Ask for 5 messages, 10 in queue. Get 5 back, no blocking
        messages = set()
        with Timer() as t:
            for i in range(5):
                msg = next(consumer)
                messages.add((msg.partition, msg.offset))
        self.assertEqual(len(messages), 5)
        self.assertLess(t.interval, TIMEOUT_MS / 1000.0 )

        # Ask for 10 messages, get 5 back, block 500ms
        messages = set()
        with Timer() as t:
            with self.assertRaises(StopIteration):
                for i in range(10):
                    msg = next(consumer)
                    messages.add((msg.partition, msg.offset))
        self.assertEqual(len(messages), 5)
        self.assertGreaterEqual(t.interval, TIMEOUT_MS / 1000.0 )

    @kafka_versions('>=0.8.1')
    def test_kafka_consumer__offset_commit_resume(self):
        GROUP_ID = random_string(10)

        self.send_messages(0, range(0, 100))
        self.send_messages(1, range(100, 200))

        # Start a consumer
        consumer1 = self.kafka_consumer(
            group_id=GROUP_ID,
            enable_auto_commit=True,
            auto_commit_interval_ms=100,
            auto_offset_reset='earliest',
        )

        # Grab the first 180 messages
        output_msgs1 = []
        for _ in xrange(180):
            m = next(consumer1)
            output_msgs1.append(m)
        self.assert_message_count(output_msgs1, 180)
        consumer1.close()

        # The total offset across both partitions should be at 180
        consumer2 = self.kafka_consumer(
            group_id=GROUP_ID,
            enable_auto_commit=True,
            auto_commit_interval_ms=100,
            auto_offset_reset='earliest',
        )

        # 181-200
        output_msgs2 = []
        for _ in xrange(20):
            m = next(consumer2)
            output_msgs2.append(m)
        self.assert_message_count(output_msgs2, 20)
        self.assertEqual(len(set(output_msgs1) | set(output_msgs2)), 200)
