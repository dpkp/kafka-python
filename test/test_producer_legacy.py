# -*- coding: utf-8 -*-

import collections
import logging
import threading
import time

from mock import MagicMock, patch
from . import unittest

from kafka import SimpleClient, SimpleProducer, KeyedProducer
from kafka.errors import (
    AsyncProducerQueueFull, FailedPayloadsError, NotLeaderForPartitionError)
from kafka.producer.base import Producer, _send_upstream
from kafka.protocol import CODEC_NONE
from kafka.structs import (
    ProduceResponsePayload, RetryOptions, TopicPartition)

from six.moves import queue, xrange


class TestKafkaProducer(unittest.TestCase):
    def test_producer_message_types(self):

        producer = Producer(MagicMock())
        topic = b"test-topic"
        partition = 0

        bad_data_types = (u'你怎么样?', 12, ['a', 'list'],
                          ('a', 'tuple'), {'a': 'dict'}, None,)
        for m in bad_data_types:
            with self.assertRaises(TypeError):
                logging.debug("attempting to send message of type %s", type(m))
                producer.send_messages(topic, partition, m)

        good_data_types = (b'a string!',)
        for m in good_data_types:
            # This should not raise an exception
            producer.send_messages(topic, partition, m)

    def test_keyedproducer_message_types(self):
        client = MagicMock()
        client.get_partition_ids_for_topic.return_value = [0, 1]
        producer = KeyedProducer(client)
        topic = b"test-topic"
        key = b"testkey"

        bad_data_types = (u'你怎么样?', 12, ['a', 'list'],
                          ('a', 'tuple'), {'a': 'dict'},)
        for m in bad_data_types:
            with self.assertRaises(TypeError):
                logging.debug("attempting to send message of type %s", type(m))
                producer.send_messages(topic, key, m)

        good_data_types = (b'a string!', None,)
        for m in good_data_types:
            # This should not raise an exception
            producer.send_messages(topic, key, m)

    def test_topic_message_types(self):
        client = MagicMock()

        def partitions(topic):
            return [0, 1]

        client.get_partition_ids_for_topic = partitions

        producer = SimpleProducer(client, random_start=False)
        topic = b"test-topic"
        producer.send_messages(topic, b'hi')
        assert client.send_produce_request.called

    @patch('kafka.producer.base._send_upstream')
    def test_producer_async_queue_overfilled(self, mock):
        queue_size = 2
        producer = Producer(MagicMock(), async=True,
                            async_queue_maxsize=queue_size)

        topic = b'test-topic'
        partition = 0
        message = b'test-message'

        with self.assertRaises(AsyncProducerQueueFull):
            message_list = [message] * (queue_size + 1)
            producer.send_messages(topic, partition, *message_list)
        self.assertEqual(producer.queue.qsize(), queue_size)
        for _ in xrange(producer.queue.qsize()):
            producer.queue.get()

    def test_producer_sync_fail_on_error(self):
        error = FailedPayloadsError('failure')
        with patch.object(SimpleClient, 'load_metadata_for_topics'):
            with patch.object(SimpleClient, 'ensure_topic_exists'):
                with patch.object(SimpleClient, 'get_partition_ids_for_topic', return_value=[0, 1]):
                    with patch.object(SimpleClient, '_send_broker_aware_request', return_value = [error]):

                        client = SimpleClient(MagicMock())
                        producer = SimpleProducer(client, async=False, sync_fail_on_error=False)

                        # This should not raise
                        (response,) = producer.send_messages('foobar', b'test message')
                        self.assertEqual(response, error)

                        producer = SimpleProducer(client, async=False, sync_fail_on_error=True)
                        with self.assertRaises(FailedPayloadsError):
                            producer.send_messages('foobar', b'test message')

    def test_cleanup_is_not_called_on_stopped_producer(self):
        producer = Producer(MagicMock(), async=True)
        producer.stopped = True
        with patch.object(producer, 'stop') as mocked_stop:
            producer._cleanup_func(producer)
            self.assertEqual(mocked_stop.call_count, 0)

    def test_cleanup_is_called_on_running_producer(self):
        producer = Producer(MagicMock(), async=True)
        producer.stopped = False
        with patch.object(producer, 'stop') as mocked_stop:
            producer._cleanup_func(producer)
            self.assertEqual(mocked_stop.call_count, 1)


class TestKafkaProducerSendUpstream(unittest.TestCase):

    def setUp(self):
        self.client = MagicMock()
        self.queue = queue.Queue()

    def _run_process(self, retries_limit=3, sleep_timeout=1):
        # run _send_upstream process with the queue
        stop_event = threading.Event()
        retry_options = RetryOptions(limit=retries_limit,
                                     backoff_ms=50,
                                     retry_on_timeouts=False)
        self.thread = threading.Thread(
            target=_send_upstream,
            args=(self.queue, self.client, CODEC_NONE,
                  0.3, # batch time (seconds)
                  3, # batch length
                  Producer.ACK_AFTER_LOCAL_WRITE,
                  Producer.DEFAULT_ACK_TIMEOUT,
                  retry_options,
                  stop_event))
        self.thread.daemon = True
        self.thread.start()
        time.sleep(sleep_timeout)
        stop_event.set()

    def test_wo_retries(self):

        # lets create a queue and add 10 messages for 1 partition
        for i in range(10):
            self.queue.put((TopicPartition("test", 0), "msg %i", "key %i"))

        self._run_process()

        # the queue should be void at the end of the test
        self.assertEqual(self.queue.empty(), True)

        # there should be 4 non-void cals:
        # 3 batches of 3 msgs each + 1 batch of 1 message
        self.assertEqual(self.client.send_produce_request.call_count, 4)

    def test_first_send_failed(self):

        # lets create a queue and add 10 messages for 10 different partitions
        # to show how retries should work ideally
        for i in range(10):
            self.queue.put((TopicPartition("test", i), "msg %i", "key %i"))

        # Mock offsets counter for closure
        offsets = collections.defaultdict(lambda: collections.defaultdict(lambda: 0))
        self.client.is_first_time = True
        def send_side_effect(reqs, *args, **kwargs):
            if self.client.is_first_time:
                self.client.is_first_time = False
                return [FailedPayloadsError(req) for req in reqs]
            responses = []
            for req in reqs:
                offset = offsets[req.topic][req.partition]
                offsets[req.topic][req.partition] += len(req.messages)
                responses.append(
                    ProduceResponsePayload(req.topic, req.partition, 0, offset)
                )
            return responses

        self.client.send_produce_request.side_effect = send_side_effect

        self._run_process(2)

        # the queue should be void at the end of the test
        self.assertEqual(self.queue.empty(), True)

        # there should be 5 non-void calls: 1st failed batch of 3 msgs
        # plus 3 batches of 3 msgs each + 1 batch of 1 message
        self.assertEqual(self.client.send_produce_request.call_count, 5)

    def test_with_limited_retries(self):

        # lets create a queue and add 10 messages for 10 different partitions
        # to show how retries should work ideally
        for i in range(10):
            self.queue.put((TopicPartition("test", i), "msg %i" % i, "key %i" % i))

        def send_side_effect(reqs, *args, **kwargs):
            return [FailedPayloadsError(req) for req in reqs]

        self.client.send_produce_request.side_effect = send_side_effect

        self._run_process(3, 3)

        # the queue should be void at the end of the test
        self.assertEqual(self.queue.empty(), True)

        # there should be 16 non-void calls:
        # 3 initial batches of 3 msgs each + 1 initial batch of 1 msg +
        # 3 retries of the batches above = (1 + 3 retries) * 4 batches = 16
        self.assertEqual(self.client.send_produce_request.call_count, 16)

    def test_async_producer_not_leader(self):

        for i in range(10):
            self.queue.put((TopicPartition("test", i), "msg %i", "key %i"))

        # Mock offsets counter for closure
        offsets = collections.defaultdict(lambda: collections.defaultdict(lambda: 0))
        self.client.is_first_time = True
        def send_side_effect(reqs, *args, **kwargs):
            if self.client.is_first_time:
                self.client.is_first_time = False
                return [ProduceResponsePayload(req.topic, req.partition,
                                               NotLeaderForPartitionError.errno, -1)
                        for req in reqs]

            responses = []
            for req in reqs:
                offset = offsets[req.topic][req.partition]
                offsets[req.topic][req.partition] += len(req.messages)
                responses.append(
                    ProduceResponsePayload(req.topic, req.partition, 0, offset)
                )
            return responses

        self.client.send_produce_request.side_effect = send_side_effect

        self._run_process(2)

        # the queue should be void at the end of the test
        self.assertEqual(self.queue.empty(), True)

        # there should be 5 non-void calls: 1st failed batch of 3 msgs
        # + 3 batches of 3 msgs each + 1 batch of 1 msg = 1 + 3 + 1 = 5
        self.assertEqual(self.client.send_produce_request.call_count, 5)

    def tearDown(self):
        for _ in xrange(self.queue.qsize()):
            self.queue.get()
