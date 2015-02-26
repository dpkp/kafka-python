# -*- coding: utf-8 -*-

import time
import logging

from mock import MagicMock
from . import unittest

from kafka.common import TopicAndPartition, FailedPayloadsError
from kafka.producer.base import Producer
from kafka.producer.base import _send_upstream
from kafka.protocol import CODEC_NONE

import threading
import multiprocessing as mp
try:
    from queue import Empty
except ImportError:
    from Queue import Empty


class TestKafkaProducer(unittest.TestCase):
    def test_producer_message_types(self):

        producer = Producer(MagicMock())
        topic = b"test-topic"
        partition = 0

        bad_data_types = (u'你怎么样?', 12, ['a', 'list'], ('a', 'tuple'), {'a': 'dict'})
        for m in bad_data_types:
            with self.assertRaises(TypeError):
                logging.debug("attempting to send message of type %s", type(m))
                producer.send_messages(topic, partition, m)

        good_data_types = (b'a string!',)
        for m in good_data_types:
            # This should not raise an exception
            producer.send_messages(topic, partition, m)

    def test_topic_message_types(self):
        from kafka.producer.simple import SimpleProducer

        client = MagicMock()

        def partitions(topic):
            return [0, 1]

        client.get_partition_ids_for_topic = partitions

        producer = SimpleProducer(client, random_start=False)
        topic = b"test-topic"
        producer.send_messages(topic, b'hi')
        assert client.send_produce_request.called


class TestKafkaProducerSendUpstream(unittest.TestCase):

    def setUp(self):

        # create a multiprocessing Value to store call counter
        # (magicmock counters don't work with other processes)
        self.send_calls_count = mp.Value('i', 0)

        def send_side_effect(*args, **kwargs):
            self.send_calls_count.value += 1

        self.client = MagicMock()
        self.client.send_produce_request.side_effect = send_side_effect
        self.queue = mp.Queue()

    def _run_process(self, retries_limit=3, sleep_timeout=1):
        # run _send_upstream process with the queue
        self.process = mp.Process(
            target=_send_upstream,
            args=(self.queue, self.client, CODEC_NONE,
                  0.3, # batch time (seconds)
                  3, # batch length
                  Producer.ACK_AFTER_LOCAL_WRITE,
                  Producer.DEFAULT_ACK_TIMEOUT,
                  50, # retry backoff (ms)
                  retries_limit))
        self.process.daemon = True
        self.process.start()
        time.sleep(sleep_timeout)
        self.process.terminate()

    def test_wo_retries(self):

        # lets create a queue and add 10 messages for 1 partition
        for i in range(10):
            self.queue.put((TopicAndPartition("test", 0), "msg %i", "key %i"))

        self._run_process()

        # the queue should be void at the end of the test
        self.assertEqual(self.queue.empty(), True)

        # there should be 4 non-void cals:
        # 3 batches of 3 msgs each + 1 batch of 1 message
        self.assertEqual(self.send_calls_count.value, 4)

    def test_first_send_failed(self):

        # lets create a queue and add 10 messages for 10 different partitions
        # to show how retries should work ideally
        for i in range(10):
            self.queue.put((TopicAndPartition("test", i), "msg %i", "key %i"))

        flag = mp.Value('c', 'f')
        def send_side_effect(reqs, *args, **kwargs):
            self.send_calls_count.value += 1
            if flag.value == 'f':
                flag.value = 't'
                raise FailedPayloadsError(reqs)

        self.client.send_produce_request.side_effect = send_side_effect

        self._run_process(2)

        # the queue should be void at the end of the test
        self.assertEqual(self.queue.empty(), True)

        # there should be 5 non-void cals: 1st failed batch of 3 msgs
        # + 3 batches of 3 msgs each + 1 batch of 1 msg = 1 + 3 + 1 = 5
        self.assertEqual(self.send_calls_count.value, 5)

    def test_with_limited_retries(self):

        # lets create a queue and add 10 messages for 10 different partitions
        # to show how retries should work ideally
        for i in range(10):
            self.queue.put((TopicAndPartition("test", i), "msg %i", "key %i"))

        def send_side_effect(reqs, *args, **kwargs):
            self.send_calls_count.value += 1
            raise FailedPayloadsError(reqs)

        self.client.send_produce_request.side_effect = send_side_effect

        self._run_process(3, 2)

        # the queue should be void at the end of the test
        self.assertEqual(self.queue.empty(), True)

        # there should be 16 non-void cals:
        # 3 initial batches of 3 msgs each + 1 initial batch of 1 msg +
        # 3 retries of the batches above = 4 + 3 * 4 = 16, all failed
        self.assertEqual(self.send_calls_count.value, 16)


    def test_with_unlimited_retries(self):

        # lets create a queue and add 10 messages for 10 different partitions
        # to show how retries should work ideally
        for i in range(10):
            self.queue.put((TopicAndPartition("test", i), "msg %i", "key %i"))

        def send_side_effect(reqs, *args, **kwargs):
            self.send_calls_count.value += 1
            raise FailedPayloadsError(reqs)

        self.client.send_produce_request.side_effect = send_side_effect

        self._run_process(None)

        # the queue should have 7 elements
        # 3 batches of 1 msg each were retried all this time
        self.assertEqual(self.queue.empty(), False)
        left = 0
        for i in range(10):
            try:
                self.queue.get(timeout=0.01)
                left += 1
            except Empty:
                break
        self.assertEqual(left, 7)

        # 1s / 50ms of backoff = 20 times
        self.assertEqual(self.send_calls_count.value, 20)
