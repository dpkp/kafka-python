# -*- coding: utf-8 -*-

import logging

try:
    from queue import Full
except ImportError:
    from Queue import Full
from mock import MagicMock
from . import unittest

from kafka.producer.base import Producer


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

    def test_producer_async_queue_overfilled_batch_send(self):
        queue_size = 2
        producer = Producer(MagicMock(), batch_send=True, maxsize=queue_size)

        topic = b'test-topic'
        partition = 0
        message = b'test-message'

        with self.assertRaises(Full):
            message_list = [message] * (queue_size + 1)
            producer.send_messages(topic, partition, *message_list)
        self.assertEqual(producer.queue.qsize(), queue_size)

    def test_producer_async_queue_overfilled(self):
        queue_size = 2
        producer = Producer(MagicMock(), async=True, maxsize=queue_size)

        topic = b'test-topic'
        partition = 0
        message = b'test-message'

        with self.assertRaises(Full):
            message_list = [message] * (queue_size + 1)
            producer.send_messages(topic, partition, *message_list)
        self.assertEqual(producer.queue.qsize(), queue_size)

    def test_producer_async_queue_normal(self):
        queue_size = 4
        producer = Producer(MagicMock(), async=True, maxsize=queue_size)

        topic = b'test-topic'
        partition = 0
        message = b'test-message'

        acceptable_size = (queue_size / 2 + 1)

        message_list = [message] * acceptable_size
        resp = producer.send_messages(topic, partition, *message_list)
        self.assertEqual(type(resp), list)
        self.assertEqual(producer.queue.qsize(), acceptable_size)
        self.assertFalse(producer.queue.full())
