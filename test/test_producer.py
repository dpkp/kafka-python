# -*- coding: utf-8 -*-

import logging

from mock import MagicMock, patch
from . import unittest

from kafka.common import BatchQueueOverfilledError
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

    @patch('kafka.producer.base.Process')
    def test_producer_async_queue_overfilled_batch_send(self, process_mock):
        queue_size = 2
        producer = Producer(MagicMock(), batch_send=True,
                            async_queue_maxsize=queue_size)

        topic = b'test-topic'
        partition = 0
        message = b'test-message'

        with self.assertRaises(BatchQueueOverfilledError):
            message_list = [message] * (queue_size + 1)
            producer.send_messages(topic, partition, *message_list)

    @patch('kafka.producer.base.Process')
    def test_producer_async_queue_overfilled(self, process_mock):
        queue_size = 2
        producer = Producer(MagicMock(), async=True,
                            async_queue_maxsize=queue_size)

        topic = b'test-topic'
        partition = 0
        message = b'test-message'

        with self.assertRaises(BatchQueueOverfilledError):
            message_list = [message] * (queue_size + 1)
            producer.send_messages(topic, partition, *message_list)
