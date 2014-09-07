# -*- coding: utf-8 -*-

import logging

import unittest2
from mock import MagicMock

from kafka.producer import Producer

class TestKafkaProducer(unittest2.TestCase):
    def test_producer_message_types(self):

        producer = Producer(MagicMock())
        topic = "test-topic"
        partition = 0

        bad_data_types = (u'你怎么样?', 12, ['a','list'], ('a','tuple'), {'a': 'dict'})
        for m in bad_data_types:
            with self.assertRaises(TypeError):
                logging.debug("attempting to send message of type %s", type(m))
                producer.send_messages(topic, partition, m)

        good_data_types = ('a string!',)
        for m in good_data_types:
            # This should not raise an exception
            producer.send_messages(topic, partition, m)

