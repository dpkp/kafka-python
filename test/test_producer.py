# -*- coding: utf-8 -*-

import logging

from mock import MagicMock, Mock, patch
from . import unittest

from kafka.common import ProduceResponse
from kafka.producer.base import Producer
from kafka.producer.kafka import KafkaProducer


class TestBaseProducer(unittest.TestCase):
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

class TestKafkaProducer(unittest.TestCase):

    @patch('kafka.client.KafkaConnection')
    @patch('kafka.client.KafkaProtocol')
    def test_kafka_producer_client_configs(self, protocol, conn):
        conn.recv.return_value = 'response'  # anything but None

        producer = KafkaProducer(
            bootstrap_servers=['foobar:1234', 'foobaz:4321'],
            client_id='barbaz',
            socket_timeout_ms=1234
        )
        self.assertEquals(producer._client.client_id, b'barbaz')
        self.assertEquals(producer._client.timeout, 1.234)
        self.assertEquals(sorted(producer._client.hosts),
                          [('foobar', 1234), ('foobaz', 4321)])

    @patch('kafka.client.KafkaConnection')
    @patch('kafka.client.KafkaProtocol')
    def test_kafka_producer_callback(self, protocol, conn):
        conn.recv.return_value = 'response'  # anything but None

        producer = KafkaProducer(
            bootstrap_servers=['foobar:1234', 'foobaz:4321'],
            client_id='barbaz',
            retries=5
        )
        producer.partitions_for_topic = lambda _: [0, 1]
        producer._client._get_leader_for_partition = MagicMock()
        producer._client.send_produce_request = MagicMock()
        producer._client.send_produce_request.return_value = [
            ProduceResponse('test-topic', 11, 0, 30)
        ]

        mock = Mock()
        producer.send(topic='test-topic', value=u'你怎么样?', callback=mock.callback)
        producer._producer_queue.join()
        producer.close()

        self.assertTrue(mock.callback.called)
        self.assertEquals(mock.callback.call_count, 1)
        record = mock.callback.call_args[0][0]
        response = mock.callback.call_args[0][1]
        self.assertEquals(record.topic, 'test-topic')
        self.assertEquals(record.value, u'你怎么样?')
        self.assertEquals(response.topic, 'test-topic')
        self.assertEquals(response.partition, 11)
        self.assertEquals(response.offset, 30)
