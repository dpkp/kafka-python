
from mock import patch
from . import unittest

from kafka.client_async import KafkaClient
from kafka.common import BrokerMetadata
from kafka.conn import ConnectionStates
from kafka.future import Future
from kafka.protocol.metadata import MetadataResponse, MetadataRequest


class TestAsyncKafkaClient(unittest.TestCase):

    def test_init(self):
        with patch.object(KafkaClient, '_bootstrap') as bootstrap:

            KafkaClient()
            bootstrap.assert_called_with([('localhost', 9092)])

            other_test_cases = [
                ('foobar:1234', [('foobar', 1234)]),
                ('fizzbuzz', [('fizzbuzz', 9092)]),
                ('foo:12,bar:34', [('foo', 12), ('bar', 34)]),
                (['fizz:56', 'buzz'], [('fizz', 56), ('buzz', 9092)])
            ]
            for arg, test in other_test_cases:
                KafkaClient(bootstrap_servers=arg)
                # host order is randomized internally, so resort before testing
                (hosts,), _ = bootstrap.call_args
                assert sorted(hosts) == sorted(test)

    @patch('kafka.client_async.BrokerConnection')
    def test_bootstrap(self, conn):
        conn.return_value = conn
        conn.state = ConnectionStates.CONNECTED
        conn.send.return_value = Future().success(MetadataResponse(
            [(0, 'foo', 12), (1, 'bar', 34)], []))

        cli = KafkaClient()
        conn.assert_called_once_with('localhost', 9092, **cli.config)
        conn.connect.assert_called_with()
        conn.send.assert_called_once_with(MetadataRequest([]))
        assert cli._bootstrap_fails == 0
        assert cli.cluster.brokers() == set([BrokerMetadata(0, 'foo', 12),
                                             BrokerMetadata(1, 'bar', 34)])

        conn.state = ConnectionStates.DISCONNECTED
        cli = KafkaClient()
        conn.connect.assert_called_with()
        conn.close.assert_called_with()
        assert cli._bootstrap_fails == 1

    def test_can_connect(self):
        pass

    def test_initiate_connect(self):
        pass

    def test_finish_connect(self):
        pass

    def test_ready(self):
        pass

    def test_close(self):
        pass

    def test_is_disconnected(self):
        pass

    def test_is_ready(self):
        pass

    def test_can_send_request(self):
        pass

    def test_send(self):
        pass

    def test_poll(self):
        pass

    def test__poll(self):
        pass

    def test_in_flight_request_count(self):
        pass

    def test_least_loaded_node(self):
        pass

    def test_set_topics(self):
        pass

    def test_maybe_refresh_metadata(self):
        pass

    def test_schedule(self):
        pass

    def test_unschedule(self):
        pass

