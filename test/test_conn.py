import os
import random
import socket
import struct

import mock
import unittest2

from kafka.common import *
from kafka.conn import *

class ConnTest(unittest2.TestCase):
    def setUp(self):
        # Mocking socket.create_connection will cause _sock to always be a
        # MagicMock()
        patcher = mock.patch('socket.create_connection', spec=True)
        self.MockCreateConn = patcher.start()

        # Also mock socket.sendall() to appear successful
        self.MockCreateConn().sendall.return_value = None
        self.addCleanup(patcher.stop)

    def test_collect_hosts__happy_path(self):
        hosts = "localhost:1234,localhost"
        results = collect_hosts(hosts)

        self.assertEqual(set(results), set([
            ('localhost', 1234),
            ('localhost', 9092),
        ]))

    def test_collect_hosts__string_list(self):
        hosts = [
            'localhost:1234',
            'localhost',
        ]

        results = collect_hosts(hosts)

        self.assertEqual(set(results), set([
            ('localhost', 1234),
            ('localhost', 9092),
        ]))

    def test_collect_hosts__with_spaces(self):
        hosts = "localhost:1234, localhost"
        results = collect_hosts(hosts)

        self.assertEqual(set(results), set([
            ('localhost', 1234),
            ('localhost', 9092),
        ]))

    def test_send(self):
        fake_config = {
            'host': 'localhost',
            'port': 9090,
            'request_id': 0,
            'payload': 'test data'
        }

        def mock_reinit(obj):
            obj._sock = mock.MagicMock()
            obj._sock.sendall.return_value = None
            obj._dirty = False

        with mock.patch.object(KafkaConnection, 'reinit', new=mock_reinit):
            conn = KafkaConnection(fake_config['host'], fake_config['port'])
            conn.send(fake_config['request_id'], fake_config['payload'])
            conn._sock.sendall.assert_called_with(fake_config['payload'])

    def test_init_creates_socket_connection(self):
        fake_config = {
            'host': 'localhost',
            'port': 9090,
        }

        assert socket.create_connection is self.MockCreateConn
        conn = KafkaConnection(fake_config['host'], fake_config['port'])
        socket.create_connection.assert_called_with((fake_config['host'], fake_config['port']), DEFAULT_SOCKET_TIMEOUT_SECONDS)

    def test_init_failure_raises_connection_error(self):
        fake_config = {
            'host': 'localhost',
            'port': 9090,
        }

        def raise_error(*args):
            raise socket.error

        with mock.patch.object(socket, 'create_connection', new=raise_error):
            with self.assertRaises(ConnectionError):
                KafkaConnection(fake_config['host'], fake_config['port'])

    def test_send__reconnects_on_dirty_conn(self):
        fake_config = {
            'host': 'localhost',
            'port': 9090,
            'request_id': 0,
            'payload': 'test data'
        }

        # Get a connection (with socket mocked)
        assert socket.create_connection is self.MockCreateConn
        conn = KafkaConnection(fake_config['host'], fake_config['port'])
 
        # Dirty it
        
        try:
            conn._raise_connection_error()
        except ConnectionError:
            pass

        # Reset the socket call counts
        socket.create_connection.reset_mock()
        self.assertEqual(socket.create_connection.call_count, 0)

        # Now test that sending attempts to reconnect
        conn.send(fake_config['request_id'], fake_config['payload'])
        self.assertEqual(socket.create_connection.call_count, 1)

        # A second way to dirty it...
        conn.close()

        # Reset the socket call counts
        socket.create_connection.reset_mock()
        self.assertEqual(socket.create_connection.call_count, 0)

        # Now test that sending attempts to reconnect
        conn.send(fake_config['request_id'], fake_config['payload'])
        self.assertEqual(socket.create_connection.call_count, 1)


    @unittest2.skip("Not Implemented")
    def test_send__failure_sets_dirty_connection(self):
        pass

    @unittest2.skip("Not Implemented")
    def test_recv(self):
        pass

    @unittest2.skip("Not Implemented")
    def test_recv__reconnects_on_dirty_conn(self):
        pass

    @unittest2.skip("Not Implemented")
    def test_recv__failure_sets_dirty_connection(self):
        pass

    @unittest2.skip("Not Implemented")
    def test_recv__doesnt_consume_extra_data_in_stream(self):
        pass

    @unittest2.skip("Not Implemented")
    def test_close__object_is_reusable(self):
        pass
