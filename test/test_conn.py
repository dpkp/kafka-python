import socket
import struct

import mock
from . import unittest

from kafka.common import ConnectionError
from kafka.conn import KafkaConnection, collect_hosts, DEFAULT_SOCKET_TIMEOUT_SECONDS

class ConnTest(unittest.TestCase):
    def setUp(self):
        self.config = {
            'host': 'localhost',
            'port': 9090,
            'request_id': 0,
            'payload': b'test data',
            'payload2': b'another packet'
        }

        # Mocking socket.create_connection will cause _sock to always be a
        # MagicMock()
        patcher = mock.patch('socket.create_connection', spec=True)
        self.MockCreateConn = patcher.start()
        self.addCleanup(patcher.stop)

        # Also mock socket.sendall() to appear successful
        self.MockCreateConn().sendall.return_value = None

        # And mock socket.recv() to return two payloads, then '', then raise
        # Note that this currently ignores the num_bytes parameter to sock.recv()
        payload_size = len(self.config['payload'])
        payload2_size = len(self.config['payload2'])
        self.MockCreateConn().recv.side_effect = [
            struct.pack('>i', payload_size),
            struct.pack('>%ds' % payload_size, self.config['payload']),
            struct.pack('>i', payload2_size),
            struct.pack('>%ds' % payload2_size, self.config['payload2']),
            b''
        ]

        # Create a connection object
        self.conn = KafkaConnection(self.config['host'], self.config['port'])

        # Reset any mock counts caused by __init__
        self.MockCreateConn.reset_mock()

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
        self.conn.send(self.config['request_id'], self.config['payload'])
        self.conn._sock.sendall.assert_called_with(self.config['payload'])

    def test_init_creates_socket_connection(self):
        KafkaConnection(self.config['host'], self.config['port'])
        self.MockCreateConn.assert_called_with((self.config['host'], self.config['port']), DEFAULT_SOCKET_TIMEOUT_SECONDS)

    def test_init_failure_raises_connection_error(self):

        def raise_error(*args):
            raise socket.error

        assert socket.create_connection is self.MockCreateConn
        socket.create_connection.side_effect=raise_error
        with self.assertRaises(ConnectionError):
            KafkaConnection(self.config['host'], self.config['port'])

    def test_send__reconnects_on_dirty_conn(self):

        # Dirty the connection
        try:
            self.conn._raise_connection_error()
        except ConnectionError:
            pass

        # Now test that sending attempts to reconnect
        self.assertEqual(self.MockCreateConn.call_count, 0)
        self.conn.send(self.config['request_id'], self.config['payload'])
        self.assertEqual(self.MockCreateConn.call_count, 1)

    def test_send__failure_sets_dirty_connection(self):

        def raise_error(*args):
            raise socket.error

        assert isinstance(self.conn._sock, mock.Mock)
        self.conn._sock.sendall.side_effect=raise_error
        try:
            self.conn.send(self.config['request_id'], self.config['payload'])
        except ConnectionError:
            self.assertIsNone(self.conn._sock)

    def test_recv(self):

        self.assertEqual(self.conn.recv(self.config['request_id']), self.config['payload'])

    def test_recv__reconnects_on_dirty_conn(self):

        # Dirty the connection
        try:
            self.conn._raise_connection_error()
        except ConnectionError:
            pass

        # Now test that recv'ing attempts to reconnect
        self.assertEqual(self.MockCreateConn.call_count, 0)
        self.conn.recv(self.config['request_id'])
        self.assertEqual(self.MockCreateConn.call_count, 1)

    def test_recv__failure_sets_dirty_connection(self):

        def raise_error(*args):
            raise socket.error

        # test that recv'ing attempts to reconnect
        assert isinstance(self.conn._sock, mock.Mock)
        self.conn._sock.recv.side_effect=raise_error
        try:
            self.conn.recv(self.config['request_id'])
        except ConnectionError:
            self.assertIsNone(self.conn._sock)

    def test_recv__doesnt_consume_extra_data_in_stream(self):

        # Here just test that each call to recv will return a single payload
        self.assertEqual(self.conn.recv(self.config['request_id']), self.config['payload'])
        self.assertEqual(self.conn.recv(self.config['request_id']), self.config['payload2'])

    def test_close__object_is_reusable(self):

        # test that sending to a closed connection
        # will re-connect and send data to the socket
        self.conn.close()
        self.conn.send(self.config['request_id'], self.config['payload'])
        self.assertEqual(self.MockCreateConn.call_count, 1)
        self.conn._sock.sendall.assert_called_with(self.config['payload'])
