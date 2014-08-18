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
        self.config = {
            'host': 'localhost',
            'port': 9090,
            'request_id': 0,
            'payload': 'test data'
        }

        # Mocking socket.create_connection will cause _sock to always be a
        # MagicMock()
        patcher = mock.patch('socket.create_connection', spec=True)
        self.MockCreateConn = patcher.start()

        # Also mock socket.sendall() to appear successful
        self.MockCreateConn().sendall.return_value = None
        self.addCleanup(patcher.stop)

        self.conn = KafkaConnection(self.config['host'], self.config['port'])
        socket.create_connection.reset_mock()

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
        socket.create_connection.assert_called_with((self.config['host'], self.config['port']), DEFAULT_SOCKET_TIMEOUT_SECONDS)

    def test_init_failure_raises_connection_error(self):

        def raise_error(*args):
            raise socket.error

        assert socket.create_connection is self.MockCreateConn
        socket.create_connection.side_effect=raise_error
        with self.assertRaises(ConnectionError):
            KafkaConnection(self.config['host'], self.config['port'])

    def test_send__reconnects_on_dirty_conn(self):

        # Dirty the connection
        assert self.conn._dirty is False
        try:
            self.conn._raise_connection_error()
        except ConnectionError:
            pass
        assert self.conn._dirty is True

        # Now test that sending attempts to reconnect
        self.assertEqual(socket.create_connection.call_count, 0)
        self.conn.send(self.config['request_id'], self.config['payload'])
        self.assertEqual(socket.create_connection.call_count, 1)

        # A second way to dirty it...
        self.conn.close()

        # Reset the socket call counts
        socket.create_connection.reset_mock()
        self.assertEqual(socket.create_connection.call_count, 0)

        # Now test that sending attempts to reconnect
        self.conn.send(self.config['request_id'], self.config['payload'])
        self.assertEqual(socket.create_connection.call_count, 1)

    def test_send__failure_sets_dirty_connection(self):

        def raise_error(*args):
            raise socket.error

        assert self.conn._dirty is False

        assert isinstance(self.conn._sock, mock.Mock)
        self.conn._sock.sendall.side_effect=raise_error
        try:
            self.conn.send(self.config['request_id'], self.config['payload'])
        except ConnectionError:
            self.assertEquals(self.conn._dirty, True)

    def test_recv(self):

        # A function to mock _read_bytes
        self.conn._mock_sent_size = False
        self.conn._mock_data_sent = 0
        def mock_socket_recv(num_bytes):
            if not self.conn._mock_sent_size:
                assert num_bytes == 4
                self.conn._mock_sent_size = True
                return struct.pack('>i', len(self.config['payload']))

            recv_data = struct.pack('>%ds' % num_bytes, self.config['payload'][self.conn._mock_data_sent:self.conn._mock_data_sent+num_bytes])
            self.conn._mock_data_sent += num_bytes
            return recv_data

        with mock.patch.object(self.conn, '_read_bytes', new=mock_socket_recv):
            self.assertEquals(self.conn.recv(self.config['request_id']), self.config['payload'])

    def test_recv__reconnects_on_dirty_conn(self):

        # Dirty the connection
        try:
            self.conn._raise_connection_error()
        except ConnectionError:
            pass
        assert self.conn._dirty is True

        # Now test that recv'ing attempts to reconnect
        self.assertEqual(socket.create_connection.call_count, 0)
        self.conn._sock.recv.return_value = self.config['payload']
        self.conn._read_bytes(len(self.config['payload']))
        self.assertEqual(socket.create_connection.call_count, 1)

        # A second way to dirty it...
        self.conn.close()

        # Reset the socket call counts
        socket.create_connection.reset_mock()
        self.assertEqual(socket.create_connection.call_count, 0)

        # Now test that recv'ing attempts to reconnect
        self.conn._read_bytes(len(self.config['payload']))
        self.assertEqual(socket.create_connection.call_count, 1)

    def test_recv__failure_sets_dirty_connection(self):

        def raise_error(*args):
            raise socket.error

        # test that recv'ing attempts to reconnect
        assert self.conn._dirty is False
        assert isinstance(self.conn._sock, mock.Mock)
        self.conn._sock.recv.side_effect=raise_error
        try:
            self.conn.recv(self.config['request_id'])
        except ConnectionError:
            self.assertEquals(self.conn._dirty, True)

    def test_recv__doesnt_consume_extra_data_in_stream(self):
        data1 = self.config['payload']
        size1 = len(data1)
        encoded1 = struct.pack('>i%ds' % size1, size1, data1)
        data2 = "an extra payload"
        size2 = len(data2)
        encoded2 = struct.pack('>i%ds' % size2, size2, data2)

        self.conn._recv_buffer  = encoded1
        self.conn._recv_buffer += encoded2

        def mock_socket_recv(num_bytes):
            data = self.conn._recv_buffer[0:num_bytes]
            self.conn._recv_buffer = self.conn._recv_buffer[num_bytes:]
            return data

        with mock.patch.object(self.conn._sock, 'recv', new=mock_socket_recv):
            self.assertEquals(self.conn.recv(self.config['request_id']), self.config['payload'])
            self.assertEquals(str(self.conn._recv_buffer), encoded2)

    def test_close__object_is_reusable(self):

        # test that sending to a closed connection
        # will re-connect and send data to the socket
        self.conn.close()
        self.conn.send(self.config['request_id'], self.config['payload'])
        self.assertEqual(socket.create_connection.call_count, 1)
        self.conn._sock.sendall.assert_called_with(self.config['payload'])
