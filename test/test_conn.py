# pylint: skip-file
from __future__ import absolute_import

from errno import EALREADY, EINPROGRESS, EISCONN, ECONNRESET
import socket
import time

import mock
import pytest

from kafka.conn import BrokerConnection, ConnectionStates, collect_hosts
from kafka.protocol.api import RequestHeader
from kafka.protocol.metadata import MetadataRequest
from kafka.protocol.produce import ProduceRequest

import kafka.common as Errors


@pytest.fixture
def _socket(mocker):
    socket = mocker.MagicMock()
    socket.connect_ex.return_value = 0
    mocker.patch('socket.socket', return_value=socket)
    return socket


@pytest.fixture
def conn(_socket):
    conn = BrokerConnection('localhost', 9092, socket.AF_INET)
    return conn


@pytest.mark.parametrize("states", [
    (([EINPROGRESS, EALREADY], ConnectionStates.CONNECTING),),
    (([EALREADY, EALREADY], ConnectionStates.CONNECTING),),
    (([0], ConnectionStates.CONNECTED),),
    (([EINPROGRESS, EALREADY], ConnectionStates.CONNECTING),
     ([ECONNRESET], ConnectionStates.DISCONNECTED)),
    (([EINPROGRESS, EALREADY], ConnectionStates.CONNECTING),
     ([EALREADY], ConnectionStates.CONNECTING),
     ([EISCONN], ConnectionStates.CONNECTED)),
])
def test_connect(_socket, conn, states):
    assert conn.state is ConnectionStates.DISCONNECTED

    for errno, state in states:
        _socket.connect_ex.side_effect = errno
        conn.connect()
        assert conn.state is state


def test_connect_timeout(_socket, conn):
    assert conn.state is ConnectionStates.DISCONNECTED

    # Initial connect returns EINPROGRESS
    # immediate inline connect returns EALREADY
    # second explicit connect returns EALREADY
    # third explicit connect returns EALREADY and times out via last_attempt
    _socket.connect_ex.side_effect = [EINPROGRESS, EALREADY, EALREADY, EALREADY]
    conn.connect()
    assert conn.state is ConnectionStates.CONNECTING
    conn.connect()
    assert conn.state is ConnectionStates.CONNECTING
    conn.last_attempt = 0
    conn.connect()
    assert conn.state is ConnectionStates.DISCONNECTED


def test_blacked_out(conn):
    assert conn.blacked_out() is False
    conn.last_attempt = time.time()
    assert conn.blacked_out() is True


def test_connection_delay(conn):
    conn.last_attempt = time.time()
    assert round(conn.connection_delay()) == round(conn.config['reconnect_backoff_ms'])
    conn.state = ConnectionStates.CONNECTING
    assert conn.connection_delay() == 0
    conn.state = ConnectionStates.CONNECTED
    assert conn.connection_delay() == float('inf')


def test_connected(conn):
    assert conn.connected() is False
    conn.state = ConnectionStates.CONNECTED
    assert conn.connected() is True


def test_connecting(conn):
    assert conn.connecting() is False
    conn.state = ConnectionStates.CONNECTING
    assert conn.connecting() is True
    conn.state = ConnectionStates.CONNECTED
    assert conn.connecting() is False


def test_send_disconnected(conn):
    conn.state = ConnectionStates.DISCONNECTED
    f = conn.send('foobar')
    assert f.failed() is True
    assert isinstance(f.exception, Errors.ConnectionError)


def test_send_connecting(conn):
    conn.state = ConnectionStates.CONNECTING
    f = conn.send('foobar')
    assert f.failed() is True
    assert isinstance(f.exception, Errors.NodeNotReadyError)


def test_send_max_ifr(conn):
    conn.state = ConnectionStates.CONNECTED
    max_ifrs = conn.config['max_in_flight_requests_per_connection']
    for _ in range(max_ifrs):
        conn.in_flight_requests.append('foo')
    f = conn.send('foobar')
    assert f.failed() is True
    assert isinstance(f.exception, Errors.TooManyInFlightRequests)


def test_send_no_response(_socket, conn):
    conn.connect()
    assert conn.state is ConnectionStates.CONNECTED
    req = ProduceRequest[0](required_acks=0, timeout=0, topics=())
    header = RequestHeader(req, client_id=conn.config['client_id'])
    payload_bytes = len(header.encode()) + len(req.encode())
    third = payload_bytes // 3
    remainder = payload_bytes % 3
    _socket.send.side_effect = [4, third, third, third, remainder]

    assert len(conn.in_flight_requests) == 0
    f = conn.send(req)
    assert f.succeeded() is True
    assert f.value is None
    assert len(conn.in_flight_requests) == 0


def test_send_response(_socket, conn):
    conn.connect()
    assert conn.state is ConnectionStates.CONNECTED
    req = MetadataRequest[0]([])
    header = RequestHeader(req, client_id=conn.config['client_id'])
    payload_bytes = len(header.encode()) + len(req.encode())
    third = payload_bytes // 3
    remainder = payload_bytes % 3
    _socket.send.side_effect = [4, third, third, third, remainder]

    assert len(conn.in_flight_requests) == 0
    f = conn.send(req)
    assert f.is_done is False
    assert len(conn.in_flight_requests) == 1


def test_send_error(_socket, conn):
    conn.connect()
    assert conn.state is ConnectionStates.CONNECTED
    req = MetadataRequest[0]([])
    try:
        _socket.send.side_effect = ConnectionError
    except NameError:
        _socket.send.side_effect = socket.error
    f = conn.send(req)
    assert f.failed() is True
    assert isinstance(f.exception, Errors.ConnectionError)
    assert _socket.close.call_count == 1
    assert conn.state is ConnectionStates.DISCONNECTED


def test_can_send_more(conn):
    assert conn.can_send_more() is True
    max_ifrs = conn.config['max_in_flight_requests_per_connection']
    for _ in range(max_ifrs):
        assert conn.can_send_more() is True
        conn.in_flight_requests.append('foo')
    assert conn.can_send_more() is False


def test_recv_disconnected(_socket, conn):
    conn.connect()
    assert conn.connected()

    req = MetadataRequest[0]([])
    header = RequestHeader(req, client_id=conn.config['client_id'])
    payload_bytes = len(header.encode()) + len(req.encode())
    _socket.send.side_effect = [4, payload_bytes]
    conn.send(req)

    # Empty data on recv means the socket is disconnected
    _socket.recv.return_value = b''

    # Attempt to receive should mark connection as disconnected
    assert conn.connected()
    conn.recv()
    assert conn.disconnected()


def test_recv(_socket, conn):
    pass # TODO


def test_close(conn):
    pass # TODO


def test_collect_hosts__happy_path():
    hosts = "127.0.0.1:1234,127.0.0.1"
    results = collect_hosts(hosts)
    assert set(results) == set([
        ('127.0.0.1', 1234, socket.AF_INET),
        ('127.0.0.1', 9092, socket.AF_INET),
    ])


def test_collect_hosts__ipv6():
    hosts = "[localhost]:1234,[2001:1000:2000::1],[2001:1000:2000::1]:1234"
    results = collect_hosts(hosts)
    assert set(results) == set([
        ('localhost', 1234, socket.AF_INET6),
        ('2001:1000:2000::1', 9092, socket.AF_INET6),
        ('2001:1000:2000::1', 1234, socket.AF_INET6),
    ])


def test_collect_hosts__string_list():
    hosts = [
        'localhost:1234',
        'localhost',
        '[localhost]',
        '2001::1',
        '[2001::1]',
        '[2001::1]:1234',
    ]
    results = collect_hosts(hosts)
    assert set(results) == set([
        ('localhost', 1234, socket.AF_UNSPEC),
        ('localhost', 9092, socket.AF_UNSPEC),
        ('localhost', 9092, socket.AF_INET6),
        ('2001::1', 9092, socket.AF_INET6),
        ('2001::1', 9092, socket.AF_INET6),
        ('2001::1', 1234, socket.AF_INET6),
    ])


def test_collect_hosts__with_spaces():
    hosts = "localhost:1234, localhost"
    results = collect_hosts(hosts)
    assert set(results) == set([
        ('localhost', 1234, socket.AF_UNSPEC),
        ('localhost', 9092, socket.AF_UNSPEC),
    ])


def test_lookup_on_connect():
    hostname = 'example.org'
    port = 9092
    conn = BrokerConnection(hostname, port, socket.AF_UNSPEC)
    assert conn.host == hostname
    assert conn.port == port
    assert conn.afi == socket.AF_UNSPEC
    afi1 = socket.AF_INET
    sockaddr1 = ('127.0.0.1', 9092)
    mock_return1 = [
        (afi1, socket.SOCK_STREAM, 6, '', sockaddr1),
    ]
    with mock.patch("socket.getaddrinfo", return_value=mock_return1) as m:
        conn.connect()
        m.assert_called_once_with(hostname, port, 0, 1)
        assert conn._sock_afi == afi1
        assert conn._sock_addr == sockaddr1
        conn.close()

    afi2 = socket.AF_INET6
    sockaddr2 = ('::1', 9092, 0, 0)
    mock_return2 = [
        (afi2, socket.SOCK_STREAM, 6, '', sockaddr2),
    ]

    with mock.patch("socket.getaddrinfo", return_value=mock_return2) as m:
        conn.last_attempt = 0
        conn.connect()
        m.assert_called_once_with(hostname, port, 0, 1)
        assert conn._sock_afi == afi2
        assert conn._sock_addr == sockaddr2
        conn.close()


def test_relookup_on_failure():
    hostname = 'example.org'
    port = 9092
    conn = BrokerConnection(hostname, port, socket.AF_UNSPEC)
    assert conn.host == hostname
    mock_return1 = []
    with mock.patch("socket.getaddrinfo", return_value=mock_return1) as m:
        last_attempt = conn.last_attempt
        conn.connect()
        m.assert_called_once_with(hostname, port, 0, 1)
        assert conn.disconnected()
        assert conn.last_attempt > last_attempt

    afi2 = socket.AF_INET
    sockaddr2 = ('127.0.0.2', 9092)
    mock_return2 = [
        (afi2, socket.SOCK_STREAM, 6, '', sockaddr2),
    ]

    with mock.patch("socket.getaddrinfo", return_value=mock_return2) as m:
        conn.last_attempt = 0
        conn.connect()
        m.assert_called_once_with(hostname, port, 0, 1)
        assert conn._sock_afi == afi2
        assert conn._sock_addr == sockaddr2
        conn.close()
