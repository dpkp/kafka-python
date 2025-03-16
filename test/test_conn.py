# pylint: skip-file
from __future__ import absolute_import

from errno import EALREADY, EINPROGRESS, EISCONN, ECONNRESET
import socket

try:
    from unittest import mock
except ImportError:
    import mock
import pytest

from kafka.conn import BrokerConnection, ConnectionStates, collect_hosts
from kafka.protocol.api import RequestHeader
from kafka.protocol.group import HeartbeatResponse
from kafka.protocol.metadata import MetadataRequest
from kafka.protocol.produce import ProduceRequest

import kafka.errors as Errors

from kafka.vendor import six

if six.PY2:
    ConnectionError = socket.error
    TimeoutError = socket.error
    BlockingIOError = Exception


@pytest.fixture
def dns_lookup(mocker):
    return mocker.patch('kafka.conn.dns_lookup',
                        return_value=[(socket.AF_INET,
                                       None, None, None,
                                       ('localhost', 9092))])

@pytest.fixture
def _socket(mocker):
    socket = mocker.MagicMock()
    socket.connect_ex.return_value = 0
    socket.send.side_effect = lambda d: len(d)
    socket.recv.side_effect = BlockingIOError("mocked recv")
    mocker.patch('socket.socket', return_value=socket)
    return socket


@pytest.fixture
def conn(_socket, dns_lookup, mocker):
    conn = BrokerConnection('localhost', 9092, socket.AF_INET)
    mocker.patch.object(conn, '_try_api_versions_check', return_value=True)
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


def test_api_versions_check(_socket):
    conn = BrokerConnection('localhost', 9092, socket.AF_INET)
    assert conn._api_versions_future is None
    conn.connect()
    assert conn._api_versions_future is not None
    assert conn.connecting() is True
    assert conn.state is ConnectionStates.API_VERSIONS_RECV

    assert conn._try_api_versions_check() is False
    assert conn.connecting() is True
    assert conn.state is ConnectionStates.API_VERSIONS_RECV

    conn._api_versions_future = None
    conn._check_version_idx = 0
    assert conn._try_api_versions_check() is False
    assert conn.connecting() is True

    conn._check_version_idx = len(conn.VERSION_CHECKS)
    conn._api_versions_future = None
    assert conn._try_api_versions_check() is False
    assert conn.connecting() is False
    assert conn.disconnected() is True


def test_api_versions_check_unrecognized(_socket):
    conn = BrokerConnection('localhost', 9092, socket.AF_INET, api_version=(0, 0))
    with pytest.raises(Errors.UnrecognizedBrokerVersion):
        conn.connect()


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
    with mock.patch("time.time", return_value=1000):
        conn.last_attempt = 0
        assert conn.blacked_out() is False
        conn.last_attempt = 1000
        assert conn.blacked_out() is True


def test_connection_delay(conn, mocker):
    mocker.patch.object(conn, '_reconnect_jitter_pct', return_value=1.0)
    with mock.patch("time.time", return_value=1000):
        conn.last_attempt = 1000
        assert conn.connection_delay() == conn.config['reconnect_backoff_ms']
        conn.state = ConnectionStates.CONNECTING
        assert conn.connection_delay() == conn.config['reconnect_backoff_ms']
        conn.state = ConnectionStates.CONNECTED
        assert conn.connection_delay() == float('inf')

        del conn._gai[:]
        conn._update_reconnect_backoff()
        conn.state = ConnectionStates.DISCONNECTED
        assert conn.connection_delay() == 1.0 * conn.config['reconnect_backoff_ms']
        conn.state = ConnectionStates.CONNECTING
        assert conn.connection_delay() == 1.0 * conn.config['reconnect_backoff_ms']

        conn._update_reconnect_backoff()
        conn.state = ConnectionStates.DISCONNECTED
        assert conn.connection_delay() == 2.0 * conn.config['reconnect_backoff_ms']
        conn.state = ConnectionStates.CONNECTING
        assert conn.connection_delay() == 2.0 * conn.config['reconnect_backoff_ms']

        conn._update_reconnect_backoff()
        conn.state = ConnectionStates.DISCONNECTED
        assert conn.connection_delay() == 4.0 * conn.config['reconnect_backoff_ms']
        conn.state = ConnectionStates.CONNECTING
        assert conn.connection_delay() == 4.0 * conn.config['reconnect_backoff_ms']


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
    assert isinstance(f.exception, Errors.KafkaConnectionError)


def test_send_connecting(conn):
    conn.state = ConnectionStates.CONNECTING
    f = conn.send('foobar')
    assert f.failed() is True
    assert isinstance(f.exception, Errors.NodeNotReadyError)


def test_send_max_ifr(conn):
    conn.state = ConnectionStates.CONNECTED
    max_ifrs = conn.config['max_in_flight_requests_per_connection']
    for i in range(max_ifrs):
        conn.in_flight_requests[i] = 'foo'
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
    assert isinstance(f.exception, Errors.KafkaConnectionError)
    assert _socket.close.call_count == 1
    assert conn.state is ConnectionStates.DISCONNECTED


def test_can_send_more(conn):
    assert conn.can_send_more() is True
    max_ifrs = conn.config['max_in_flight_requests_per_connection']
    for i in range(max_ifrs):
        assert conn.can_send_more() is True
        conn.in_flight_requests[i] = 'foo'
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
    _socket.recv.side_effect = None
    _socket.recv.return_value = b''

    # Attempt to receive should mark connection as disconnected
    assert conn.connected(), 'Not connected: %s' % conn.state
    conn.recv()
    assert conn.disconnected(), 'Not disconnected: %s' % conn.state


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
        m.assert_called_once_with(hostname, port, 0, socket.SOCK_STREAM)
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
        m.assert_called_once_with(hostname, port, 0, socket.SOCK_STREAM)
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
        m.assert_called_once_with(hostname, port, 0, socket.SOCK_STREAM)
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
        m.assert_called_once_with(hostname, port, 0, socket.SOCK_STREAM)
        assert conn._sock_afi == afi2
        assert conn._sock_addr == sockaddr2
        conn.close()


def test_requests_timed_out(conn):
    with mock.patch("time.time", return_value=0):
        # No in-flight requests, not timed out
        assert not conn.requests_timed_out()

        # Single request, timeout_at > now (0)
        conn.in_flight_requests[0] = ('foo', 0, 1)
        assert not conn.requests_timed_out()

        # Add another request w/ timestamp > request_timeout ago
        request_timeout = conn.config['request_timeout_ms']
        expired_timestamp = 0 - request_timeout - 1
        conn.in_flight_requests[1] = ('bar', 0, expired_timestamp)
        assert conn.requests_timed_out()

        # Drop the expired request and we should be good to go again
        conn.in_flight_requests.pop(1)
        assert not conn.requests_timed_out()


def test_maybe_throttle(conn):
    assert conn.state is ConnectionStates.DISCONNECTED
    assert not conn.throttled()

    conn.state = ConnectionStates.CONNECTED
    assert not conn.throttled()

    # No throttle_time_ms attribute
    conn._maybe_throttle(HeartbeatResponse[0](error_code=0))
    assert not conn.throttled()

    with mock.patch("time.time", return_value=1000) as time:
        # server-side throttling in v1.0
        conn.config['api_version'] = (1, 0)
        conn._maybe_throttle(HeartbeatResponse[1](throttle_time_ms=1000, error_code=0))
        assert not conn.throttled()

        # client-side throttling in v2.0
        conn.config['api_version'] = (2, 0)
        conn._maybe_throttle(HeartbeatResponse[2](throttle_time_ms=1000, error_code=0))
        assert conn.throttled()

        time.return_value = 3000
        assert not conn.throttled()
