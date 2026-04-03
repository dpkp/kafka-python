# pylint: skip-file

from errno import EALREADY, EINPROGRESS, EISCONN, ECONNRESET
import socket
from unittest import mock

import pytest

from kafka.conn import BrokerConnection, ConnectionStates
from kafka.future import Future
from kafka.conn import BrokerConnection, ConnectionStates, SSLWantWriteError, VERSION_CHECKS
from kafka.metrics.metrics import Metrics
from kafka.metrics.stats.sensor import Sensor
from kafka.protocol.broker_version_data import BrokerVersionData, VERSION_CHECKS
from kafka.protocol.admin import ListGroupsResponse
from kafka.protocol.consumer import HeartbeatResponse
from kafka.protocol.metadata import MetadataRequest, ApiVersionsRequest, ApiVersionsResponse
from kafka.protocol.producer import ProduceRequest
from kafka.version import __version__

import kafka.errors as Errors


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
def metrics(mocker):
    metrics = mocker.MagicMock(Metrics)
    metrics.mocked_sensors = {}
    def sensor(name, **kwargs):
        if name not in metrics.mocked_sensors:
            metrics.mocked_sensors[name] = mocker.MagicMock(Sensor)
        return metrics.mocked_sensors[name]
    metrics.sensor.side_effect = sensor
    return metrics

@pytest.fixture
def conn(_socket, dns_lookup, metrics, mocker):
    conn = BrokerConnection('localhost', 9092, socket.AF_INET, metrics=metrics)
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


def test_api_versions_check(_socket, mocker):
    conn = BrokerConnection('localhost', 9092, socket.AF_INET)
    mocker.patch.object(conn, 'send_pending_requests')
    mocker.patch.object(conn, 'connection_delay', return_value=0)
    mocker.spy(conn, '_send')
    assert conn._api_versions_future is None
    conn.connect()
    assert conn._api_versions_future is not None
    assert conn.connecting() is True
    assert conn.state is ConnectionStates.API_VERSIONS_RECV
    conn._send.assert_called_with(
        ApiVersionsRequest(version=ApiVersionsRequest.max_version, client_software_name='kafka-python', client_software_version=__version__),
        blocking=True,
        request_timeout_ms=1000.0
    )
    send_call_count = conn._send.call_count

    assert conn._try_api_versions_check() is False
    assert conn._send.call_count == send_call_count
    assert conn.connecting() is True
    assert conn.state is ConnectionStates.API_VERSIONS_RECV

    api_versions_response = ApiVersionsResponse(
        version=ApiVersionsRequest.max_version,
        error_code=0,
        api_keys=[(MetadataRequest.API_KEY, 0, 5)]
    )
    conn.recv(responses=[(1, api_versions_response)], resolve_futures=True)
    assert conn.broker_version_data.broker_version == (1, 0)
    assert conn.broker_version_data.api_versions == {MetadataRequest.API_KEY: (0, 5)}
    assert conn.state is ConnectionStates.CONNECTED


def test_api_versions_unsupported_versions(_socket, mocker):
    conn = BrokerConnection('localhost', 9092, socket.AF_INET)
    mocker.patch.object(conn, 'send_pending_requests')
    mocker.patch.object(conn, 'connection_delay', return_value=0)
    mocker.spy(conn, '_send')
    assert conn._api_versions_future is None
    conn.connect()
    assert conn._api_versions_future is not None
    assert conn.connecting() is True
    assert conn.state is ConnectionStates.API_VERSIONS_RECV
    conn._send.assert_called_with(
        ApiVersionsRequest(version=ApiVersionsRequest.max_version, client_software_name='kafka-python', client_software_version=__version__),
        blocking=True,
        request_timeout_ms=1000.0
    )
    send_call_count = conn._send.call_count

    assert conn._try_api_versions_check() is False
    assert conn._send.call_count == send_call_count
    assert conn.connecting() is True
    assert conn.state is ConnectionStates.API_VERSIONS_RECV

    # If we sent a higher-than-supported version, the broker sends back the min/max
    # We should skip to the max supported for our next try.
    api_versions_error = ApiVersionsResponse(
        version=0,
        error_code=Errors.UnsupportedVersionError.errno,
        api_keys=[(ApiVersionsRequest.API_KEY, 0, 1)]
    )
    conn.recv(responses=[(1, api_versions_error)], resolve_futures=True)
    assert conn._api_versions_idx == 1
    assert conn._api_versions_future is None
    assert conn.state is ConnectionStates.API_VERSIONS_SEND

    assert conn._try_api_versions_check() is False
    assert conn.connecting() is True
    assert conn.state is ConnectionStates.API_VERSIONS_RECV
    conn._send.assert_called_with(
        ApiVersionsRequest(version=1, client_software_name='kafka-python', client_software_version=__version__),
        blocking=True,
        request_timeout_ms=500.0
    )

    api_versions_response = ApiVersionsResponse(
        version=1, error_code=0,
        api_keys=[(ApiVersionsRequest.API_KEY, 0, 1)]
    )
    conn.recv(responses=[(2, api_versions_response)], resolve_futures=True)
    assert conn.broker_version_data.broker_version == (0, 10, 0)
    assert conn.broker_version_data.api_versions == {ApiVersionsRequest.API_KEY: (0, 1)}
    assert conn.state is ConnectionStates.CONNECTED

    conn.close()
    assert conn.state is ConnectionStates.DISCONNECTED

    # Reconnect uses the last api versions check
    conn.connect()
    assert conn.state is ConnectionStates.API_VERSIONS_RECV
    conn._send.assert_called_with(
        ApiVersionsRequest(version=1, client_software_name='kafka-python', client_software_version=__version__),
        blocking=True,
        request_timeout_ms=1000.0
    )


def test_api_versions_fallback(_socket, mocker):
    conn = BrokerConnection('localhost', 9092, socket.AF_INET)
    mocker.patch.object(conn, 'send_pending_requests')
    mocker.patch.object(conn, 'connection_delay', return_value=0)
    mocker.spy(conn, '_send')
    assert conn._api_versions_future is None
    conn.connect()
    assert conn._api_versions_future is not None
    assert conn.connecting() is True
    assert conn.state is ConnectionStates.API_VERSIONS_RECV
    conn._send.assert_called_with(
        ApiVersionsRequest(version=ApiVersionsRequest.max_version, client_software_name='kafka-python', client_software_version=__version__),
        blocking=True,
        request_timeout_ms=1000.0
    )
    send_call_count = conn._send.call_count

    assert conn._try_api_versions_check() is False
    assert conn._send.call_count == send_call_count
    assert conn.connecting() is True
    assert conn.state is ConnectionStates.API_VERSIONS_RECV

    future = conn._api_versions_future
    assert not future.is_done
    conn.close(Errors.RequestTimedOutError())
    assert future.failed()
    assert future.exception == Errors.RequestTimedOutError()
    assert conn._api_versions_idx == 0
    assert conn._api_versions_future is None
    assert conn.state is ConnectionStates.DISCONNECTED

    conn.connect()
    assert conn.connecting() is True
    assert conn.state is ConnectionStates.API_VERSIONS_RECV
    conn._send.assert_called_with(
        ApiVersionsRequest(version=0, client_software_name='kafka-python', client_software_version=__version__),
        blocking=True,
        request_timeout_ms=500.0
    )

    conn.close(Errors.RequestTimedOutError())
    assert conn._api_versions_idx == None
    assert conn._api_versions_future is None
    assert conn._check_version_idx == 0
    assert conn.state is ConnectionStates.DISCONNECTED

    conn.connect()
    assert conn.connecting() is True
    assert conn.state is ConnectionStates.API_VERSIONS_RECV
    conn._send.assert_called_with(
        VERSION_CHECKS[0][1],
        blocking=True,
        request_timeout_ms=250.0
    )

    conn.recv(responses=[(1, ListGroupsResponse())], resolve_futures=True)

    assert conn.broker_version_data == BrokerVersionData((0, 9))
    assert conn.state is ConnectionStates.CONNECTED

    conn.close()
    assert conn.state is ConnectionStates.DISCONNECTED

    # Reconnect skips check versions
    conn.connect()
    assert conn.state is ConnectionStates.CONNECTED


def test_api_versions_check_with_broker_version_data(_socket, mocker):
    conn = BrokerConnection('localhost', 9092, socket.AF_INET, broker_version_data=BrokerVersionData((1, 0)))
    mocker.patch.object(conn, 'send_pending_requests')
    mocker.patch.object(conn, 'connection_delay', return_value=0)
    mocker.spy(conn, '_send')
    assert conn._api_versions_future is None
    conn.connect()
    assert conn._api_versions_future is not None
    assert conn.connecting() is True
    assert conn.state is ConnectionStates.API_VERSIONS_RECV
    conn._send.assert_called_with(
        ApiVersionsRequest(version=1, client_software_name='kafka-python', client_software_version=__version__),
        blocking=True,
        request_timeout_ms=1000.0
    )
    send_call_count = conn._send.call_count

    assert conn._try_api_versions_check() is False
    assert conn._send.call_count == send_call_count
    assert conn.connecting() is True
    assert conn.state is ConnectionStates.API_VERSIONS_RECV

    api_versions_error = ApiVersionsResponse(
        version=0,
        error_code=Errors.UnsupportedVersionError.errno,
        api_keys=[(ApiVersionsRequest.API_KEY, 0, 1)]
    )
    conn.recv(responses=[(1, api_versions_error)], resolve_futures=True)
    assert conn._api_versions_idx == 0
    assert conn._api_versions_future is None
    assert conn.state is ConnectionStates.API_VERSIONS_SEND

    assert conn._try_api_versions_check() is False
    assert conn.connecting() is True
    assert conn.state is ConnectionStates.API_VERSIONS_RECV
    conn._send.assert_called_with(
        ApiVersionsRequest(version=0, client_software_name='kafka-python', client_software_version=__version__),
        blocking=True,
        request_timeout_ms=500.0
    )

    future = conn._api_versions_future
    assert not future.is_done
    conn.close(Errors.RequestTimedOutError())
    assert future.failed()
    assert future.exception == Errors.RequestTimedOutError()
    assert conn._api_versions_idx == 0
    assert conn._api_versions_future is None
    assert conn.state is ConnectionStates.DISCONNECTED

    conn.connect()
    assert conn.connecting() is True
    assert conn.state is ConnectionStates.API_VERSIONS_RECV
    conn._send.assert_called_with(
        ApiVersionsRequest(version=0, client_software_name='kafka-python', client_software_version=__version__),
        blocking=True,
        request_timeout_ms=250.0
    )

    conn.close(Errors.RequestTimedOutError())
    assert conn._api_versions_idx == 0
    assert conn._api_versions_future is None
    assert conn._check_version_idx is None
    assert conn.state is ConnectionStates.DISCONNECTED

    conn.connect()
    assert conn.connecting() is True
    assert conn.state is ConnectionStates.API_VERSIONS_RECV
    conn._send.assert_called_with(
        ApiVersionsRequest(version=0, client_software_name='kafka-python', client_software_version=__version__),
        blocking=True,
        request_timeout_ms=125.0
    )


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
    with mock.patch("time.monotonic", return_value=1000):
        conn.last_attempt = 0
        assert conn.blacked_out() is False
        conn.last_attempt = 1000
        assert conn.blacked_out() is True


def test_connection_delay(conn, mocker):
    mocker.patch.object(conn, '_reconnect_jitter_pct', return_value=1.0)
    with mock.patch("time.monotonic", return_value=1000):
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
    req = ProduceRequest[0](acks=0, timeout_ms=0, topic_data=())
    req.with_header(correlation_id=0, client_id=conn.config['client_id'])
    payload_bytes = len(req.encode(header=True, framed=False))
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
    req.with_header(correlation_id=0, client_id=conn.config['client_id'])
    payload_bytes = len(req.encode(header=True, framed=False))
    third = payload_bytes // 3
    remainder = payload_bytes % 3
    _socket.send.side_effect = [4, third, third, third, remainder]

    assert len(conn.in_flight_requests) == 0
    f = conn.send(req)
    assert f.is_done is False
    assert len(conn.in_flight_requests) == 1


def test_send_async_request_while_other_request_is_already_in_buffer(_socket, conn, metrics):
    conn.connect()
    assert conn.state is ConnectionStates.CONNECTED
    assert 'node-0.bytes-sent' in metrics.mocked_sensors
    bytes_sent_sensor = metrics.mocked_sensors['node-0.bytes-sent']

    req1 = MetadataRequest[0](topics='foo')
    req1.with_header(correlation_id=0, client_id=conn.config['client_id'])
    payload_bytes1 = len(req1.encode(header=True, framed=False))
    req2 = MetadataRequest[0]([])
    req2.with_header(correlation_id=0, client_id=conn.config['client_id'])
    payload_bytes2 = len(req2.encode(header=True, framed=False))

    # The first call to the socket will raise a transient SSL exception. This will make the first
    # request to be kept in the internal buffer to be sent in the next call of
    # send_pending_requests_v2.
    _socket.send.side_effect = [SSLWantWriteError, 4 + payload_bytes1, 4 + payload_bytes2]

    conn.send(req1, blocking=False)
    # This won't send any bytes because of the SSL exception and the request bytes will be kept in
    # the buffer.
    assert conn.send_pending_requests_v2() is False
    assert bytes_sent_sensor.record.call_args_list[0].args == (0,)

    conn.send(req2, blocking=False)
    # This will send the remaining bytes in the buffer from the first request, but should notice
    # that the second request was queued, therefore it should return False.
    bytes_sent_sensor.record.reset_mock()
    assert conn.send_pending_requests_v2() is False
    bytes_sent_sensor.record.assert_called_once_with(4 + payload_bytes1)

    bytes_sent_sensor.record.reset_mock()
    assert conn.send_pending_requests_v2() is True
    bytes_sent_sensor.record.assert_called_once_with(4 + payload_bytes2)

    bytes_sent_sensor.record.reset_mock()
    assert conn.send_pending_requests_v2() is True
    bytes_sent_sensor.record.assert_called_once_with(0)


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
    req.with_header(correlation_id=0, client_id=conn.config['client_id'])
    payload_bytes = len(req.encode(header=True, framed=False))
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
    with mock.patch("time.monotonic", return_value=0):
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

    with mock.patch("time.monotonic", return_value=1000) as time:
        # server-side throttling in v1.0
        conn.broker_version_data = BrokerVersionData((1, 0))
        conn._maybe_throttle(HeartbeatResponse[1](throttle_time_ms=1000, error_code=0))
        assert not conn.throttled()

        # client-side throttling in v2.0
        conn.broker_version_data = BrokerVersionData((2, 0))
        conn._maybe_throttle(HeartbeatResponse[2](throttle_time_ms=1000, error_code=0))
        assert conn.throttled()

        time.return_value = 3000
        assert not conn.throttled()


def test_host_in_sasl_config():
    hostname = 'example.org'
    port = 9092
    for security_protocol in ('SASL_PLAINTEXT', 'SASL_SSL'):
        with mock.patch("kafka.conn.get_sasl_mechanism") as get_sasl_mechanism:
            BrokerConnection(hostname, port, socket.AF_UNSPEC, security_protocol=security_protocol)
            call_config = get_sasl_mechanism.mock_calls[1].kwargs
            assert call_config['host'] == hostname
