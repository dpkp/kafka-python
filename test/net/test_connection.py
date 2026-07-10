import socket
import struct
import time
from unittest.mock import MagicMock, patch

import pytest

from kafka.future import Future
from kafka.net.selector import NetworkSelector
from kafka.net.connection import KafkaConnection
from kafka.net.transport import KafkaTCPTransport
from kafka.protocol.broker_version_data import BrokerVersionData
from kafka.protocol.metadata import ApiVersionsRequest
from kafka.protocol.parser import KafkaProtocol
import kafka.errors as Errors


@pytest.fixture
def net():
    return NetworkSelector()


@pytest.fixture
def connection(net):
    return KafkaConnection(net, node_id='test-0')


@pytest.fixture
def socketpair():
    rsock, wsock = socket.socketpair()
    rsock.setblocking(False)
    wsock.setblocking(False)
    yield rsock, wsock
    for sock in (rsock, wsock):
        try:
            sock.close()
        except OSError:
            pass


class TestKafkaConnectionInit:
    def test_default_state(self, connection):
        assert connection.node_id == 'test-0'
        assert connection.transport is None
        assert connection.connected is False
        assert connection.initializing is True
        assert connection.parser is None
        assert not connection.init_future.is_done
        assert not connection.close_future.is_done
        assert len(connection.in_flight_requests) == 0
        assert len(connection._request_buffer) == 0
        assert connection.broker_version is None

    def test_config_override(self, net):
        proto = KafkaConnection(net, node_id='n1', request_timeout_ms=5000)
        assert proto.config['request_timeout_ms'] == 5000

    def test_str_initializing(self, connection):
        s = str(connection)
        assert 'initializing' in s
        assert 'test-0' in s


class TestKafkaConnectionCheckApiVersions:
    """Test ApiVersionsRequest version negotiation in _get_api_versions."""

    def _make_conn(self, net, **kwargs):
        conn = KafkaConnection(net, node_id='test-0', **kwargs)
        transport = MagicMock()
        transport.getPeer.return_value = ('127.0.0.1', 9092)
        conn.transport = transport
        conn.initializing = True
        return conn

    def _make_api_versions_response(self, error_code=0, api_keys=None, broker_version=None):
        response = MagicMock()
        response.error_code = error_code
        response.API_KEY = ApiVersionsRequest.API_KEY
        if broker_version:
            response.api_keys = [MagicMock(api_key=key, min_version=val[0], max_version=val[1])
                                 for key, val in BrokerVersionData(broker_version).api_versions.items()]
        else:
            response.api_keys = api_keys or []
        return response

    def _run_initialize(self, net, conn, responses):
        """Run initialize() with mocked _send_request returning given responses.
        Returns list of (request_version,) for each call."""
        requests_sent = []
        response_iter = iter(responses)

        original_send = conn._send_request
        def mock_send_request(request, **kwargs):
            requests_sent.append(request.API_VERSION)
            # Match production _send_request: a loop-awaitable SelectorFuture,
            # not a plain (non-awaitable) Future -- the conn awaits this.
            f = net.create_future()
            try:
                resp = next(response_iter)
            except StopIteration:
                f.failure(Errors.KafkaConnectionError('no more responses'))
                return f
            if isinstance(resp, Exception):
                f.failure(resp)
            else:
                f.success(resp)
            return f

        conn._send_request = mock_send_request
        net.run(conn.initialize())
        return requests_sent

    def test_first_request_is_max_version(self, net):
        conn = self._make_conn(net)
        response = self._make_api_versions_response(error_code=0, broker_version=(1, 0))
        versions = self._run_initialize(net, conn, [response])
        assert versions[0] == ApiVersionsRequest.max_version
        assert conn.broker_version == (1, 0)

    def test_unsupported_version_with_api_keys_uses_response_version(self, net):
        conn = self._make_conn(net)
        # First response: UnsupportedVersionError with api_keys indicating max_version=2
        api_key_entry = MagicMock(
            api_key=ApiVersionsRequest.API_KEY,
            min_version=0,
            max_version=2,
        )
        unsupported = self._make_api_versions_response(
            error_code=35, api_keys=[api_key_entry])  # 35 = UnsupportedVersionError
        # Second response: success at version 2
        versions = self._run_initialize(net, conn, [unsupported])
        assert versions[0] == ApiVersionsRequest.max_version
        assert versions[1] == 2

    def test_unsupported_version_no_api_keys_falls_to_zero(self, net):
        conn = self._make_conn(net)
        # First response: UnsupportedVersionError with no matching api_key
        unsupported = self._make_api_versions_response(error_code=35, api_keys=[])
        # Second response: success at version 0
        versions = self._run_initialize(net, conn, [unsupported])
        assert versions[0] == ApiVersionsRequest.max_version
        assert versions[1] == 0

    def test_preconfigured_broker_version_data(self, net):
        # Pre-configure with 1.0, which supports ApiVersions 0+1
        conn = self._make_conn(net)
        conn.broker_version_data = BrokerVersionData((1, 0))
        versions = self._run_initialize(net, conn, [])
        assert versions[0] == 1

    def test_preconfigured_version_without_api_versions_skips(self, net):
        # Pre-configure with old version that doesn't support ApiVersions
        conn = self._make_conn(net)
        conn.broker_version_data = BrokerVersionData((0, 9))
        # Should not send any request -- just call _init_complete
        versions = self._run_initialize(net, conn, [])
        assert versions == []
        assert conn.connected
        assert conn.init_future.succeeded()

    def test_request_failure_closes_connection(self, net):
        conn = self._make_conn(net)
        versions = self._run_initialize(
            net, conn, [Errors.KafkaConnectionError('disconnected')])
        assert versions == [ApiVersionsRequest.max_version]
        # Connection should be closed
        transport = conn.transport
        if transport:
            transport.abort.assert_called()

    def test_discovered_version_clamped_to_user_supplied(self, net):
        # User pre-configures (1, 0); broker advertises (4, 0). The discovered
        # version must be clamped down to the user's cap so requests don't
        # negotiate higher than what the user explicitly asked for.
        conn = self._make_conn(net)
        conn.broker_version_data = BrokerVersionData((1, 0))
        response = self._make_api_versions_response(broker_version=(4, 0))
        self._run_initialize(net, conn, [response])
        assert conn.broker_version == (1, 0)

    def test_discovered_version_used_when_lower_than_user(self, net):
        # User pre-configures (4, 0); broker advertises (1, 0). The broker is
        # actually older, so we downgrade to the discovered version -- the
        # client cannot speak APIs the broker does not support.
        conn = self._make_conn(net)
        conn.broker_version_data = BrokerVersionData((4, 0))
        response = self._make_api_versions_response(broker_version=(1, 0))
        self._run_initialize(net, conn, [response])
        assert conn.broker_version == (1, 0)

    def test_discovered_version_equal_to_user_preserved(self, net):
        # User pre-configures (2, 0); broker advertises (2, 0). Either choice
        # is correct, but the implementation keeps the user-supplied object.
        conn = self._make_conn(net)
        preconfigured = BrokerVersionData((2, 0))
        conn.broker_version_data = preconfigured
        response = self._make_api_versions_response(broker_version=(2, 0))
        self._run_initialize(net, conn, [response])
        assert conn.broker_version == (2, 0)
        assert conn.broker_version_data is preconfigured

    def test_discovered_version_used_when_no_user_config(self, net):
        # No pre-configured version: whatever the broker advertises wins.
        conn = self._make_conn(net)
        assert conn.broker_version_data is None
        response = self._make_api_versions_response(broker_version=(3, 5))
        self._run_initialize(net, conn, [response])
        assert conn.broker_version == (3, 5)


class TestKafkaConnectionPause:
    def test_pause_unpause(self, connection):
        assert not connection.paused
        connection.pause('test')
        assert 'test' in connection.paused
        connection.unpause('test')
        assert not connection.paused

    def test_multiple_pauses(self, connection):
        connection.pause('a')
        connection.pause('b')
        connection.unpause('a')
        assert 'b' in connection.paused
        assert 'a' not in connection.paused

    def test_unpause_nonexistent(self, connection):
        connection.unpause('nonexistent')
        assert not connection.paused

    def test_pause_writing(self, connection):
        connection.pause_writing()
        assert 'buffer' in connection.paused

    def test_resume_writing(self, connection):
        connection.pause('buffer')
        transport = MagicMock()
        connection.transport = transport
        connection.resume_writing()
        assert 'buffer' not in connection.paused


class TestKafkaConnectionSendRequest:
    def test_send_request_buffers_during_init(self, connection):
        request = MagicMock()
        future = connection.send_request(request)
        assert not future.is_done
        assert len(connection._request_buffer) == 1
        assert connection._request_buffer[0][0] is request

    def test_send_request_fails_when_disconnected(self, connection):
        connection.initializing = False
        connection.connected = False
        request = MagicMock()
        future = connection.send_request(request)
        assert future.failed()
        assert isinstance(future.exception, Errors.KafkaConnectionError)

    def test_send_request_multiple_buffered(self, connection):
        for i in range(3):
            connection.send_request(MagicMock())
        assert len(connection._request_buffer) == 3

    def _make_send_ready(self, connection):
        connection.initializing = False
        connection.connected = True
        transport = MagicMock()
        transport.getPeer.return_value = ('127.0.0.1', 9092, 0, 0)
        connection.transport = transport
        connection.parser = MagicMock()
        cid = [0]
        def alloc_cid(_req):
            cid[0] += 1
            return cid[0]
        connection.parser.send_request.side_effect = alloc_cid
        connection.parser.send_bytes.return_value = b'data'

    def test_send_request_heterogeneous_timeouts_no_longer_raise(self, connection):
        """Per-request timers: a longer-timeout request followed by a
        shorter-timeout one must not raise ValueError. Each request owns
        its own timer.
        """
        self._make_send_ready(connection)
        long_req = MagicMock()
        long_req.expect_response.return_value = True
        long_req.API_VERSION = 1
        short_req = MagicMock()
        short_req.expect_response.return_value = True
        short_req.API_VERSION = 1

        # JoinGroup-style: 305s timeout
        connection.send_request(long_req, request_timeout_ms=305000)
        # Metadata-style: 30s timeout - would have raised under the old
        # monotonic-deadline constraint.
        connection.send_request(short_req, request_timeout_ms=30000)

        assert len(connection.in_flight_requests) == 2

    def test_send_request_uses_per_request_timeout(self, connection):
        self._make_send_ready(connection)
        request = MagicMock()
        request.expect_response.return_value = True
        request.API_VERSION = 1
        now = time.monotonic()
        connection.send_request(request, request_timeout_ms=305000)
        (_, _, sent_time, timeout_at, _task) = connection.in_flight_requests[0]
        assert (timeout_at - sent_time) == pytest.approx(305.0, abs=0.05)


class TestKafkaConnectionConnectionLifecycle:
    def test_connection_made(self, connection):
        transport = MagicMock()
        transport.get_protocol.return_value = None
        transport.getPeer.return_value = ('127.0.0.1', 9092)
        connection.connection_made(transport)
        assert connection.transport is transport
        assert connection.initializing is True
        transport.set_protocol.assert_called_once_with(connection)
        transport.resume_reading.assert_called_once()

    def test_connection_lost_fails_init_future(self, connection):
        error = Errors.KafkaConnectionError('test')
        connection.connection_lost(error)
        assert connection.connected is False
        assert connection.initializing is False
        assert connection.transport is None
        assert connection.init_future.failed()
        assert connection.init_future.exception is error

    def test_connection_lost_none_succeeds_close_future(self, connection):
        connection._init_future.success(True)
        connection.connection_lost(None)
        assert connection.close_future.succeeded()

    def test_connection_lost_error_fails_close_future(self, connection):
        connection._init_future.success(True)
        error = Errors.KafkaConnectionError('gone')
        connection.connection_lost(error)
        assert connection.close_future.failed()
        assert connection.close_future.exception is error

    def test_init_complete(self, connection):
        transport = MagicMock()
        connection.transport = transport
        connection.initializing = True
        connection._init_complete()
        assert connection.connected is True
        assert connection.initializing is False
        assert connection.init_future.succeeded()

    def test_init_complete_drains_buffer(self, net):
        proto = KafkaConnection(net, node_id='test')
        transport = MagicMock()
        transport.getPeer.return_value = ('127.0.0.1', 9092, 0, 0)
        proto.transport = transport
        request = MagicMock()
        request.API_VERSION = 1
        request.expect_response.return_value = True
        proto.parser = MagicMock()
        proto.parser.send_request.return_value = 1
        proto.parser.send_bytes.return_value = b'data'
        proto.send_request(request)
        assert len(proto._request_buffer) == 1

        proto._init_complete()
        assert len(proto._request_buffer) == 0
        assert len(proto.in_flight_requests) == 1


class TestKafkaConnectionClose:
    def test_close_with_transport(self, connection):
        transport = MagicMock()
        connection.transport = transport
        connection._init_future.success(True)
        connection.close()
        transport.close.assert_called_once()

    def test_close_with_error_aborts(self, connection):
        transport = MagicMock()
        connection.transport = transport
        error = Errors.KafkaConnectionError('fail')
        connection.close(error)
        transport.abort.assert_called_once_with(error)

    def test_close_without_transport_calls_connection_lost(self, connection):
        assert connection.transport is None
        error = Errors.KafkaConnectionError('fail')
        connection.close(error)
        assert connection.init_future.failed()
        assert connection.connected is False

    def test_close_without_transport_no_error_creates_one(self, connection):
        connection.close()
        assert connection.init_future.failed()
        assert isinstance(connection.init_future.exception, Errors.KafkaConnectionError)


class TestKafkaConnectionFailInFlight:
    def test_fail_buffered_requests(self, connection):
        futures = []
        for _ in range(3):
            f = connection.send_request(MagicMock())
            futures.append(f)

        # fail_in_flight_requests is only valid after the connection has
        # transitioned to closed; close() drives the full path.
        connection.close(Errors.KafkaConnectionError('down'))
        for f in futures:
            assert f.failed()
        assert len(connection._request_buffer) == 0

    def test_fail_in_flight_requests(self, connection):
        f1 = Future()
        f2 = Future()
        now = time.monotonic()
        t1 = connection.net.call_at(now + 30, lambda: None)
        t2 = connection.net.call_at(now + 30, lambda: None)
        connection.in_flight_requests.append((1, f1, now, now + 30, t1))
        connection.in_flight_requests.append((2, f2, now, now + 30, t2))

        connection.close(Errors.Cancelled())
        assert f1.failed()
        assert f2.failed()
        assert len(connection.in_flight_requests) == 0


class TestKafkaConnectionTimeout:
    def test_timeout_at_default(self, connection):
        now = time.monotonic()
        timeout_at = connection._timeout_at(now=now)
        expected = now + connection.config['request_timeout_ms'] / 1000
        assert abs(timeout_at - expected) < 0.001

    def test_timeout_at_custom(self, connection):
        now = time.monotonic()
        timeout_at = connection._timeout_at(now=now, timeout_ms=5000)
        expected = now + 5.0
        assert abs(timeout_at - expected) < 0.001


class TestKafkaConnectionSasl:
    def test_sasl_enabled(self, net):
        conn = KafkaConnection(net, node_id='test', security_protocol='SASL_PLAINTEXT')
        assert conn.sasl_enabled

    def test_sasl_not_enabled_plaintext(self, connection):
        assert not connection.sasl_enabled

    def test_sasl_not_enabled_ssl(self, net):
        conn = KafkaConnection(net, node_id='test', security_protocol='SSL')
        assert not conn.sasl_enabled

    def test_sasl_enabled_sasl_ssl(self, net):
        conn = KafkaConnection(net, node_id='test', security_protocol='SASL_SSL')
        assert conn.sasl_enabled

    def test_sasl_authenticate_handshake_error(self, net):
        conn = KafkaConnection(net, node_id='test',
                               security_protocol='SASL_PLAINTEXT',
                               sasl_mechanism='PLAIN')
        transport = MagicMock()
        transport.getPeer.return_value = ('127.0.0.1', 9092)
        conn.transport = transport
        conn.initializing = True

        from kafka.protocol.broker_version_data import BrokerVersionData
        from kafka.protocol.sasl import SaslHandshakeRequest
        api_versions = {SaslHandshakeRequest[0].API_KEY: (0, 1)}
        conn.broker_version_data = BrokerVersionData(api_versions=api_versions)

        handshake_response = MagicMock()
        handshake_response.error_code = 33  # UnsupportedSaslMechanismError
        handshake_response.mechanisms = ['GSSAPI']

        f = Future()
        f.success(handshake_response)
        conn._send_request = MagicMock(return_value=f)

        net.run(conn.initialize())
        transport.abort.assert_called_once()

    def test_sasl_authenticate_mechanism_not_supported(self, net):
        conn = KafkaConnection(net, node_id='test',
                               security_protocol='SASL_PLAINTEXT',
                               sasl_mechanism='PLAIN')
        transport = MagicMock()
        transport.getPeer.return_value = ('127.0.0.1', 9092)
        conn.transport = transport
        conn.initializing = True

        from kafka.protocol.broker_version_data import BrokerVersionData
        from kafka.protocol.sasl import SaslHandshakeRequest
        api_versions = {SaslHandshakeRequest[0].API_KEY: (0, 1)}
        conn.broker_version_data = BrokerVersionData(api_versions=api_versions)

        handshake_response = MagicMock()
        handshake_response.error_code = 0
        handshake_response.mechanisms = ['GSSAPI', 'SCRAM-SHA-256']

        f = Future()
        f.success(handshake_response)
        conn._send_request = MagicMock(return_value=f)

        net.run(conn.initialize())
        transport.abort.assert_called_once()

    def test_sasl_authenticate_success(self, net):
        conn = KafkaConnection(net, node_id='test',
                               security_protocol='SASL_PLAINTEXT',
                               sasl_mechanism='PLAIN',
                               sasl_plain_username='user',
                               sasl_plain_password='pass')
        transport = MagicMock()
        transport.getPeer.return_value = ('127.0.0.1', 9092)
        conn.transport = transport
        conn.initializing = True

        from kafka.protocol.broker_version_data import BrokerVersionData
        from kafka.protocol.sasl import SaslHandshakeRequest
        api_versions = {SaslHandshakeRequest[0].API_KEY: (0, 1)}
        conn.broker_version_data = BrokerVersionData(api_versions=api_versions)

        # Handshake response
        handshake_response = MagicMock()
        handshake_response.error_code = 0
        handshake_response.mechanisms = ['PLAIN']

        # Auth response
        auth_response = MagicMock()
        auth_response.error_code = 0
        auth_response.auth_bytes = b''
        # KIP-368: explicit 0 so _schedule_reauthenticate() short-circuits
        auth_response.session_lifetime_ms = 0

        responses = iter([handshake_response, auth_response])
        def mock_send_request(request, **kwargs):
            f = conn.net.create_future()  # loop-awaitable, like production
            f.success(next(responses))
            return f
        conn._send_request = mock_send_request

        net.run(conn.initialize())
        # Should not have closed -- auth succeeded
        assert conn.connected
        assert conn.init_future.succeeded()

    def test_sasl_authenticate_auth_failure(self, net):
        conn = KafkaConnection(net, node_id='test',
                               security_protocol='SASL_PLAINTEXT',
                               sasl_mechanism='PLAIN',
                               sasl_plain_username='user',
                               sasl_plain_password='wrong')
        transport = MagicMock()
        transport.getPeer.return_value = ('127.0.0.1', 9092)
        conn.transport = transport
        conn.initializing = True

        from kafka.protocol.broker_version_data import BrokerVersionData
        from kafka.protocol.sasl import SaslHandshakeRequest
        api_versions = {SaslHandshakeRequest[0].API_KEY: (0, 1)}
        conn.broker_version_data = BrokerVersionData(api_versions=api_versions)

        # Handshake succeeds
        handshake_response = MagicMock()
        handshake_response.error_code = 0
        handshake_response.mechanisms = ['PLAIN']

        # Auth fails
        auth_response = MagicMock()
        auth_response.error_code = 58  # SaslAuthenticationFailedError
        auth_response.error_message = 'Authentication failed'

        responses = iter([handshake_response, auth_response])
        def mock_send_request(request, **kwargs):
            f = conn.net.create_future()  # loop-awaitable, like production
            f.success(next(responses))
            return f
        conn._send_request = mock_send_request

        net.run(conn.initialize())
        transport.abort.assert_called_once()

    def _drive_handshake_with_recording_mechanism(self, net, conn):
        from kafka.protocol.sasl import SaslHandshakeRequest
        api_versions = {SaslHandshakeRequest[0].API_KEY: (0, 1)}
        conn.broker_version_data = BrokerVersionData(api_versions=api_versions)
        handshake_response = MagicMock()
        handshake_response.error_code = 0
        handshake_response.mechanisms = ['PLAIN']
        auth_response = MagicMock()
        auth_response.error_code = 0
        auth_response.auth_bytes = b''
        # KIP-368: explicit 0 so _schedule_reauthenticate() short-circuits
        auth_response.session_lifetime_ms = 0
        responses = iter([handshake_response, auth_response])
        def mock_send_request(request, **kwargs):
            f = conn.net.create_future()  # loop-awaitable, like production
            f.success(next(responses))
            return f
        conn._send_request = mock_send_request

        captured = {}
        from kafka.net.sasl import register_sasl_mechanism
        from kafka.net.sasl.plain import SaslMechanismPlain

        class RecordingPlain(SaslMechanismPlain):
            def __init__(self, **config):
                captured['host'] = config.get('host')
                super().__init__(**config)
        register_sasl_mechanism('PLAIN', RecordingPlain, overwrite=True)
        try:
            net.run(conn.initialize())
        finally:
            register_sasl_mechanism('PLAIN', SaslMechanismPlain, overwrite=True)
        return captured

    def test_sasl_uses_transport_host_for_mechanism(self, net):
        conn = KafkaConnection(
            net, node_id='test',
            security_protocol='SASL_PLAINTEXT', sasl_mechanism='PLAIN',
            sasl_plain_username='user', sasl_plain_password='pass')
        transport = MagicMock()
        transport.host = 'kafka.example.com'
        transport.getPeer.return_value = ('10.0.0.1', 9092)
        conn.transport = transport
        conn.initializing = True

        captured = self._drive_handshake_with_recording_mechanism(net, conn)
        assert captured['host'] == 'kafka.example.com'

    def test_sasl_falls_back_to_peer_ip_when_transport_host_unset(self, net):
        conn = KafkaConnection(
            net, node_id='test',
            security_protocol='SASL_PLAINTEXT', sasl_mechanism='PLAIN',
            sasl_plain_username='user', sasl_plain_password='pass')
        transport = MagicMock()
        transport.host = None
        transport.getPeer.return_value = ('10.0.0.1', 9092)
        conn.transport = transport
        conn.initializing = True

        captured = self._drive_handshake_with_recording_mechanism(net, conn)
        assert captured['host'] == '10.0.0.1'


class TestKafkaConnectionInvalidReceive:
    """An over-large/negative frame size makes the parser raise
    InvalidReceiveError from inside data_received. On a connected
    connection that must tear the connection down (the transport catches
    the KafkaProtocolError and aborts), rather than leaving a wedged,
    half-read connection in place."""

    def _make_connected(self, net, transport, max_frame_bytes=1000):
        conn = KafkaConnection(net, node_id='test-0')
        conn.transport = transport
        conn.connected = True
        conn.initializing = False
        conn._init_future.success(True)
        conn.parser = KafkaProtocol(
            client_id='test', receive_message_max_bytes=max_frame_bytes)
        transport.set_protocol(conn)
        return conn

    def test_oversized_frame_closes_connection(self, net, socketpair):
        rsock, wsock = socketpair
        transport = KafkaTCPTransport(net, wsock)
        conn = self._make_connected(net, transport, max_frame_bytes=1000)
        assert not conn.closed

        transport.resume_reading()
        # 4-byte length prefix declaring a 2000-byte frame, over the 1000
        # byte limit -> parser raises InvalidReceiveError on the header.
        rsock.send(struct.pack('>i', 2000))
        net.poll(timeout_ms=1000, future=conn.close_future)

        assert conn.closed
        assert conn.close_future.failed()
        assert isinstance(conn.close_future.exception, Errors.InvalidReceiveError)

    def test_negative_frame_closes_connection(self, net, socketpair):
        rsock, wsock = socketpair
        transport = KafkaTCPTransport(net, wsock)
        conn = self._make_connected(net, transport)
        assert not conn.closed

        transport.resume_reading()
        rsock.send(struct.pack('>i', -1))
        net.poll(timeout_ms=1000, future=conn.close_future)

        assert conn.closed
        assert conn.close_future.failed()
        assert isinstance(conn.close_future.exception, Errors.InvalidReceiveError)
