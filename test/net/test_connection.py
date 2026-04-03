import time
from unittest.mock import MagicMock, patch

import pytest

from kafka.future import Future
from kafka.net.selector import NetworkSelector
from kafka.net.connection import KafkaConnection
import kafka.errors as Errors


@pytest.fixture
def net():
    return NetworkSelector()


@pytest.fixture
def connection(net):
    return KafkaConnection(net, node_id='test-0')


class TestKafkaConnectionInit:
    def test_default_state(self, connection):
        assert connection.node_id == 'test-0'
        assert connection.transport is None
        assert connection.connected is False
        assert connection.initializing is True
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


class TestKafkaConnectionConnectionLifecycle:
    def test_connection_made(self, connection):
        transport = MagicMock()
        transport.get_protocol.return_value = None
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

        connection.fail_in_flight_requests(Errors.KafkaConnectionError('down'))
        for f in futures:
            assert f.failed()
        assert len(connection._request_buffer) == 0

    def test_fail_in_flight_requests(self, connection):
        f1 = Future()
        f2 = Future()
        connection.in_flight_requests.append((1, f1, time.monotonic(), time.monotonic() + 30))
        connection.in_flight_requests.append((2, f2, time.monotonic(), time.monotonic() + 30))

        connection.fail_in_flight_requests(Errors.Cancelled())
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

        net.run_until_done(conn._sasl_authenticate())
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

        net.run_until_done(conn._sasl_authenticate())
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

        responses = iter([handshake_response, auth_response])
        def mock_send_request(request):
            f = Future()
            f.success(next(responses))
            return f
        conn._send_request = mock_send_request

        net.run_until_done(conn._sasl_authenticate())
        # Should not have closed -- auth succeeded
        assert conn.initializing  # still initializing, _init_complete not called by _sasl_authenticate

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
        def mock_send_request(request):
            f = Future()
            f.success(next(responses))
            return f
        conn._send_request = mock_send_request

        net.run_until_done(conn._sasl_authenticate())
        transport.abort.assert_called_once()
