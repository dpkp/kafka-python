"""KIP-368: SASL connection re-authentication."""
import time

import pytest

import kafka.errors as Errors
from kafka.net.connection import KafkaConnection
from kafka.net.manager import KafkaConnectionManager
from kafka.net.selector import NetworkSelector
from kafka.protocol.sasl import (
    SaslAuthenticateRequest,
    SaslAuthenticateResponse,
    SaslHandshakeRequest,
    SaslHandshakeResponse,
)

from test.mock_broker import MockBroker


SASL_CONFIG = {
    'security_protocol': 'SASL_PLAINTEXT',
    'sasl_mechanism': 'PLAIN',
    'sasl_plain_username': 'user',
    'sasl_plain_password': 'pass',
}


@pytest.fixture
def net():
    sel = NetworkSelector()
    try:
        yield sel
    finally:
        sel.close()


@pytest.fixture
def sasl_broker():
    return MockBroker(broker_version=(2, 5))  # supports SaslAuthenticate v0-2


@pytest.fixture
def sasl_manager(net, sasl_broker):
    manager = KafkaConnectionManager(
        net,
        bootstrap_servers='%s:%d' % (sasl_broker.host, sasl_broker.port),
        api_version=sasl_broker.broker_version,
        request_timeout_ms=5000,
        **SASL_CONFIG,
    )
    sasl_broker.attach(manager)
    try:
        yield manager
    finally:
        manager.close()


def _script_sasl(broker, session_lifetime_ms=0, auth_error_code=0):
    """Queue the SaslHandshake + SaslAuthenticate response pair on the mock broker."""
    handshake = SaslHandshakeResponse(error_code=0, mechanisms=['PLAIN'])
    broker.respond(SaslHandshakeRequest, handshake)

    auth = SaslAuthenticateResponse(
        version=1,
        error_code=auth_error_code,
        error_message='' if auth_error_code == 0 else 'failed',
        auth_bytes=b'',
        session_lifetime_ms=session_lifetime_ms,
    )
    broker.respond(SaslAuthenticateRequest, auth)


def _bootstrap_and_open(manager, broker, lifetime_ms=0, auth_error_code=0):
    """Bootstrap the manager (consumes one SASL pair), then open a real
    connection (consumes a second SASL pair scripted with `lifetime_ms`)."""
    _script_sasl(broker, session_lifetime_ms=0)  # bootstrap connection
    _script_sasl(broker, session_lifetime_ms=lifetime_ms, auth_error_code=auth_error_code)
    manager.bootstrap()
    conn = manager.get_connection(broker.node_id)
    # get_connection is non-blocking; spin the loop until init completes.
    deadline = time.monotonic() + 2.0
    while not conn.connected and not conn.closed and time.monotonic() < deadline:
        manager._net.poll(timeout_ms=10)
    return conn


class TestSaslReauthentication:
    def test_no_lifetime_does_not_schedule_reauth(self, sasl_manager, sasl_broker):
        conn = _bootstrap_and_open(sasl_manager, sasl_broker, lifetime_ms=0)

        assert conn.connected
        assert conn._reauth.session_lifetime_ms == 0
        assert conn._reauth.task is None
        assert conn._reauth.authenticated_at is not None

    def test_lifetime_schedules_reauth(self, sasl_manager, sasl_broker, monkeypatch):
        # Pin jitter to lower bound for deterministic delay.
        monkeypatch.setattr('kafka.net.connection.random.uniform', lambda a, b: a)

        conn = _bootstrap_and_open(sasl_manager, sasl_broker, lifetime_ms=10_000)

        assert conn.connected
        assert conn._reauth.session_lifetime_ms == 10_000
        assert conn._reauth.task is not None
        assert conn._reauth.task.scheduled_at is not None
        delay = conn._reauth.task.scheduled_at - time.monotonic()
        assert 8.0 < delay <= 8.6  # 10s * 0.85 jitter

    def test_reauth_fires_and_runs_second_handshake(self, sasl_manager, sasl_broker, monkeypatch):
        monkeypatch.setattr('kafka.net.connection.random.uniform', lambda a, b: a)

        conn = _bootstrap_and_open(sasl_manager, sasl_broker, lifetime_ms=1000)
        # Queue responses for the re-auth that's about to fire.
        _script_sasl(sasl_broker, session_lifetime_ms=0)

        assert conn._reauth.session_lifetime_ms == 1000
        initial_auth_at = conn._reauth.authenticated_at

        # Drive the loop past the jittered delay (200ms * 0.85 = 170ms)
        deadline = time.monotonic() + 1.0
        while conn._reauth.authenticated_at == initial_auth_at and time.monotonic() < deadline:
            sasl_manager._net.poll(timeout_ms=10)

        assert conn.connected
        assert conn._reauth.authenticated_at > initial_auth_at
        assert conn._reauth.session_lifetime_ms == 0
        assert conn._reauth.task is None
        assert not conn._reauth.is_reauthenticating

    def test_reauth_failure_closes_connection(self, sasl_manager, sasl_broker, monkeypatch):
        monkeypatch.setattr('kafka.net.connection.random.uniform', lambda a, b: a)

        conn = _bootstrap_and_open(sasl_manager, sasl_broker, lifetime_ms=200)
        # Re-auth response with SaslAuthenticationFailedError (error code 58)
        _script_sasl(sasl_broker, session_lifetime_ms=0, auth_error_code=58)

        deadline = time.monotonic() + 1.0
        while not conn.closed and time.monotonic() < deadline:
            sasl_manager._net.poll(timeout_ms=10)

        assert conn.closed
        # KIP-368: reauth failure is transient -- must NOT be sticky in the
        # manager's auth-failure cache (initial-auth failures are sticky).
        assert sasl_broker.node_id not in sasl_manager._auth_failures

    def test_close_cancels_scheduled_reauth(self, sasl_manager, sasl_broker, monkeypatch):
        monkeypatch.setattr('kafka.net.connection.random.uniform', lambda a, b: a)

        conn = _bootstrap_and_open(sasl_manager, sasl_broker, lifetime_ms=60_000)
        assert conn._reauth.task is not None

        conn.close()
        # Let close propagate through the loop.
        sasl_manager._net.drain()

        assert conn._reauth.task is None

    def test_negotiates_v2_when_broker_supports(self, sasl_manager, sasl_broker):
        # Bootstrap uses an unobserved SASL pair; on the real connection we
        # capture the SaslAuthenticateRequest's wire version via respond_fn.
        _script_sasl(sasl_broker, session_lifetime_ms=0)  # bootstrap
        sasl_broker.respond(SaslHandshakeRequest,
                            SaslHandshakeResponse(error_code=0, mechanisms=['PLAIN']))
        captured = {}

        def auth_handler(api_key, api_version, correlation_id, request_bytes):
            captured['api_version'] = api_version
            return SaslAuthenticateResponse(
                version=1, error_code=0, error_message='',
                auth_bytes=b'', session_lifetime_ms=0)
        sasl_broker.respond_fn(SaslAuthenticateRequest, auth_handler)

        sasl_manager.bootstrap()
        conn = sasl_manager.get_connection(sasl_broker.node_id)
        deadline = time.monotonic() + 2.0
        while not conn.connected and not conn.closed and time.monotonic() < deadline:
            sasl_manager._net.poll(timeout_ms=10)

        # MockBroker is broker_version=(2,5) -> SaslAuthenticate v0-2
        assert captured.get('api_version') == 2


class TestSaslReauthenticationUnit:
    """Lower-level tests that don't need a MockBroker."""

    def test_session_lifetime_captured_from_v1_response(self, net):
        """getattr falls back to 0 when the response object lacks the field."""
        from unittest.mock import MagicMock
        from kafka.future import Future
        from kafka.protocol.broker_version_data import BrokerVersionData

        conn = KafkaConnection(net, node_id='test', **SASL_CONFIG)
        transport = MagicMock()
        transport.getPeer.return_value = ('127.0.0.1', 9092)
        transport.host = 'broker'
        conn.transport = transport
        conn.initializing = True

        # Broker supports SaslHandshake v0-1 AND SaslAuthenticate v0-1
        conn.broker_version_data = BrokerVersionData(api_versions={
            SaslHandshakeRequest.API_KEY: (0, 1),
            SaslAuthenticateRequest.API_KEY: (0, 1),
        })

        handshake_response = MagicMock()
        handshake_response.error_code = 0
        handshake_response.mechanisms = ['PLAIN']
        handshake_response.API_VERSION = 1

        auth_response = MagicMock()
        auth_response.error_code = 0
        auth_response.auth_bytes = b''
        auth_response.session_lifetime_ms = 5_000

        responses = iter([handshake_response, auth_response])
        def mock_send_request(request, **kwargs):
            f = conn.net.create_future()  # loop-awaitable, like production
            f.success(next(responses))
            return f
        conn._send_request = mock_send_request

        net.run(conn.initialize())

        assert conn.connected
        assert conn._reauth.session_lifetime_ms == 5_000
        assert conn._reauth.task is not None

    def test_schedule_skipped_when_sasl_disabled(self, net):
        conn = KafkaConnection(net, node_id='test', security_protocol='PLAINTEXT')
        conn._reauth.session_lifetime_ms = 30_000  # would normally schedule
        conn._reauth.schedule()
        assert conn._reauth.task is None

    def test_schedule_skipped_when_lifetime_zero(self, net):
        conn = KafkaConnection(net, node_id='test', **SASL_CONFIG)
        conn._reauth.session_lifetime_ms = 0
        conn._reauth.schedule()
        assert conn._reauth.task is None

    def test_send_request_buffers_during_reauth(self, net):
        from kafka.protocol.metadata import MetadataRequest

        conn = KafkaConnection(net, node_id='test', **SASL_CONFIG)
        conn.connected = True
        conn.initializing = False
        conn._reauth._reauthenticating = True

        req = MetadataRequest()
        future = conn.send_request(req)

        assert not future.is_done
        assert len(conn._request_buffer) == 1
