import collections
import copy
import logging
import struct
import time

import kafka.errors as Errors
from kafka.future import Future
from kafka.net.metrics import KafkaConnectionMetrics
from kafka.protocol.metadata import ApiVersionsRequest
from kafka.protocol.sasl import SaslAuthenticateRequest, SaslHandshakeRequest, SaslBytesRequest
from kafka.protocol.broker_version_data import BrokerVersionData
from kafka.protocol.parser import KafkaProtocol
from kafka.sasl import get_sasl_mechanism
from kafka.version import __version__


log = logging.getLogger(__name__)


class KafkaConnection:
    DEFAULT_CONFIG = {
        'client_id': 'kafka-python-' + __version__,
        'client_software_name': 'kafka-python',
        'client_software_version': __version__,
        'request_timeout_ms': 30000,
        'max_in_flight_requests_per_connection': 5,
        'security_protocol': 'PLAINTEXT',
        'sasl_mechanism': None,
        'sasl_plain_username': None,
        'sasl_plain_password': None,
        'sasl_kerberos_name': None,
        'sasl_kerberos_service_name': 'kafka',
        'sasl_kerberos_domain_name': None,
        'sasl_oauth_token_provider': None,
        'api_version_auto_timeout_ms': 2000,
        'metrics': None,
        'metric_group_prefix': '',
    }

    def __init__(self, net, node_id=None, broker_version_data=None, **configs):
        self.config = copy.copy(self.DEFAULT_CONFIG)
        for key in self.config:
            if key in configs:
                self.config[key] = configs[key]

        self.node_id = node_id
        self.net = net
        self.transport = None
        self.parser = None
        self._request_buffer = collections.deque()
        self.paused = set()
        self.connected = False
        self.initializing = True
        self._init_future = Future()
        self._close_future = Future()
        self.in_flight_requests = collections.deque()
        self.broker_version_data = broker_version_data
        self._api_versions_idx = ApiVersionsRequest.max_version # version of ApiVersionsRequest to try on first connect
        self._throttle_time = 0
        if self.config['metrics']:
            self._sensors = KafkaConnectionMetrics(
                self.config['metrics'], self.config['metric_group_prefix'], node_id)
        else:
            self._sensors = None
        self._init_future.add_errback(self.fail_in_flight_requests)
        self._close_future.add_both(self.fail_in_flight_requests)

    @property
    def broker_version(self):
        if self.broker_version_data is None:
            return None
        return self.broker_version_data.broker_version

    def __str__(self):
        if self.initializing:
            state = 'initializing'
        elif not self.connected:
            state = 'disconnected'
        elif self.paused:
            state = 'paused'
        else:
            state = 'connected'
        host_port = ' host=[%s]' % self.transport.host_port() if self.transport else ''
        broker_version = self.broker_version if self.broker_version is not None else 'unknown'
        return f'<KafkaConnection node_id={self.node_id}{host_port} broker_version={broker_version} ({state})>'

    @property
    def init_future(self):
        return self._init_future

    @property
    def close_future(self):
        return self._close_future

    def _timeout_at(self, now=None, timeout_ms=None):
        if now is None:
            now = time.monotonic()
        if timeout_ms is not None:
            return now + timeout_ms / 1000
        else:
            try:
                return now + self._timeout_secs
            except AttributeError:
                self._timeout_secs = self.config['request_timeout_ms'] / 1000
                return now + self._timeout_secs

    def send_request(self, request, request_timeout_ms=None):
        future = Future()
        timeout_at = self._timeout_at(timeout_ms=request_timeout_ms)
        if self.initializing:
            self._request_buffer.append((request, future, timeout_at))
            return future
        elif self.paused:
            return future.failure(Errors.NodeNotReadyError('Node paused: %s') % self.paused)
        elif not self.connected:
            return future.failure(Errors.KafkaConnectionError('Node not connected'))
        else:
            self._send_request(request, future=future, timeout_at=timeout_at)
            return future

    def _send_request(self, request, future=None, timeout_at=None):
        if future is None:
            future = Future()
        if request.API_VERSION is None:
            try:
                request.API_VERSION = self.broker_version_data.api_version(request)
            except Errors.IncompatibleBrokerVersion as exc:
                future.failure(exc)
                return future
        sent_time = time.monotonic()
        if timeout_at is None:
            timeout_at = self._timeout_at(now=sent_time)
        if timeout_at <= sent_time:
            future.failure(Errors.KafkaTimeoutError())
            return future
        correlation_id = self.parser.send_request(request)
        log.debug('%s Request %d: %s', self, correlation_id, request)
        if request.expect_response():
            if not self.in_flight_requests:
                self._timeout_task = self.net.call_at(timeout_at, self._maybe_timeout_ifrs)
            elif timeout_at < self.in_flight_requests[-1][-1]:
                raise ValueError('New request timeout cannot be lower than in-flight request!')
            self.in_flight_requests.append((correlation_id, future, sent_time, timeout_at))
        else:
            future.success(None)

        # Write the current request's bytes before checking max_in_flight.
        # Otherwise with max_in_flight=1, the first request would be added to
        # in_flight_requests (len==1), trip the >= check, pause, and never be
        # written to the transport — hanging forever.
        if not self.paused:
            self.transport.write(self.parser.send_bytes())
        if len(self.in_flight_requests) >= self.config['max_in_flight_requests_per_connection']:
            self.pause('max_in_flight')
        return future

    def send_buffered(self):
        while self._request_buffer:
            request, future, timeout_at = self._request_buffer.popleft()
            self._send_request(request, future=future, timeout_at=timeout_at)

    def _maybe_timeout_ifrs(self):
        if not self.in_flight_requests:
            return
        _, _, sent_at, timeout_at = self.in_flight_requests[0]
        if time.monotonic() > timeout_at:
            timeout_ms = (timeout_at - sent_at) * 1000
            log.warning('%s: Request timed out after %d ms. Closing connection.', self, timeout_ms)
            self.close(Errors.RequestTimedOutError('Request timed out after %d ms' % timeout_ms))

    def data_received(self, data):
        """ Called when some data is received."""
        responses = self.parser.receive_bytes(data)

        # augment responses w/ correlation_id, future, and timestamp
        for i, (resp_correlation_id, response) in enumerate(responses):
            try:
                (req_correlation_id, future, sent_time, _timeout_at) = self.in_flight_requests.popleft()
            except IndexError:
                return self.close(Errors.KafkaConnectionError('Received response with no in-flight-requests!'))

            if req_correlation_id != resp_correlation_id:
                return self.close(Errors.KafkaConnectionError('Received unrecognized correlation id'))

            latency_ms = (time.monotonic() - sent_time) * 1000
            if self._sensors:
                self._sensors.request_time.record(latency_ms)

            log.debug('%s: Response %d (%s ms): %s', self, resp_correlation_id, latency_ms, response)
            self._maybe_throttle(response)
            future.success(response)
        if 'max_in_flight' in self.paused and len(self.in_flight_requests) < self.config['max_in_flight_requests_per_connection']:
            self.unpause('max_in_flight')
        if self.in_flight_requests:
            next_timeout_at = self.in_flight_requests[0][3]
            self.net.reschedule(self._timeout_task, next_timeout_at)
        else:
            self.net.unschedule(self._timeout_task)
            self._timeout_task = None

    def eof_received(self):
        """ Called when the other end calls write_eof() or equivalent.

        If this returns a false value (including None), the transport
        will close itself.  If it returns a true value, closing the
        transport is up to the protocol.
        """
        return False

    def connection_lost(self, exc):
        """ Called when the connection is lost or closed.

        The argument is an exception object or None (the latter
        meaning a regular EOF is received or the connection was
        aborted or closed).
        """
        self.connected = self.initializing = False
        self.transport = None
        error = exc or Errors.KafkaConnectionError()
        if not self._init_future.is_done:
            self._init_future.failure(error)
        if not self._close_future.is_done:
            if exc is None:
                self._close_future.success(None)
            else:
                self._close_future.failure(exc)

    def fail_in_flight_requests(self, error):
        error = error or Errors.Cancelled()
        for _, future, _ in self._request_buffer:
            future.failure(error)
        self._request_buffer.clear()
        for _, future, _, _ in self.in_flight_requests:
            future.failure(error)
        self.in_flight_requests.clear()

    def connection_made(self, transport):
        """ Called when a connection is made.

        The argument is the transport representing the pipe connection.
        To receive data, wait for data_received() calls.
        When the connection is closed, connection_lost() is called.
        """
        self.transport = transport
        if self.transport.get_protocol() != self:
            self.transport.set_protocol(self)
        self.initializing = True
        self.transport.resume_reading()
        self.parser = KafkaProtocol(client_id=self.config['client_id'])
        self.net.call_soon(self._check_version)

    def pause(self, v):
        self.paused.add(v)

    def unpause(self, v):
        try:
            self.paused.remove(v)
        except KeyError:
            pass
        else:
            if not self.paused and self.parser and self.transport:
                to_send = self.parser.send_bytes()
                if to_send:
                    self.transport.write(to_send)

    def pause_writing(self):
        """ Called when the transport's buffer goes over the high-water mark.

        Pause and resume calls are paired -- pause_writing() is called
        once when the buffer goes strictly over the high-water mark
        (even if subsequent writes increases the buffer size even
        more), and eventually resume_writing() is called once when the
        buffer size reaches the low-water mark.

        Note that if the buffer size equals the high-water mark,
        pause_writing() is not called -- it must go strictly over.
        Conversely, resume_writing() is called when the buffer size is
        equal or lower than the low-water mark.  These end conditions
        are important to ensure that things go as expected when either
        mark is zero.

        NOTE: This is the only Protocol callback that is not called
        through EventLoop.call_soon() -- if it were, it would have no
        effect when it's most needed (when the app keeps writing
        without yielding until pause_writing() is called).
        """
        self.pause('buffer')

    def resume_writing(self):
        """ Called when the transport's buffer drains below the low-water mark."""
        self.unpause('buffer')

    def close(self, error=None):
        if error is None and not self._init_future.is_done:
            error = Errors.KafkaConnectionError()
        if not self.transport:
            self.connection_lost(error)
            return
        if error:
            self.transport.abort(error)
        else:
            self.transport.close()

    def _maybe_throttle(self, response):
        throttle_time_ms = getattr(response, 'throttle_time_ms', 0)
        if self._sensors:
            self._sensors.throttle_time.record(throttle_time_ms)
        if not throttle_time_ms:
            return
        # Client side throttling enabled in v2.0 brokers
        # prior to that throttling (if present) was managed broker-side
        if self.broker_version is not None and self.broker_version >= (2, 0):
            throttle_time = time.monotonic() + throttle_time_ms / 1000
            if throttle_time > self._throttle_time:
                self._throttle_time = throttle_time
                self.net.call_at(throttle_time, self._maybe_unthrottle)
                self.pause('throttle')
        log.warning("%s: %s throttled by broker (%d ms)", self,
                    response.__class__.__name__, throttle_time_ms)

    def _maybe_unthrottle(self):
        if time.monotonic() >= self._throttle_time:
            self._throttle_time = 0
            self.unpause('throttle')

    async def _check_version(self, timeout_ms=None):
        if self.broker_version_data is not None:
            try:
                self._api_versions_idx = self.broker_version_data.api_version(ApiVersionsRequest)
            except Errors.IncompatibleBrokerVersion:
                log.debug('%s: Using pre-configured api_version %s for ApiVersions', self, self.broker_version)
                self._init_complete()
                return

        if timeout_ms is not None:
            timeout_ms = self.config['api_version_auto_timeout_ms']
        timeout_at = self._timeout_at(timeout_ms=timeout_ms)
        while timeout_at > time.monotonic():
            version = self._api_versions_idx
            request = ApiVersionsRequest(
                version=version,
                client_software_name=self.config['client_software_name'],
                client_software_version=self.config['client_software_version'],
            )
            try:
                response = await self._send_request(request, timeout_at=timeout_at)
            except Exception as exc:
                self.close(exc)
                return

            error_type = Errors.for_code(response.error_code)
            if error_type is Errors.NoError:
                break
            elif error_type is Errors.UnsupportedVersionError:
                for api_version in response.api_keys:
                    if api_version.api_key == response.API_KEY:
                        self._api_versions_idx = min(self._api_versions_idx, api_version.max_version)
                        break
                else:
                    self._api_versions_idx = 0
                continue
            else:
                self.close(error_type())
                return

        api_versions = {api_version.api_key: (api_version.min_version, api_version.max_version)
                        for api_version in response.api_keys}
        self.broker_version_data = BrokerVersionData(api_versions=api_versions)
        log.info('%s: Broker version identified as %s', self, '.'.join(map(str, self.broker_version)))
        if self.sasl_enabled:
            await self._sasl_authenticate()
        if self.initializing:
            self._init_complete()

    @property
    def sasl_enabled(self):
        return self.config['security_protocol'] in ('SASL_PLAINTEXT', 'SASL_SSL')

    async def _sasl_authenticate(self):
        # Step 1: SaslHandshake to negotiate mechanism
        version = self.broker_version_data.api_version(SaslHandshakeRequest, max_version=1)
        request = SaslHandshakeRequest[version](self.config['sasl_mechanism'])
        try:
            response = await self._send_request(request)
        except Exception as exc:
            self.close(Errors.KafkaConnectionError('SaslHandshake failed: %s' % exc))
            return

        error_type = Errors.for_code(response.error_code)
        if error_type is not Errors.NoError:
            self.close(error_type())
            return

        if self.config['sasl_mechanism'] not in response.mechanisms:
            self.close(Errors.UnsupportedSaslMechanismError(
                'Kafka broker does not support %s sasl mechanism. Enabled mechanisms: %s'
                % (self.config['sasl_mechanism'], response.mechanisms)))
            return

        # Step 2: SASL authentication exchange
        mechanism = get_sasl_mechanism(self.config['sasl_mechanism'])(
            host=self.transport.getPeer()[0], **self.config)

        while not mechanism.is_done():
            token = mechanism.auth_bytes()
            if version == 1:
                auth_request = SaslAuthenticateRequest[0](token)
            else:
                auth_request = SaslBytesRequest(token)

            try:
                auth_response = await self._send_request(auth_request)
            except Exception as exc:
                self.close(Errors.KafkaConnectionError('SaslAuthenticate failed: %s' % exc))
                return

            error_type = Errors.for_code(auth_response.error_code)
            if error_type is not Errors.NoError:
                self.close(Errors.SaslAuthenticationFailedError(
                    '%s: %s' % (error_type.__name__, auth_response.error_message)))
                return

            # GSSAPI does not get a final recv in v0 unframed mode
            if version == 0 and mechanism.is_done():
                break

            mechanism.receive(auth_response.auth_bytes)

        if not mechanism.is_authenticated():
            self.close(Errors.SaslAuthenticationFailedError(
                'Failed to authenticate via SASL %s' % self.config['sasl_mechanism']))
            return

        log.info('%s: %s', self, mechanism.auth_details())

    def _init_complete(self):
        self.initializing = False
        self.connected = True
        self.send_buffered()
        self._init_future.success(True)
