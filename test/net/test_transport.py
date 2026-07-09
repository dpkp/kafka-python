import socket
import time
from unittest.mock import MagicMock

import pytest

import kafka.errors as Errors
from kafka.future import Future
from kafka.net.selector import NetworkSelector, TaskState
from kafka.net.transport import KafkaSSLTransport, KafkaTCPTransport


@pytest.fixture
def socketpair():
    rsock, wsock = socket.socketpair()
    rsock.setblocking(False)
    wsock.setblocking(False)
    yield rsock, wsock
    try:
        rsock.close()
    except OSError:
        pass
    try:
        wsock.close()
    except OSError:
        pass


@pytest.fixture
def net():
    return NetworkSelector()


def _make_mock_sock():
    sock = MagicMock()
    sock.getpeername.return_value = ('127.0.0.1', 9092)
    sock.getsockname.return_value = ('127.0.0.1', 12345)
    sock.fileno.return_value = 99
    return sock


class TestKafkaTCPTransport:
    def test_init(self, net):
        sock = _make_mock_sock()
        t = KafkaTCPTransport(net, sock)
        assert not t.is_closing()
        assert not t.is_reading()
        assert t.get_protocol() is None

    def test_set_get_protocol(self, net):
        sock = _make_mock_sock()
        t = KafkaTCPTransport(net, sock)
        proto = MagicMock()
        t.set_protocol(proto)
        assert t.get_protocol() is proto

    def test_last_activity(self, net):
        sock = _make_mock_sock()
        t = KafkaTCPTransport(net, sock)
        now = time.monotonic()
        assert t.last_activity >= now - 1
        assert t.last_activity == max(t.last_read, t.last_write)

    def test_pause_resume_reading(self, net):
        sock = _make_mock_sock()
        t = KafkaTCPTransport(net, sock)
        assert not t.is_reading()
        t.resume_reading()
        assert t.is_reading()
        t.pause_reading()
        assert not t.is_reading()

    def test_write_buffers_data(self, net):
        sock = _make_mock_sock()
        t = KafkaTCPTransport(net, sock)
        t.write(b'hello')
        assert len(t._write_buffer) == 1
        assert t._write_buffer[0] == b'hello'

    def test_write_schedules_send(self, net):
        sock = _make_mock_sock()
        t = KafkaTCPTransport(net, sock)
        initial_ready = len(net._ready)
        t.write(b'hello')
        assert len(net._ready) == initial_ready + 1

    def test_write_second_does_not_reschedule(self, net):
        sock = _make_mock_sock()
        t = KafkaTCPTransport(net, sock)
        t.write(b'hello')
        ready_after_first = len(net._ready)
        t.write(b'world')
        assert len(net._ready) == ready_after_first
        assert len(t._write_buffer) == 2

    def test_write_empty_raises(self, net):
        sock = _make_mock_sock()
        t = KafkaTCPTransport(net, sock)
        with pytest.raises(ValueError):
            t.write(b'')

    def test_write_after_close_raises(self, net):
        sock = _make_mock_sock()
        t = KafkaTCPTransport(net, sock)
        t._closed = True
        with pytest.raises(RuntimeError):
            t.write(b'hello')

    def test_writelines(self, net):
        sock = _make_mock_sock()
        t = KafkaTCPTransport(net, sock)
        t.writelines([b'hello', b'world'])
        assert len(t._write_buffer) == 2

    def test_close_marks_closed(self, net):
        sock = _make_mock_sock()
        t = KafkaTCPTransport(net, sock)
        t.close()
        assert t.is_closing()
        assert not t.is_reading()

    def test_abort_clears_buffer(self, net):
        sock = _make_mock_sock()
        proto = MagicMock()
        t = KafkaTCPTransport(net, sock)
        t.set_protocol(proto)
        t.write(b'data')
        t.abort(error=Exception('test'))
        assert t.is_closing()
        assert len(t._write_buffer) == 0
        proto.connection_lost.assert_called_once()

    def test_abort_idempotent(self, net):
        sock = _make_mock_sock()
        proto = MagicMock()
        t = KafkaTCPTransport(net, sock)
        t.set_protocol(proto)
        t.abort()
        t.abort()
        proto.connection_lost.assert_called_once()

    def test_sock_send_error_closes_transport(self, net, socketpair):
        """If _sock_send returns an error, _write_to_sock must close the
        transport and propagate the error via protocol.connection_lost.

        Regression for an earlier bug where the err return value from
        _sock_send was discarded and the loop kept retrying on a broken
        socket forever.
        """
        _, wsock = socketpair
        t = KafkaTCPTransport(net, wsock)
        proto = MagicMock()
        done = Future()
        proto.connection_lost.side_effect = lambda err: done.success(err)
        t.set_protocol(proto)

        err = Errors.KafkaConnectionError('write failed')

        # Stub _sock_send: drain one chunk (real _sock_send drops the chunk
        # that errored - no appendleft on BaseException) and return the
        # error so the while-loop's buffer check terminates.
        def fake_sock_send():
            if t._write_buffer:
                t._write_buffer.popleft()
            return 0, err
        t._sock_send = fake_sock_send

        t.write(b'data')
        net.poll(timeout_ms=1000, future=done)

        assert done.is_done
        assert not t._writing
        assert t._sock is None
        proto.connection_lost.assert_called_once_with(err)

    def test_sock_send_when_sock_closed_returns_clean_error(self, net):
        """If the socket was closed (set to None by _close/abort) while data is
        still buffered, _sock_send must short-circuit with a clean
        KafkaConnectionError('Connection closed during send') rather than
        dereferencing None -> AttributeError.
        """
        sock = _make_mock_sock()
        t = KafkaTCPTransport(net, sock)
        t.write(b'data')  # leave bytes buffered
        t._sock = None    # socket torn down out from under the pending send

        total_bytes, err = t._sock_send()

        assert total_bytes == 0
        assert isinstance(err, Errors.KafkaConnectionError)
        # Exactly the clean message -- not a wrapped AttributeError.
        assert err.args == ('Connection closed during send',)
        assert not isinstance(err.args[0], BaseException)
        # The buffered chunk must not be consumed by the short-circuit.
        assert list(t._write_buffer) == [b'data']
        sock.send.assert_not_called()

    def test_sock_recv_error_closes_transport(self, net, socketpair):
        """If _sock_recv returns an error, _read_from_sock must close the
        transport and propagate the error via protocol.connection_lost.
        """
        rsock, wsock = socketpair
        t = KafkaTCPTransport(net, wsock)
        proto = MagicMock()
        done = Future()
        proto.connection_lost.side_effect = lambda err: done.success(err)
        t.set_protocol(proto)

        err = Errors.KafkaConnectionError('read failed')

        def fake_sock_recv():
            return 0, err
        t._sock_recv = fake_sock_recv

        t.resume_reading()
        # _read_from_sock's loop awaits wait_read(wsock); push a byte from
        # the peer end so the socket becomes readable and the stub fires.
        rsock.send(b'x')
        net.poll(timeout_ms=1000, future=done)

        assert done.is_done
        assert t._sock is None
        proto.connection_lost.assert_called_once_with(err)

    def test_can_write_eof(self, net):
        sock = _make_mock_sock()
        t = KafkaTCPTransport(net, sock)
        assert t.can_write_eof()

    def test_write_eof_sets_flag(self, net):
        sock = _make_mock_sock()
        t = KafkaTCPTransport(net, sock)
        t.write_eof()
        assert not t._write

    def test_end_to_end_write_read(self, net, socketpair):
        rsock, wsock = socketpair
        t = KafkaTCPTransport(net, wsock)
        t.write(b'hello world')

        f = Future()
        received = []
        async def reader():
            await net.wait_read(rsock)
            data = rsock.recv(1024)
            received.append(data)
            f.success(True)
        net.call_soon(reader)
        net.poll(timeout_ms=1000, future=f)
        assert received == [b'hello world']

    def test_write_eof_empty_buffer_shuts_down_immediately(self, net):
        # Fast path: nothing buffered, so write_eof half-closes right away.
        sock = _make_mock_sock()
        t = KafkaTCPTransport(net, sock)
        t.write_eof()
        sock.shutdown.assert_called_once_with(socket.SHUT_WR)

    def test_write_eof_defers_shutdown_until_buffer_flushed(self, net):
        # With data still buffered, write_eof must NOT shut down the write side
        # yet -- doing so would discard the unflushed bytes (the latent bug).
        sock = _make_mock_sock()
        t = KafkaTCPTransport(net, sock)
        t.write(b'pending')
        t.write_eof()
        assert not t._write
        assert t._write_buffer  # still buffered
        sock.shutdown.assert_not_called()

    def test_write_eof_flushes_buffered_data_then_shuts_write(self, net, socketpair):
        # Regression: data buffered before write_eof() must be fully delivered
        # to the peer, and only then is the write side shut down (peer sees EOF).
        rsock, wsock = socketpair
        t = KafkaTCPTransport(net, wsock)
        t.write(b'hello world')  # buffered; _write_to_sock scheduled
        t.write_eof()            # half-close requested -- flush must win
        assert t._write_buffer

        f = Future()
        received = []
        async def reader():
            while True:
                await net.wait_read(rsock)
                data = rsock.recv(1024)
                received.append(data)
                if data == b'':  # EOF => peer shut down its write side
                    break
            f.success(True)
        net.call_soon(reader)
        net.poll(timeout_ms=1000, future=f)

        assert b''.join(received) == b'hello world'
        assert received[-1] == b''  # shutdown(SHUT_WR) happened after the flush
        assert not t._write_buffer

    def test_close_empty_buffer_closes_immediately(self, net):
        # Fast path: nothing buffered, so close() tears down synchronously.
        sock = _make_mock_sock()
        proto = MagicMock()
        t = KafkaTCPTransport(net, sock)
        t.set_protocol(proto)
        t.close()
        assert t._sock is None
        proto.connection_lost.assert_called_once_with(None)

    def test_close_defers_teardown_until_buffer_flushed(self, net):
        # With data still buffered, close() must defer the actual socket
        # teardown to _write_to_sock so the bytes are not dropped.
        sock = _make_mock_sock()
        proto = MagicMock()
        t = KafkaTCPTransport(net, sock)
        t.set_protocol(proto)
        t.write(b'pending')
        t.close()
        assert t.is_closing()
        assert t._write_buffer       # not yet flushed
        assert t._sock is not None   # not yet closed
        proto.connection_lost.assert_not_called()

    def test_close_flushes_buffered_data_then_closes(self, net, socketpair):
        # Regression: data buffered before close() must reach the peer before
        # the transport tears the socket down.
        rsock, wsock = socketpair
        t = KafkaTCPTransport(net, wsock)
        proto = MagicMock()
        t.set_protocol(proto)
        t.write(b'goodbye world')  # buffered; _write_to_sock scheduled
        t.close()                  # closed flag set, teardown deferred
        assert t.is_closing()
        assert t._write_buffer

        f = Future()
        received = []
        async def reader():
            while True:
                await net.wait_read(rsock)
                data = rsock.recv(1024)
                received.append(data)
                if data == b'':
                    break
            f.success(True)
        net.call_soon(reader)
        net.poll(timeout_ms=1000, future=f)

        assert b''.join(received) == b'goodbye world'
        assert t._sock is None  # _write_to_sock flushed then _close()d
        proto.connection_lost.assert_called_once_with(None)

    def test_writeSequence(self, net):
        sock = _make_mock_sock()
        t = KafkaTCPTransport(net, sock)
        t.writeSequence([b'a', b'b'])
        assert len(t._write_buffer) == 2

    def test_str_with_tcp_socket(self, net):
        sock = _make_mock_sock()
        t = KafkaTCPTransport(net, sock)
        s = str(t)
        assert 'KafkaTCPTransport' in s
        assert '127.0.0.1' in s

    def test_str_closed(self, net):
        sock = _make_mock_sock()
        t = KafkaTCPTransport(net, sock)
        t._closed = True
        s = str(t)
        assert 'closed' in s


class TestKafkaSSLTransport:
    """Regression tests for https://github.com/dpkp/kafka-python/issues/3113:
    TLS SNI (server_hostname) must be sent regardless of ssl_check_hostname so
    that SNI-routed clusters (nginx/Istio/Strimzi ingress) remain reachable when
    hostname verification is disabled.
    """

    def _make_ssl_sock(self):
        # wrap_socket returns a wrapped socket; give it the peer/name accessors
        # that KafkaTCPTransport.__init__ pokes at via str()/repr helpers.
        wrapped = _make_mock_sock()
        sock = _make_mock_sock()
        ctx = MagicMock()
        ctx.wrap_socket.return_value = wrapped
        return sock, ctx, wrapped

    def test_sni_sent_when_check_hostname_true(self, net):
        sock, ctx, _ = self._make_ssl_sock()
        KafkaSSLTransport(net, sock, host='broker.example.com',
                          ssl_context=ctx, ssl_check_hostname=True)
        _, kwargs = ctx.wrap_socket.call_args
        assert kwargs['server_hostname'] == 'broker.example.com'

    def test_sni_sent_when_check_hostname_false(self, net):
        # The bug: SNI used to be suppressed when verification was disabled.
        sock, ctx, _ = self._make_ssl_sock()
        KafkaSSLTransport(net, sock, host='broker.example.com',
                          ssl_context=ctx, ssl_check_hostname=False)
        _, kwargs = ctx.wrap_socket.call_args
        assert kwargs['server_hostname'] == 'broker.example.com'

    def test_sni_strips_trailing_dot(self, net):
        # A trailing dot is a valid FQDN but illegal in the SNI extension.
        sock, ctx, _ = self._make_ssl_sock()
        KafkaSSLTransport(net, sock, host='broker.example.com.',
                          ssl_context=ctx, ssl_check_hostname=False)
        _, kwargs = ctx.wrap_socket.call_args
        assert kwargs['server_hostname'] == 'broker.example.com'

    def test_sni_none_when_host_missing(self, net):
        sock, ctx, _ = self._make_ssl_sock()
        KafkaSSLTransport(net, sock, host=None, ssl_context=ctx)
        _, kwargs = ctx.wrap_socket.call_args
        assert kwargs['server_hostname'] is None

    def test_handshake_not_done_on_connect(self, net):
        sock, ctx, _ = self._make_ssl_sock()
        KafkaSSLTransport(net, sock, host='broker.example.com', ssl_context=ctx)
        _, kwargs = ctx.wrap_socket.call_args
        assert kwargs['do_handshake_on_connect'] is False

    def test_provided_ssl_context_is_used(self, net):
        sock, ctx, wrapped = self._make_ssl_sock()
        t = KafkaSSLTransport(net, sock, host='broker.example.com',
                              ssl_context=ctx)
        assert t._ssl_context is ctx
        assert t._sock is wrapped

    def test_config_defaults_and_overrides(self, net):
        sock, ctx, _ = self._make_ssl_sock()
        t = KafkaSSLTransport(net, sock, host='h', ssl_context=ctx,
                              ssl_check_hostname=False)
        # Explicitly-passed ssl_* keys land in ssl_config...
        assert t.ssl_config['ssl_check_hostname'] is False
        assert t.ssl_config['ssl_context'] is ctx
        # ...unspecified keys keep their defaults.
        assert t.ssl_config['ssl_cafile'] is None
        assert t.ssl_config['ssl_check_hostname'] is not None


class TestBuildSSLContext:
    def test_returns_provided_context(self):
        ctx = MagicMock()
        config = dict(KafkaSSLTransport.DEFAULT_CONFIG, ssl_context=ctx)
        assert KafkaSSLTransport._build_ssl_context(config) is ctx

    def test_check_hostname_propagates_to_context(self):
        config = dict(KafkaSSLTransport.DEFAULT_CONFIG, ssl_check_hostname=False)
        ctx = KafkaSSLTransport._build_ssl_context(config)
        assert ctx.check_hostname is False

    def test_check_hostname_true_requires_verification(self):
        config = dict(KafkaSSLTransport.DEFAULT_CONFIG, ssl_check_hostname=True)
        ctx = KafkaSSLTransport._build_ssl_context(config)
        assert ctx.check_hostname is True


class TestTransportWaiterCleanup:
    """Regression: a locally-initiated close()/abort() must reclaim the socket
    read/write coroutine tasks parked in the event loop.

    These tests fail until ``KafkaTCPTransport._close`` cancels its read/write
    waiter tasks (``net.cancel(task)``); the selector's existing WAIT_IO branch
    in ``cancel()`` then drives the io_guard finalizer and discards the task.
    """

    def test_local_close_reclaims_parked_reader(self, net, socketpair):
        rsock, wsock = socketpair
        t = KafkaTCPTransport(net, wsock)
        t.set_protocol(MagicMock())
        baseline = len(net._pending_tasks)

        t.resume_reading()
        net.drain()  # reader runs and parks in WAIT_IO on wait_read(wsock)
        parked = [task for task in net._pending_tasks if task.state is TaskState.WAIT_IO]
        assert len(parked) == 1, 'reader did not park as expected'
        assert len(net._pending_tasks) == baseline + 1

        # Empty write buffer -> close() tears the socket down synchronously. The
        # peer (rsock) never sent anything, so there is no I/O event to wake the
        # parked reader; close() itself must reclaim it.
        t.close()
        net.drain()  # running the loop must not be needed -- and must not help either

        assert t._sock is None
        assert len(net._pending_tasks) == baseline, (
            'parked reader leaked into _pending_tasks after local close')
        assert not any(task.state is TaskState.WAIT_IO for task in net._pending_tasks)

    def test_local_close_reclaims_parked_writer(self, net, socketpair):
        rsock, wsock = socketpair
        t = KafkaTCPTransport(net, wsock)
        t.set_protocol(MagicMock())
        baseline = len(net._pending_tasks)

        # write() schedules _write_to_sock, whose loop parks on the first
        # wait_write before the bytes leave the buffer. drain() steps it once
        # to the park and returns (no _ready left), leaving it suspended.
        t.write(b'data')
        net.drain()
        parked = [task for task in net._pending_tasks if task.state is TaskState.WAIT_IO]
        assert len(parked) == 1, 'writer did not park as expected'

        t.abort(error=Errors.KafkaConnectionError('boom'))
        net.drain()

        assert t._sock is None
        assert len(net._pending_tasks) == baseline, (
            'parked writer leaked into _pending_tasks after abort')
        assert not any(task.state is TaskState.WAIT_IO for task in net._pending_tasks)
