import socket
import time
from unittest.mock import MagicMock

import pytest

from kafka.future import Future
from kafka.net.selector import NetworkSelector
from kafka.net.transport import KafkaTCPTransport


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
