"""Tests for the asyncio NetBackend (kafka/net/asyncio_backend.py).

Covers backend-specific behavior (lifecycle, timers, cross-thread run), reuses
the shared BackendFuture conformance suite against the asyncio-backed future,
and drives a real protocol round-trip through a MockBroker on a started
AsyncioBackend -- the both-backends coverage for the async paths.
"""
import asyncio
import socket
import threading
import time

import pytest

from kafka.net.asyncio_backend import AsyncioBackend, AsyncioFuture
from kafka.net.backend import NetBackend
from kafka.net.manager import KafkaConnectionManager
from kafka.protocol.metadata import MetadataRequest
from test.mock_broker import MockBroker
from test.net.test_backend_future import BackendFutureContract


@pytest.fixture
def backend():
    b = AsyncioBackend(client_id='test')
    try:
        yield b
    finally:
        b.close()


@pytest.fixture
def started_backend():
    b = AsyncioBackend(client_id='test')
    b.start()
    try:
        yield b
    finally:
        b.close()


class TestAsyncioBackendContract:
    def test_satisfies_netbackend(self, backend):
        assert isinstance(backend, NetBackend)

    def test_isinstance_after_start(self, started_backend):
        assert isinstance(started_backend, NetBackend)


class TestLifecycle:
    def test_start_idempotent(self, backend):
        backend.start()
        t = backend._io_thread
        backend.start()
        assert backend._io_thread is t

    def test_on_io_thread(self, started_backend):
        async def where():
            return started_backend.on_io_thread()
        assert started_backend.run(where) is True
        assert started_backend.on_io_thread() is False

    def test_stop_is_idempotent(self, backend):
        backend.start()
        backend.stop()
        backend.stop()  # no raise
        assert backend._io_thread is None

    def test_run_before_start_raises(self, backend):
        async def noop():
            return 1
        with pytest.raises(RuntimeError, match='not started'):
            backend.run(noop)

    def test_run_from_io_thread_raises(self, started_backend):
        async def nested():
            started_backend.run(lambda: 1)
        with pytest.raises(RuntimeError, match='IO thread'):
            started_backend.run(nested)

    def test_stop_fails_pending_run_waiters(self, started_backend):
        # A run() blocked on a never-resolving coroutine is released with an
        # error when the loop stops, rather than hanging forever.
        errors = []

        def caller():
            async def forever():
                await asyncio.Event().wait()
            try:
                started_backend.run(forever)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        t = threading.Thread(target=caller)
        t.start()
        time.sleep(0.1)
        started_backend.stop()
        t.join(timeout=5)
        assert not t.is_alive()
        assert len(errors) == 1


class TestRun:
    def test_run_coroutine_function_with_args(self, started_backend):
        async def add(x, y):
            return x + y
        assert started_backend.run(add, 2, 3) == 5

    def test_run_propagates_exception(self, started_backend):
        async def boom():
            raise ValueError('kaboom')
        with pytest.raises(ValueError, match='kaboom'):
            started_backend.run(boom)

    def test_run_resolves_returned_future(self, started_backend):
        # _invoke must bridge a trailing kafka Future to its value.
        def returns_future():
            fut = started_backend.create_future()
            started_backend.call_soon(lambda: fut.success('deferred'))
            return fut
        assert started_backend.run(returns_future) == 'deferred'


class TestTimers:
    def test_call_later_fires(self, started_backend):
        fired = threading.Event()
        started_backend.call_soon(lambda: started_backend.call_later(0.01, fired.set))
        assert fired.wait(timeout=2)

    def test_call_later_cancel_prevents_fire(self, started_backend):
        fired = threading.Event()

        async def schedule_and_cancel():
            handle = started_backend.call_later(0.5, fired.set)
            started_backend.cancel(handle)
        started_backend.run(schedule_and_cancel)
        assert not fired.wait(timeout=0.3)

    def test_call_at_converts_monotonic(self, started_backend):
        fired = threading.Event()
        when = time.monotonic() + 0.01
        started_backend.call_soon(lambda: started_backend.call_at(when, fired.set))
        assert fired.wait(timeout=2)

    def test_sleep_awaitable(self, started_backend):
        async def nap():
            start = time.monotonic()
            await started_backend.sleep(0.05)
            return time.monotonic() - start
        assert started_backend.run(nap) >= 0.04


class TestCreateFuture:
    def test_create_future_type(self, backend):
        fut = backend.create_future()
        assert isinstance(fut, AsyncioFuture)
        assert not fut.is_done


class TestAsyncioBackendFuture(BackendFutureContract):
    """Reuse the shared BackendFuture conformance suite for the asyncio future."""

    @pytest.fixture(autouse=True)
    def _net(self):
        self.net = AsyncioBackend(client_id='future-contract')
        self.net.start()
        try:
            yield
        finally:
            self.net.close()

    def make_future(self):
        return self.net.create_future()

    def drive(self, coros):
        async def _all():
            await asyncio.gather(*coros)
        self.net.run(_all)


class _StubProtocol:
    """Minimal KafkaConnection-shaped protocol for transport tests."""
    def __init__(self):
        self.received = bytearray()
        self.lost = False
    def data_received(self, data):
        self.received += data
    def eof_received(self):
        return None
    def connection_lost(self, exc):
        self.lost = True
    def pause_writing(self):
        pass
    def resume_writing(self):
        pass


def _echo_server():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(('127.0.0.1', 0))
    srv.listen(1)
    host, port = srv.getsockname()

    def serve():
        try:
            conn, _ = srv.accept()
        except OSError:
            return
        while True:
            data = conn.recv(4096)
            if not data:
                break
            conn.sendall(data)
        conn.close()

    t = threading.Thread(target=serve, daemon=True)
    t.start()
    return srv, host, port


class TestCreateConnection:
    def test_roundtrip_write_read_close(self, started_backend):
        srv, host, port = _echo_server()
        proto = _StubProtocol()

        async def do():
            transport = await started_backend.create_connection(proto, host, port)
            # Wire like manager._connect -> conn.connection_made does.
            transport.set_protocol(proto)
            transport.resume_reading()
            assert transport.host_port() == '%s:%s' % (host, port)
            assert transport.getPeer()[0:2] == (host, port)
            transport.write(b'ping')
            for _ in range(100):
                if proto.received:
                    break
                await started_backend.sleep(0.01)
            transport.close()
            return bytes(proto.received)

        try:
            assert started_backend.run(do) == b'ping'
        finally:
            srv.close()

    def test_early_data_is_buffered_until_wired(self, started_backend):
        # Server sends immediately on connect; data must not be lost before
        # the protocol is wired via set_protocol().
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(('127.0.0.1', 0))
        srv.listen(1)
        host, port = srv.getsockname()

        def serve():
            conn, _ = srv.accept()
            conn.sendall(b'hello-early')
            conn.recv(4096)
            conn.close()

        threading.Thread(target=serve, daemon=True).start()
        proto = _StubProtocol()

        async def do():
            transport = await started_backend.create_connection(proto, host, port)
            await started_backend.sleep(0.05)  # let early bytes arrive (buffered)
            transport.set_protocol(proto)      # wiring flushes the buffer
            transport.resume_reading()
            for _ in range(100):
                if proto.received:
                    break
                await started_backend.sleep(0.01)
            transport.close()
            return bytes(proto.received)

        try:
            assert started_backend.run(do) == b'hello-early'
        finally:
            srv.close()

    def test_connect_refused_raises(self, started_backend):
        # Nothing listening -> KafkaConnectionError.
        s = socket.socket(); s.bind(('127.0.0.1', 0)); host, port = s.getsockname(); s.close()

        async def do():
            await started_backend.create_connection(_StubProtocol(), host, port)

        with pytest.raises(Exception):  # KafkaConnectionError
            started_backend.run(do)

    def test_proxy_url_raises(self, started_backend):
        async def do():
            await started_backend.create_connection(
                _StubProtocol(), 'h', 1, proxy_url='socks5://proxy:1080')
        with pytest.raises(NotImplementedError, match='proxy'):
            started_backend.run(do)


class TestLoopConfig:
    def test_loop_factory_used_and_owned(self):
        calls = []

        def factory():
            calls.append(1)
            return asyncio.new_event_loop()

        b = AsyncioBackend(loop_factory=factory, client_id='lf')
        try:
            assert calls == [1]
            assert b._owns_loop is True
        finally:
            b.close()

    def test_injected_loop_not_owned_or_closed(self):
        loop = asyncio.new_event_loop()
        b = AsyncioBackend(loop=loop, client_id='inj')
        assert b._loop is loop
        assert b._owns_loop is False
        b.close()
        assert not loop.is_closed()  # injected loop must survive close()
        loop.close()


class TestEndToEndMockBroker:
    """Drive a real bootstrap + protocol round-trip through MockBroker on a
    started AsyncioBackend -- proves the backend runs the full IO machinery."""

    def test_bootstrap_and_send(self):
        broker = MockBroker()
        net = AsyncioBackend(client_id='e2e')
        manager = KafkaConnectionManager(
            net,
            bootstrap_servers='%s:%d' % (broker.host, broker.port),
            api_version=broker.broker_version,
            request_timeout_ms=5000,
        )
        broker.attach(manager)
        net.start()
        try:
            manager.bootstrap(timeout_ms=5000)
            assert manager.bootstrapped
            assert manager.cluster.brokers()

            async def do_send():
                return await manager.send(MetadataRequest[0]([]))
            resp = net.run(do_send)
            assert resp is not None
            assert broker.requests_received > 0
        finally:
            manager.close()
            net.close()
