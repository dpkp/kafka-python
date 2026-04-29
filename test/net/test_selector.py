import socket
import threading
import time

import pytest

from kafka.future import Future
from kafka.net.selector import (
    KernelEvent,
    NetworkSelector,
    Task,
    _initialize_coro,
)


class TestKernelEvent:
    def test_init(self):
        event = KernelEvent('_sleep', 1.0)
        assert event.method == '_sleep'
        assert event.args == (1.0,)

    def test_init_multiple_args(self):
        event = KernelEvent('_wait_write', 'fd', 'extra')
        assert event.args == ('fd', 'extra')

    def test_await(self):
        event = KernelEvent('_sleep', 1.0)
        coro = event.__await__()
        # Should yield self
        yielded = next(coro)
        assert yielded is event


class TestInitializeCoro:
    def test_generator(self):
        def gen():
            yield 1
        g = gen()
        assert _initialize_coro(g) is g

    def test_generator_function(self):
        def gen():
            yield 1
        result = _initialize_coro(gen)
        # Generator functions are deferred -- returned as-is
        assert result is gen

    def test_coroutine(self):
        async def coro():
            return 42
        c = coro()
        result = _initialize_coro(c)
        assert result is c
        c.close() # pylint: disable-msg=no-member

    def test_coroutine_function(self):
        async def coro():
            return 42
        result = _initialize_coro(coro)
        # Coroutine functions are deferred -- returned as-is
        assert result is coro

    def test_plain_function(self):
        called = []
        def func():
            called.append(True)
            return 99
        gen = _initialize_coro(func)
        val = next(gen)
        assert val == 99
        assert called

    def test_invalid_type(self):
        with pytest.raises(TypeError):
            _initialize_coro(42)


class TestTask:
    def test_generator_task(self):
        net = NetworkSelector()
        results = []
        def gen():
            results.append('started')
            yield KernelEvent('_sleep', 0)
        task = Task(gen)
        event = task()
        assert event.method == '_sleep'
        assert results == ['started']
        assert not task.is_done

    def test_async_task(self):
        net = NetworkSelector()
        async def coro():
            return 42
        task = Task(coro)
        with pytest.raises(StopIteration):
            task()
        assert task.is_done
        assert task.result == 42

    def test_async_task_with_await(self):
        net = NetworkSelector()
        async def coro():
            await net.sleep(0)
            return 'done'
        task = Task(coro)
        event = task()
        assert isinstance(event, KernelEvent)
        assert event.method == '_sleep'
        assert not task.is_done

    def test_nested_generators(self):
        net = NetworkSelector()
        def inner():
            yield KernelEvent('_sleep', 0)
        def outer():
            yield from inner()
        task = Task(outer)
        event = task()
        assert event.method == '_sleep'

    def test_nested_coroutines(self):
        net = NetworkSelector()
        async def inner():
            await net.sleep(0)
        async def outer():
            await inner()
        task = Task(outer)
        event = task()
        assert event.method == '_sleep'

    def test_exception_propagation(self):
        net = NetworkSelector()
        async def coro():
            raise ValueError('test error')
        task = Task(coro)
        with pytest.raises(ValueError, match='test error'):
            task()
        assert task.is_done
        assert isinstance(task.exception, ValueError)

    def test_result_before_done(self):
        net = NetworkSelector()
        async def coro():
            await net.sleep(0)
        task = Task(coro)
        task()
        with pytest.raises(RuntimeError):
            task.result
        with pytest.raises(RuntimeError):
            task.exception


class TestNetworkSelector:
    def test_call_soon(self):
        net = NetworkSelector()
        results = []
        def task():
            results.append(1)
            yield KernelEvent('_sleep', 0)
        net.call_soon(task)
        assert len(net._ready) == 1

    def test_run_simple(self):
        net = NetworkSelector()
        results = []
        async def task():
            results.append('ran')
        net.call_soon(task)
        net.run()
        assert results == ['ran']

    def test_run_multiple_tasks(self):
        net = NetworkSelector()
        results = []
        async def task(n):
            results.append(n)
        net.call_soon(task(1))
        net.call_soon(task(2))
        net.call_soon(task(3))
        net.run()
        assert results == [1, 2, 3]

    def test_call_later(self):
        net = NetworkSelector()
        results = []
        async def task():
            results.append('delayed')
        net.call_later(0.01, task)
        assert len(net._scheduled) == 1
        net.run()
        assert results == ['delayed']

    def test_call_at(self):
        net = NetworkSelector()
        results = []
        async def task():
            results.append('at')
        when = time.monotonic() + 0.01
        net.call_at(when, task)
        net.run()
        assert results == ['at']

    def test_run_until_done_with_task(self):
        net = NetworkSelector()
        async def task():
            return 42
        result = net.run_until_done(task)
        assert result.is_done
        assert result.result == 42

    def test_run_until_done_with_future(self):
        net = NetworkSelector()
        f = Future()
        async def resolve():
            f.success('hello')
        net.call_soon(resolve)
        net.run_until_done(f)
        assert f.value == 'hello'

    def test_sleep(self):
        net = NetworkSelector()
        event = net.sleep(1.0)
        assert isinstance(event, KernelEvent)
        assert event.method == '_sleep'
        assert event.args == (1.0,)

    def test_wait_read(self):
        net = NetworkSelector()
        event = net.wait_read('fd')
        assert isinstance(event, KernelEvent)
        assert event.method == '_wait_read'

    def test_wait_write(self):
        net = NetworkSelector()
        event = net.wait_write('fd')
        assert isinstance(event, KernelEvent)
        assert event.method == '_wait_write'

    def test_sleep_in_coroutine(self):
        net = NetworkSelector()
        results = []
        async def task():
            await net.sleep(0.01)
            results.append('after_sleep')
        net.run_until_done(task)
        assert results == ['after_sleep']

    def test_poll_with_future(self):
        net = NetworkSelector()
        f = Future()
        async def resolve():
            f.success(True)
        net.call_soon(resolve)
        net.poll(timeout_ms=1000, future=f)
        assert f.succeeded()

    def test_poll_timeout(self):
        net = NetworkSelector()
        f = Future()
        start = time.monotonic()
        net.poll(timeout_ms=100, future=f)
        elapsed = time.monotonic() - start
        assert not f.is_done
        assert elapsed < 0.5

    def test_poll_no_future(self):
        net = NetworkSelector()
        results = []
        async def task():
            results.append(1)
        net.call_soon(task)
        net.poll(timeout_ms=100)
        assert results == [1]

    def test_socketpair_read_write(self):
        net = NetworkSelector()
        rsock, wsock = socket.socketpair()
        rsock.setblocking(False)
        wsock.setblocking(False)
        results = []

        async def writer():
            await net.wait_write(wsock)
            wsock.send(b'hello')

        async def reader():
            await net.wait_read(rsock)
            data = rsock.recv(1024)
            results.append(data)

        net.call_soon(writer)
        net.call_soon(reader)
        f = Future()
        async def done():
            await net.sleep(0.05)
            f.success(True)
        net.call_soon(done)
        net.poll(timeout_ms=1000, future=f)
        rsock.close()
        wsock.close()
        assert results == [b'hello']

    def test_register_duplicate_read_raises(self):
        net = NetworkSelector()
        rsock, wsock = socket.socketpair()
        try:
            def dummy():
                yield
            net.register_event(rsock, 1, dummy)  # EVENT_READ = 1
            with pytest.raises(RuntimeError, match='already registered'):
                net.register_event(rsock, 1, dummy)
        finally:
            rsock.close()
            wsock.close()

    def test_unregister_nonexistent(self):
        net = NetworkSelector()
        rsock, wsock = socket.socketpair()
        try:
            # Should not raise
            net.unregister_event(rsock, 1)
        finally:
            rsock.close()
            wsock.close()

    def test_unschedule(self):
        net = NetworkSelector()
        def task():
            yield
        t = net.call_later(10, task)
        assert len(net._scheduled) == 1
        net.unschedule(t)
        assert len(net._scheduled) == 0
        assert t.scheduled_at is None

    def test_unschedule_unscheduled(self):
        net = NetworkSelector()
        def task():
            yield
        assert len(net._scheduled) == 0
        net.unschedule(Task(task))
        assert len(net._scheduled) == 0

    def test_reschedule(self):
        net = NetworkSelector()
        def task():
            yield
        t = net.call_later(10, task)
        new_when = time.monotonic() + 0.01
        net.reschedule(new_when, t)
        assert len(net._scheduled) == 1
        assert net._scheduled[0][0] == new_when

    def test_reschedule_unscheduled(self):
        net = NetworkSelector()
        def task():
            yield
        new_when = time.monotonic() + 0.01
        net.reschedule(new_when, Task(task))
        assert len(net._scheduled) == 1
        assert net._scheduled[0][0] == new_when

    def test_await_future_already_done(self):
        net = NetworkSelector()
        f = Future()
        f.success(99)
        results = []

        async def waiter():
            val = await f
            results.append(val)

        net.run_until_done(waiter)
        assert results == [99]

    def test_await_future_already_failed(self):
        net = NetworkSelector()
        f = Future()
        f.failure(ValueError('oops'))
        errors = []

        async def waiter():
            try:
                await f
            except ValueError as e:
                errors.append(str(e))

        net.run_until_done(waiter)
        assert errors == ['oops']

    def test_wakeup(self):
        net = NetworkSelector()
        f = Future()
        # poll should block, but wakeup from another thread unblocks it
        def wake_after_delay():
            time.sleep(0.05)
            net.wakeup()
            # Schedule a task that resolves the future
            net.call_soon_threadsafe(lambda: f.success(True))

        t = threading.Thread(target=wake_after_delay)
        t.start()
        start = time.monotonic()
        net.poll(timeout_ms=5000, future=f)
        elapsed = time.monotonic() - start
        t.join()
        assert f.succeeded()
        assert elapsed < 1.0

    def test_call_soon_threadsafe(self):
        net = NetworkSelector()
        results = []
        f = Future()

        def background():
            time.sleep(0.02)
            net.call_soon_threadsafe(lambda: results.append('from_thread'))
            net.call_soon_threadsafe(lambda: f.success(True))

        t = threading.Thread(target=background)
        t.start()
        net.poll(timeout_ms=1000, future=f)
        t.join()
        assert results == ['from_thread']

    def test_wakeup_is_idempotent(self):
        net = NetworkSelector()
        # Multiple wakeups should not cause errors
        net.wakeup()
        net.wakeup()
        net.wakeup()
        # Drain wakeup bytes by running one poll
        net.poll(timeout_ms=0)

    def test_close(self):
        net = NetworkSelector()
        net.close()
        assert net._closed

    def test_await_future_resolved(self):
        net = NetworkSelector()
        f = Future()
        f.success(42)
        results = []

        async def task():
            val = await f
            results.append(val)

        net.run_until_done(task)
        assert results == [42]

    def test_await_future_pending(self):
        net = NetworkSelector()
        f = Future()
        results = []
        done = Future()

        async def waiter():
            val = await f
            results.append(val)
            done.success(True)

        async def resolver():
            await net.sleep(0.01)
            f.success('hello')

        net.call_soon(waiter)
        net.call_soon(resolver)
        net.poll(timeout_ms=1000, future=done)
        assert results == ['hello']

    def test_await_future_failure(self):
        net = NetworkSelector()
        f = Future()
        errors = []
        done = Future()

        async def waiter():
            try:
                await f
            except ValueError as e:
                errors.append(str(e))
            done.success(True)

        async def resolver():
            await net.sleep(0.01)
            f.failure(ValueError('boom'))

        net.call_soon(waiter)
        net.call_soon(resolver)
        net.poll(timeout_ms=1000, future=done)
        assert errors == ['boom']

    def test_await_multiple_futures(self):
        net = NetworkSelector()
        f1 = Future()
        f2 = Future()
        results = []
        done = Future()

        async def waiter():
            val1 = await f1
            val2 = await f2
            results.append((val1, val2))
            done.success(True)

        async def resolver():
            await net.sleep(0.01)
            f1.success('a')
            await net.sleep(0.01)
            f2.success('b')

        net.call_soon(waiter)
        net.call_soon(resolver)
        net.poll(timeout_ms=1000, future=done)
        assert results == [('a', 'b')]
