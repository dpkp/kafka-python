import socket
import selectors
import threading
import time

import pytest

from kafka.errors import KafkaTimeoutError
from kafka.future import Future
from kafka.net.selector import (
    KernelEvent,
    NetworkSelector,
    Task,
    TaskState,
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

    def test_drain_simple(self):
        net = NetworkSelector()
        results = []
        async def task():
            results.append('ran')
        net.call_soon(task)
        net.drain()
        assert results == ['ran']

    def test_drain_multiple_tasks(self):
        net = NetworkSelector()
        results = []
        async def task(n):
            results.append(n)
        net.call_soon(task(1))
        net.call_soon(task(2))
        net.call_soon(task(3))
        net.drain()
        assert results == [1, 2, 3]

    def test_call_later(self):
        net = NetworkSelector()
        results = []
        async def task():
            results.append('delayed')
        net.call_later(0.01, task)
        assert len(net._scheduled) == 1
        net.drain(scheduled=True)
        assert results == ['delayed']

    def test_call_at(self):
        net = NetworkSelector()
        results = []
        async def task():
            results.append('at')
        when = time.monotonic() + 0.01
        net.call_at(when, task)
        net.drain(scheduled=True)
        assert results == ['at']

    def test_run_with_task(self):
        net = NetworkSelector()
        async def task():
            return 42
        result = net.run(task)
        assert result == 42

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
        net.run(task)
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

    def test_wait_io_timeout_raises(self):
        # A timed-out I/O wait must raise KafkaTimeoutError into the awaiting
        # coroutine. Regression: the injected exception was dropped because
        # _poll_once cleared self._exc before __call__ could consume it.
        net = NetworkSelector()
        rsock, wsock = socket.socketpair()
        rsock.setblocking(False)
        wsock.setblocking(False)
        outcome = []

        async def reader():
            try:
                await net.wait_read(rsock, timeout_at=time.monotonic() + 0.02)
                outcome.append(('resumed', rsock.recv(1024)))
            except KafkaTimeoutError:
                outcome.append(('timeout',))

        net.call_soon(reader)
        try:
            net.drain(scheduled=True)
            assert outcome == [('timeout',)], outcome
            with pytest.raises(KeyError):
                net._selector.get_key(rsock)
            assert len(net._scheduled) == 0
        finally:
            rsock.close()
            wsock.close()

    def test_wait_io_resume_unregisters_and_cancels_timer(self):
        # Normal I/O readiness must unregister the fileobj and cancel the
        # paired timeout timer; cleanup runs in the io_guard finally.
        net = NetworkSelector()
        rsock, wsock = socket.socketpair()
        rsock.setblocking(False)
        wsock.setblocking(False)
        seen = []

        async def reader():
            await net.wait_read(rsock, timeout_at=time.monotonic() + 100)
            seen.append(rsock.recv(1024))

        net.call_soon(reader)
        try:
            net.drain()                        # park on I/O
            assert net._selector.get_key(rsock) is not None
            assert len(net._scheduled) == 1    # paired timer scheduled
            wsock.send(b'hi')
            net.poll(timeout_ms=200)           # resume on readability
            assert seen == [b'hi'], seen
            with pytest.raises(KeyError):
                net._selector.get_key(rsock)
            assert len(net._scheduled) == 0    # paired timer cancelled
        finally:
            rsock.close()
            wsock.close()

    def test_cancel_unregisters_wait_io_task(self):
        # Cancelling a task parked on I/O must unregister its fileobj and must
        # not let the task resume.
        net = NetworkSelector()
        rsock, wsock = socket.socketpair()
        rsock.setblocking(False)
        wsock.setblocking(False)

        async def reader():
            await net.wait_read(rsock)         # no timeout: bare-guard path
            raise AssertionError('cancelled task must not resume')

        task = net.call_soon(reader)
        try:
            net.drain()                        # park on I/O
            assert task.state is TaskState.WAIT_IO
            assert net._selector.get_key(rsock) is not None
            net.cancel(task)
            assert task.state is TaskState.CANCELLED
            with pytest.raises(KeyError):
                net._selector.get_key(rsock)
            net.drain()                        # ensure it never runs
        finally:
            rsock.close()
            wsock.close()

    def test_cancel_running_task(self):
        # A task may cancel itself while running. Task.close() cannot close the
        # currently-executing coroutine from within it (ValueError: coroutine
        # already executing); Task.close()'s guard must absorb that. The task is
        # then torn down when it next suspends, and the kernel event it yielded
        # after cancelling is short-circuited -- so no I/O registration happens.
        net = NetworkSelector()
        rsock, wsock = socket.socketpair()
        rsock.setblocking(False)
        wsock.setblocking(False)
        holder = {}

        async def self_cancel():
            net.cancel(holder['task'])      # cancel self while RUNNING
            await net.wait_read(rsock)      # short-circuited; must not register
            raise AssertionError('cancelled task must not run past the await')

        task = net.call_soon(self_cancel)
        holder['task'] = task
        try:
            net.drain()
            assert task.state is TaskState.CANCELLED, task.state
            assert task.is_done
            assert task not in net._pending_tasks
            with pytest.raises(KeyError):
                net._selector.get_key(rsock)   # never registered after cancel
        finally:
            rsock.close()
            wsock.close()

    def test_cancel_wait_io_after_socket_closed(self):
        # During shutdown a connection may unregister and close its socket
        # before the parked task is cancelled. The io_guard finally then runs
        # unregister_event on a closed fileobj (fileno() == -1) that is no
        # longer in the selector map; this must be tolerated, not raise
        # ValueError (which surfaced as an ignored-in-generator warning).
        net = NetworkSelector()
        rsock, wsock = socket.socketpair()
        rsock.setblocking(False)
        wsock.setblocking(False)

        async def reader():
            await net.wait_read(rsock)
            raise AssertionError('cancelled task must not resume')

        task = net.call_soon(reader)
        try:
            net.drain()                        # park on I/O
            # simulate connection teardown: drop the registration, then close
            net.unregister_event(rsock, selectors.EVENT_READ)
            rsock.close()                      # fileno() -> -1
            net.cancel(task)                   # io_guard unregisters a dead fd
            assert task.state is TaskState.CANCELLED
        finally:
            wsock.close()

    def test_wait_on_closed_socket_injects_into_coro(self):
        # Arming an I/O wait on a socket whose fd is already closed (fileno()
        # == -1) makes register_event raise.
        # That failure must not crash the whole IO loop; it
        # must be injected back into the awaiting coro at its await site.
        net = NetworkSelector()
        rsock, wsock = socket.socketpair()
        rsock.setblocking(False)
        wsock.setblocking(False)
        rsock.close()                              # fileno() -> -1 up front
        outcome = []

        async def reader():
            try:
                await net.wait_read(rsock)
            except BaseException as e:              # pylint: disable=broad-except
                outcome.append(type(e).__name__)

        task = net.call_soon(reader)
        try:
            net.drain()                            # must not raise / crash loop
            assert outcome and outcome[0] == 'ValueError', outcome
            assert task.is_done
            assert task not in net._pending_tasks
            assert net._current is None
        finally:
            wsock.close()

    def test_running_task_demoted_when_left_running(self):
        # Defensive backstop: a kernel-event handler that returns without
        # parking the task leaves it stranded in RUNNING. _poll_once's finally
        # must demote it to UNSCHEDULED (with a warning) -- not crash the loop
        # -- so a later cancel()/close() can reclaim it instead of tripping
        # cancel()'s `task is self._current` assert. Simulate a buggy handler
        # with a no-op method that parks nothing.
        net = NetworkSelector()
        net._buggy_noop = lambda: None             # handler that parks nothing

        def coro():
            yield KernelEvent('_buggy_noop')

        task = net.call_soon(coro)
        net.drain()                                # demotes + warns, no crash
        assert task.state is TaskState.UNSCHEDULED, task.state
        assert net._current is None
        # cancel() must reclaim the demoted task without asserting.
        net.cancel(task)
        assert task.state is TaskState.CANCELLED
        assert task not in net._pending_tasks

    def test_cancel_wait_io_cancels_paired_timer(self):
        # Cancelling a timed I/O wait tears down both the fileobj and the
        # paired timeout timer.
        net = NetworkSelector()
        rsock, wsock = socket.socketpair()
        rsock.setblocking(False)
        wsock.setblocking(False)

        async def reader():
            await net.wait_read(rsock, timeout_at=time.monotonic() + 100)
            raise AssertionError('cancelled task must not resume')

        task = net.call_soon(reader)
        try:
            net.drain()
            assert len(net._scheduled) == 1
            timer = net._scheduled[0][1]
            net.cancel(task)
            with pytest.raises(KeyError):
                net._selector.get_key(rsock)
            assert timer.is_done               # paired timer cancelled/closed
            assert len(net._scheduled) == 0
        finally:
            rsock.close()
            wsock.close()

    def test_unschedule(self):
        net = NetworkSelector()
        def task():
            yield
        t = net.call_later(10, task)
        assert len(net._scheduled) == 1
        net._unschedule(t)
        assert len(net._scheduled) == 0
        assert t.scheduled_at is None

    def test_unschedule_unscheduled_raises(self):
        net = NetworkSelector()
        def task():
            yield
        assert len(net._scheduled) == 0
        with pytest.raises(AssertionError):
            net._unschedule(Task(task))
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
        f = net.create_future()
        f.success(99)
        results = []

        async def waiter():
            val = await f
            results.append(val)

        net.run(waiter)
        assert results == [99]

    def test_await_future_already_failed(self):
        net = NetworkSelector()
        f = net.create_future()
        f.failure(ValueError('oops'))
        errors = []

        async def waiter():
            try:
                await f
            except ValueError as e:
                errors.append(str(e))

        net.run(waiter)
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
        f = net.create_future()
        f.success(42)
        results = []

        async def task():
            val = await f
            results.append(val)

        net.run(task)
        assert results == [42]

    def test_await_future_pending(self):
        net = NetworkSelector()
        f = net.create_future()
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
        f = net.create_future()
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
        f1 = net.create_future()
        f2 = net.create_future()
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

    def test_cancel_closes_ready_task(self):
        net = NetworkSelector()
        fired = []
        timer = net.call_at(time.monotonic() - 1, lambda: fired.append(True))
        net._schedule_tasks()  # move the due timer from the heap into _ready
        assert timer in net._ready
        assert timer.scheduled_at is None

        net.cancel(timer)

        assert timer in net._ready  # timer still in ready queue
        assert timer.is_done, \
            'unschedule() did not close task queued in _ready'
        net.drain()  # skips the queued timer without running
        assert fired == []


class TestCreateFuture:
    """The pluggable-backend portability seam.

    Core coroutines call ``net.create_future()`` instead of constructing
    ``Future`` directly so an alternate backend can return its own awaitable.
    For ``NetworkSelector`` the native awaitable is ``kafka.future.Future``;
    the contract these tests pin is: it is awaitable on the loop *and* honors
    the callback-chain surface (``add_callback`` / ``add_errback`` /
    ``add_both`` / ``success`` / ``failure``) that callers like
    ``KafkaConnectionManager.wait_for`` rely on.
    """

    def test_returns_future(self):
        net = NetworkSelector()
        fut = net.create_future()
        assert isinstance(fut, Future)
        assert not fut.is_done

    def test_callback_chain(self):
        net = NetworkSelector()
        fut = net.create_future()
        seen = []
        fut.add_callback(seen.append)
        fut.success(42)
        assert seen == [42]
        assert fut.succeeded() and fut.value == 42

    def test_errback_chain(self):
        net = NetworkSelector()
        fut = net.create_future()
        errs = []
        fut.add_errback(errs.append)
        exc = ValueError('boom')
        fut.failure(exc)
        assert errs == [exc]
        assert fut.failed()

    def test_awaitable_on_loop(self):
        net = NetworkSelector()
        result = []

        async def producer(fut):
            fut.success('done')

        async def consumer(fut):
            result.append(await fut)

        fut = net.create_future()
        net.call_soon(consumer(fut))
        net.call_soon(producer(fut))
        net.drain()
        assert result == ['done']


class TestSlowTaskMonitor:
    """Detection for tasks that hog the event loop (livelock guard).

    See task #44: a coroutine in a tight sync loop never yields back to the
    selector. From the outside this looks like a hang; with monitoring it
    becomes a clean warning (or, in raise-mode, a RuntimeError).
    """

    def test_slow_task_warns_with_default_threshold(self, caplog):
        net = NetworkSelector(slow_task_threshold_secs=0.01)
        done = Future()

        async def hog():
            time.sleep(0.05)  # synchronous sleep - does not yield to loop
            done.success(True)

        net.call_soon(hog)
        with caplog.at_level('WARNING', logger='kafka.net.selector'):
            net.poll(timeout_ms=1000, future=done)
        assert any('blocking the event loop' in rec.message for rec in caplog.records), (
            'expected slow-task warning, got: %r'
            % [(r.levelname, r.message) for r in caplog.records])
        assert done.succeeded()

    def test_slow_task_below_threshold_no_warning(self, caplog):
        net = NetworkSelector(slow_task_threshold_secs=0.5)
        done = Future()

        async def quick():
            done.success(True)

        net.call_soon(quick)
        with caplog.at_level('WARNING', logger='kafka.net.selector'):
            net.poll(timeout_ms=1000, future=done)
        assert not any('blocking the event loop' in rec.message for rec in caplog.records)

    def test_slow_task_disabled_when_threshold_zero(self, caplog):
        net = NetworkSelector(slow_task_threshold_secs=0)
        done = Future()

        async def hog():
            time.sleep(0.02)
            done.success(True)

        net.call_soon(hog)
        with caplog.at_level('WARNING', logger='kafka.net.selector'):
            net.poll(timeout_ms=1000, future=done)
        assert not any('blocking the event loop' in rec.message for rec in caplog.records)

    def test_slow_task_raise_mode(self):
        net = NetworkSelector(slow_task_threshold_secs=0.01,
                              raise_on_slow_task=True)
        done = Future()

        async def hog():
            time.sleep(0.05)
            done.success(True)

        net.call_soon(hog)
        with pytest.raises(RuntimeError, match='blocking the event loop'):
            net.poll(timeout_ms=1000, future=done)

    def test_concurrent_poll_raises(self):
        """Two threads calling poll() simultaneously should raise instead of
        racing on selector / task state."""
        net = NetworkSelector()
        gate = threading.Event()
        done = Future()
        errors = []

        async def slow():
            gate.set()
            time.sleep(0.1)
            done.success(True)

        def driver_a():
            net.call_soon(slow)
            net.poll(timeout_ms=1000, future=done)

        def driver_b():
            gate.wait(timeout=1)
            try:
                net.poll(timeout_ms=10)
            except RuntimeError as exc:
                errors.append(str(exc))

        ta = threading.Thread(target=driver_a)
        tb = threading.Thread(target=driver_b)
        ta.start()
        tb.start()
        ta.join(2)
        tb.join(2)
        assert errors and 'Concurrent access' in errors[0], (
            'expected Concurrent access error, got: %r' % errors)

    def test_recursive_poll_raises_recursive_error(self):
        """A task callback calling poll() reentrantly should be diagnosed as
        recursive (same-thread), not as concurrent."""
        net = NetworkSelector()
        errors = []

        async def reenter():
            try:
                net.poll(timeout_ms=10)
            except RuntimeError as exc:
                errors.append(str(exc))

        net.call_soon(reenter)
        net.poll(timeout_ms=100)
        assert errors and 'Recursive access' in errors[0], (
            'expected Recursive access error, got: %r' % errors)

    def test_poll_lock_released_on_exception(self):
        """An exception in _poll_once must release the poll lock so the next
        caller doesn't see a stale 'Concurrent access' error."""
        net = NetworkSelector()

        # Inject a coroutine that raises a base-level error to escape the
        # per-task BaseException catch (StopIteration / Exception are caught).
        # We use a custom signal: monkey-patch _poll_once to raise.
        orig = net._poll_once
        first_call = [True]

        def _poll_once_raising(*args, **kwargs):
            if first_call[0]:
                first_call[0] = False
                raise KeyboardInterrupt('simulated Ctrl-C')
            return orig(*args, **kwargs)

        net._poll_once = _poll_once_raising
        with pytest.raises(KeyboardInterrupt):
            net.poll(timeout_ms=10)

        # Restore and verify the lock was released so the next poll succeeds.
        net._poll_once = orig
        net.poll(timeout_ms=10)  # would raise 'Concurrent access' if leaked


class TestRunBridgeBackstop:
    """Issue #3121: NetworkSelector.run() must bound its cross-thread wait so a
    stalled IO loop can't hang a caller thread forever.

    The original bug: run() blocked on a bare event.wait() with no timeout, so a
    user-thread commit()/poll() hung indefinitely whenever the IO loop stopped
    making progress (a blocking sync rebalance listener/assignor, or a blocking
    DNS lookup on the IO thread). run() now bounds the wait by timeout_ms (or
    default_api_timeout_ms when None) plus a grace margin, and raises
    KafkaTimeoutError as a liveness backstop.
    """

    @staticmethod
    def _wedge(net, release):
        """Occupy the single IO thread until released; returns (wedged, release)."""
        wedged = threading.Event()

        async def wedge():
            wedged.set()
            release.wait(timeout=5.0)  # safety cap so the suite can't hang

        net.call_soon_threadsafe(wedge)
        assert wedged.wait(timeout=1.0), 'IO thread never entered the wedge'

    def _run_in_thread(self, net, coro, **kw):
        outcome = {}

        def caller():
            start = time.monotonic()
            try:
                outcome['value'] = net.run(coro, **kw)
            except BaseException as exc:  # noqa: B036 - record whatever surfaces
                outcome['exc'] = exc
            outcome['elapsed'] = time.monotonic() - start

        th = threading.Thread(target=caller, daemon=True)
        th.start()
        th.join(timeout=2.0)
        return th, outcome

    def test_default_deadline_bounds_wedged_loop(self):
        """With no explicit timeout_ms, run() is still bounded by
        default_api_timeout_ms (+grace) -- the core #3121 fix."""
        net = NetworkSelector(default_api_timeout_ms=100, bridge_grace_ms=100)
        net.start()

        async def work():
            return 'ok'

        release = threading.Event()
        try:
            self._wedge(net, release)
            th, outcome = self._run_in_thread(net, work)  # no timeout_ms
            assert not th.is_alive(), 'run() hung past the default deadline (#3121)'
            assert isinstance(outcome.get('exc'), KafkaTimeoutError), (
                'expected KafkaTimeoutError, got: %r' % outcome)
            assert 0.15 <= outcome['elapsed'] < 2.0
            assert 'may be stalled' in str(outcome['exc'])
        finally:
            release.set()
            net.close()

    def test_explicit_timeout_bounds_wedged_loop(self):
        """An explicit timeout_ms is honored even when the loop is wedged."""
        net = NetworkSelector(default_api_timeout_ms=60000, bridge_grace_ms=100)
        net.start()

        async def work():
            return 'ok'

        release = threading.Event()
        try:
            self._wedge(net, release)
            th, outcome = self._run_in_thread(net, work, timeout_ms=100)
            assert not th.is_alive()
            assert isinstance(outcome.get('exc'), KafkaTimeoutError)
            assert 0.15 <= outcome['elapsed'] < 2.0
        finally:
            release.set()
            net.close()

    def test_healthy_loop_returns_value(self):
        """The backstop must not perturb the normal path."""
        net = NetworkSelector(default_api_timeout_ms=100, bridge_grace_ms=100)
        net.start()

        async def work():
            return 42

        try:
            assert net.run(work) == 42
        finally:
            net.close()

    def test_healthy_loop_propagates_coroutine_exception(self):
        """A coroutine that raises surfaces its own exception, not the backstop's
        generic timeout -- the backstop only fires on non-completion."""
        net = NetworkSelector(default_api_timeout_ms=100, bridge_grace_ms=100)
        net.start()

        async def boom():
            raise ValueError('from coroutine')

        try:
            with pytest.raises(ValueError, match='from coroutine'):
                net.run(boom)
        finally:
            net.close()

    def test_no_io_thread_fallback_honors_deadline(self):
        """The no-IO-thread fallback path (drives the loop inline) also bounds
        the wait -- previously it called poll(future=...) with no timeout."""
        net = NetworkSelector(default_api_timeout_ms=100, bridge_grace_ms=50)
        never = net.create_future()  # never resolved

        async def waits_forever():
            return await never

        try:
            start = time.monotonic()
            with pytest.raises(KafkaTimeoutError):
                net.run(waits_forever)  # no start() -> fallback path
            elapsed = time.monotonic() - start
            assert 0.1 <= elapsed < 2.0
        finally:
            net.close()
