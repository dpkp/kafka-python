# pylint: skip-file
"""Tests for WakeupNotifier, including the lost-wakeup race fix.

The notifier is level-triggered: a notify() that arrives while no one is
awaiting must be latched and consumed by the next __call__. Without that
latching the metadata-refresh loop in kafka/cluster.py can sleep for the
full metadata_max_age_ms (default 300s) even though request_update() was
called - the race is between ttl()/await in _refresh_loop and the
notify() coming in. See task #15 in this branch's task list.
"""

import threading
import time

import pytest

from kafka.future import Future
from kafka.net.selector import NetworkSelector
from kafka.net.wakeup_notifier import WakeupNotifier


@pytest.fixture
def net():
    return NetworkSelector()


@pytest.fixture
def notifier(net):
    return WakeupNotifier(net)


class TestWakeupNotifier:
    def test_timeout_fires_when_no_notify(self, net, notifier):
        """Without any notify(), __call__ returns after timeout_secs."""
        async def task():
            start = time.monotonic()
            await notifier(timeout_secs=0.05)
            return time.monotonic() - start

        elapsed = net.run(task)
        assert 0.04 <= elapsed < 0.5, (
            'expected ~0.05s timeout; took %.3fs' % elapsed)

    def test_notify_during_await_wakes_immediately(self, net, notifier):
        """notify() called while __call__ is awaiting returns the awaiter
        well before timeout_secs."""
        async def task():
            # Schedule a notify call_soon after a tiny delay, so the
            # notifier has entered its await before notify fires.
            async def fire_notify():
                notifier.notify()
            net.call_later(0.01, fire_notify)
            start = time.monotonic()
            await notifier(timeout_secs=5.0)
            return time.monotonic() - start

        elapsed = net.run(task)
        assert elapsed < 0.5, (
            'expected early wakeup; took %.3fs (close to 5s timeout?)' % elapsed)

    def test_notify_before_await_is_latched(self, net, notifier):
        """Race fix: notify() arriving while no one is awaiting must be
        latched so the next __call__ returns immediately. This is the
        scenario that the WakeupNotifier docstring describes."""
        async def task():
            # notify() before any awaiter - simulates the race in
            # cluster._refresh_loop where request_update() lands between
            # ttl() and await self._wakeup(...).
            notifier.notify()
            # Yield to let the queued _wakeup callback run (which sees
            # self._fut is None and latches _pending=True).
            await net.sleep(0)
            assert notifier._pending is True, 'notify should have latched'

            start = time.monotonic()
            await notifier(timeout_secs=5.0)
            elapsed = time.monotonic() - start
            assert notifier._pending is False, 'pending should be consumed'
            return elapsed

        elapsed = net.run(task)
        assert elapsed < 0.5, (
            'latched notify should fire __call__ immediately; took %.3fs'
            % elapsed)

    def test_consumed_pending_does_not_carry_over(self, net, notifier):
        """After a latched notification is consumed by one __call__, the
        next __call__ blocks normally until its own timeout."""
        async def task():
            notifier.notify()
            await net.sleep(0)
            await notifier(timeout_secs=5.0)  # consumes the latched wakeup
            assert notifier._pending is False

            # Second __call__: no pending notification, should hit timeout.
            start = time.monotonic()
            await notifier(timeout_secs=0.05)
            return time.monotonic() - start

        elapsed = net.run(task)
        assert 0.04 <= elapsed < 0.5, (
            'second __call__ should block until its own timeout; took %.3fs'
            % elapsed)

    def test_multiple_notifies_before_await_collapse_to_one(self, net, notifier):
        """Several notify() calls before an awaiter only produce a single
        immediate wakeup, not N (the latch is a boolean, not a counter).
        Subsequent __call__ blocks normally."""
        async def task():
            notifier.notify()
            notifier.notify()
            notifier.notify()
            await net.sleep(0)
            await notifier(timeout_secs=5.0)
            assert notifier._pending is False

            start = time.monotonic()
            await notifier(timeout_secs=0.05)
            return time.monotonic() - start

        elapsed = net.run(task)
        assert 0.04 <= elapsed < 0.5, (
            'a single latch should be consumed by one __call__; took %.3fs'
            % elapsed)

    def test_coalesced_notify_still_latches(self, net, notifier):
        """Coalescing guard: a burst of notify() before any awaiter schedules
        exactly one _wakeup (the rest are skipped because _scheduled is True),
        and that single _wakeup still latches _pending -- so the skips do NOT
        lose the wakeup. The next __call__ returns immediately."""
        async def task():
            notifier.notify()                     # schedules _wakeup; _scheduled=True
            assert notifier._scheduled is True
            notifier.notify()                     # coalesced: _scheduled already True
            notifier.notify()                     # coalesced
            await net.sleep(0)                    # let the one queued _wakeup run
            assert notifier._scheduled is False, '_wakeup must clear the guard'
            assert notifier._pending is True, 'coalesced burst must still latch'
            start = time.monotonic()
            await notifier(timeout_secs=5.0)
            assert notifier._pending is False, 'latch consumed'
            return time.monotonic() - start

        elapsed = net.run(task)
        assert elapsed < 0.5, (
            'latched wakeup should fire __call__ immediately; took %.3fs' % elapsed)

    def test_coalescing_reschedules_after_wakeup_runs(self, net, notifier):
        """Once a _wakeup has run (clearing _scheduled), a subsequent notify()
        must schedule a fresh wake rather than be silently skipped. Verifies
        _scheduled is not a one-way latch that would swallow later notifies."""
        async def task():
            notifier.notify()
            await net.sleep(0)                     # _wakeup runs, _scheduled=False
            await notifier(timeout_secs=5.0)       # consume first latched wake
            assert notifier._scheduled is False

            # A brand new notify() after the first cycle must wake again.
            notifier.notify()
            assert notifier._scheduled is True
            await net.sleep(0)
            assert notifier._pending is True
            start = time.monotonic()
            await notifier(timeout_secs=5.0)
            return time.monotonic() - start

        elapsed = net.run(task)
        assert elapsed < 0.5, (
            'second cycle should wake immediately; took %.3fs' % elapsed)

    def test_no_lost_wakeup_under_concurrent_notify_stress(self):
        """Probabilistic regression guard for the coalescing path: a consumer
        coroutine awaits the notifier every iteration (max race exposure) and
        drains a shared queue; many cross-thread producers append work and
        notify(). Each notify must either wake the current await, latch for the
        next one, or be coalesced into an already-pending wake -- never lost.

        A long (5s) awaiter timeout means a genuinely lost-and-uncovered wakeup
        would stall the consumer for ~5s; the test asserts the tail (time from
        'all producers done' to 'consumer drained everything') stays well under
        that. With the latch + coalescing correct, it finishes in milliseconds.
        """
        import collections
        net = NetworkSelector()
        net.start()
        try:
            notifier = WakeupNotifier(net)
            work = collections.deque()
            N = 4000
            n_threads = 4
            per = N // n_threads
            consumed = []
            finished = threading.Event()

            async def consumer():
                while len(consumed) < N:
                    await notifier(timeout_secs=5.0)   # always await -> every
                                                       # notify is a race chance
                    while work:                        # drain all available
                        try:
                            consumed.append(work.popleft())
                        except IndexError:
                            break
                finished.set()

            def producer(base):
                for i in range(per):
                    work.append((base, i))             # deque append is atomic
                    notifier.notify()

            net.call_soon_with_future(consumer)        # thread-safe schedule
            threads = [threading.Thread(target=producer, args=(b,), daemon=True)
                       for b in range(n_threads)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            t_joined = time.monotonic()
            ok = finished.wait(timeout=15)
            tail = time.monotonic() - t_joined

            assert ok, 'consumer never finished -- a wakeup was lost'
            assert len(consumed) == N, (
                'consumed %d of %d -- lost work' % (len(consumed), N))
            assert tail < 2.0, (
                'consumer stalled %.2fs after producers finished -- a tail '
                'wakeup was lost and only the 5s timeout recovered it' % tail)
        finally:
            net.stop()

    def test_notify_from_other_thread(self, net, notifier):
        """notify() is safe to call from another thread; the wakeup
        routes through call_soon_threadsafe to the IO thread."""
        async def task():
            def background():
                # Slight delay so the notifier is definitely awaiting.
                time.sleep(0.02)
                notifier.notify()
            t = threading.Thread(target=background, daemon=True)
            t.start()
            start = time.monotonic()
            await notifier(timeout_secs=5.0)
            t.join()
            return time.monotonic() - start

        elapsed = net.run(task)
        assert elapsed < 0.5, (
            'cross-thread notify should wake awaiter; took %.3fs' % elapsed)

    def test_concurrent_call_raises(self, net, notifier):
        """A second __call__ entered while one is already awaiting raises.
        This guards against accidental misuse - the notifier holds a
        single shared future and cannot serve two simultaneous awaiters."""
        first_done = Future()

        async def first_awaiter():
            await notifier(timeout_secs=1.0)
            first_done.success(None)

        async def task():
            net.call_soon(first_awaiter)
            await net.sleep(0)  # let first_awaiter enter its await
            with pytest.raises(RuntimeError, match='Concurrent access'):
                await notifier(timeout_secs=0.05)
            notifier.notify()  # release first_awaiter

        net.run(task)
        # Drain any remaining tasks (first_awaiter completing).
        net.poll(timeout_ms=500, future=first_done)
        assert first_done.is_done
