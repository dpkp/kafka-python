"""Conformance suite for the NetBackendFuture contract (kafka/future.py).

``NetBackendFutureContract`` is a reusable, backend-agnostic test mixin: it
pins the contract that every backend's ``create_future()`` must satisfy
(callback core, fan-out, await semantics). A backend provides two hooks --
``make_future()`` (a fresh pending future) and ``drive(coros)`` (run the
given coroutines on the backend's loop to completion) -- and inherits the
whole suite. The selector implementation is below; Step 4's asyncio backend
reuses the same mixin with an asyncio-driven subclass.
"""
import pytest

from kafka.future import Future
from kafka.net.backend import NetBackendFuture
from kafka.net.selector import NetworkSelector


class NetBackendFutureContract:
    # --- hooks a backend must provide -------------------------------------
    def make_future(self):
        raise NotImplementedError

    def drive(self, coros):
        """Run coros on the backend loop until all complete."""
        raise NotImplementedError

    # --- typing / identity ------------------------------------------------
    def test_satisfies_protocol(self):
        assert isinstance(self.make_future(), NetBackendFuture)

    # --- callback core (no loop needed) -----------------------------------
    def test_success_fires_callback(self):
        fut = self.make_future()
        seen = []
        fut.add_callback(seen.append)
        fut.success(42)
        assert seen == [42]
        assert fut.is_done and fut.succeeded()
        assert fut.value == 42 and fut.exception is None

    def test_failure_fires_errback(self):
        fut = self.make_future()
        errs = []
        fut.add_errback(errs.append)
        exc = ValueError('boom')
        fut.failure(exc)
        assert errs == [exc]
        assert fut.is_done and fut.failed()
        assert fut.exception is exc

    def test_success_does_not_fire_errback(self):
        fut = self.make_future()
        cb, eb = [], []
        fut.add_callback(cb.append)
        fut.add_errback(eb.append)
        fut.success('ok')
        assert cb == ['ok'] and eb == []

    def test_add_both_on_success_and_failure(self):
        ok = self.make_future()
        bad = self.make_future()
        seen_ok, seen_bad = [], []
        ok.add_both(seen_ok.append)
        bad.add_both(seen_bad.append)
        ok.success(1)
        exc = ValueError('x')
        bad.failure(exc)
        assert seen_ok == [1]
        assert seen_bad == [exc]

    def test_chain(self):
        src = self.make_future()
        dst = self.make_future()
        src.chain(dst)
        src.success('chained')
        assert dst.succeeded() and dst.value == 'chained'

    def test_callback_added_after_resolution_fires_immediately(self):
        fut = self.make_future()
        fut.success(7)
        seen = []
        fut.add_callback(seen.append)
        assert seen == [7]

    # --- fan-out ----------------------------------------------------------
    def test_multiple_callbacks_all_fire(self):
        fut = self.make_future()
        a, b, c = [], [], []
        fut.add_callback(a.append)
        fut.add_callback(b.append)
        fut.add_callback(c.append)
        fut.success('v')
        assert a == b == c == ['v']

    def test_multiple_awaiters_all_resume(self):
        fut = self.make_future()
        results = []

        async def waiter(i):
            results.append((i, await fut))

        async def resolver():
            fut.success('v')

        self.drive([waiter(1), waiter(2), waiter(3), resolver()])
        assert sorted(results) == [(1, 'v'), (2, 'v'), (3, 'v')]

    # --- await semantics --------------------------------------------------
    def test_await_then_resolve(self):
        fut = self.make_future()
        results = []

        async def waiter():
            results.append(await fut)

        async def resolver():
            fut.success('done')

        self.drive([waiter(), resolver()])
        assert results == ['done']

    def test_resolve_then_await(self):
        fut = self.make_future()
        fut.success('early')
        results = []

        async def waiter():
            results.append(await fut)

        self.drive([waiter()])
        assert results == ['early']

    def test_await_failure_raises(self):
        fut = self.make_future()
        caught = []

        async def waiter():
            try:
                await fut
            except ValueError as exc:
                caught.append(str(exc))

        async def resolver():
            fut.failure(ValueError('nope'))

        self.drive([waiter(), resolver()])
        assert caught == ['nope']


class TestNetworkSelectorNetBackendFuture(NetBackendFutureContract):
    """The selector backend: create_future() returns a SelectorFuture, driven
    synchronously via NetworkSelector.drain() (no IO thread needed)."""

    @pytest.fixture(autouse=True)
    def _net(self):
        self.net = NetworkSelector()
        try:
            yield
        finally:
            self.net.close()

    def make_future(self):
        return self.net.create_future()

    def drive(self, coros):
        for coro in coros:
            self.net.call_soon(coro)
        self.net.drain()


class TestNetBackendFutureTypeEnforcement:
    """A backend's create_future() result satisfies NetBackendFuture -- covered by
    TestNetworkSelectorNetBackendFuture.test_satisfies_protocol above. Here we pin
    the negative: the plain thread-safe Future (a cross-thread handoff)
    deliberately does NOT (no __await__), which turns 'awaited a handoff future'
    into a loud error rather than a silent backend-specific bug."""

    def test_plain_future_is_not_backend_future(self):
        assert not isinstance(Future(), NetBackendFuture)
        assert not hasattr(Future(), '__await__')

    def test_non_future_is_not_backend_future(self):
        assert not isinstance(object(), NetBackendFuture)
