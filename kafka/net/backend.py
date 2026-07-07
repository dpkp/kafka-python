"""Pluggable async-backend contracts.

For now this holds the :class:`BackendFuture` contract -- the surface of the
loop-awaitable futures a backend hands out from ``net.create_future()``. The
selector's implementation is ``kafka.net.selector.SelectorFuture``; an asyncio
(and eventually Twisted) backend supplies its own.
"""
from typing import Any, Callable, Protocol, runtime_checkable


@runtime_checkable
class BackendFuture(Protocol):
    """Contract for the awaitable futures returned by ``net.create_future()``.

    A pluggable async backend (the kafka.net selector, asyncio, Twisted, ...)
    returns its own future type from ``create_future()``. Core loop coroutines
    touch it only through this surface, so the type is interchangeable across
    backends. The selector's ``SelectorFuture`` is the reference implementation:
    it subclasses the thread-safe ``kafka.future.Future`` (the portable callback
    core) and adds ``__await__``. A plain ``Future`` is deliberately NOT a
    BackendFuture -- it has no ``__await__`` -- so awaiting a cross-thread
    handoff future fails loudly instead of silently working on one backend.

    Pinned semantics -- the three axes where backends could otherwise diverge:

    1. **Resolution thread.** A future from ``create_future()`` is created and
       resolved (``success`` / ``failure``) on the loop/IO thread only.
       Cross-thread handoffs (a user thread blocking on a loop result) use a
       plain thread-safe ``Future`` bridged via ``manager.wait_for`` /
       ``manager.run`` -- never a backend future awaited directly. Backends
       whose native awaitable is loop-affine (``asyncio.Future``, Twisted
       ``Deferred``) depend on this; their ``__await__`` adapter may assert it.

       There is no permanent third future category: a call site uses
       ``create_future()`` when a coroutine awaits the result on the loop, and
       a plain ``Future`` otherwise (a cross-thread handoff or a fan-out
       lifecycle event -- the eventual ``concurrent.futures.Future`` home).

    2. **Fan-out.** Multiple coroutines may ``await`` the same future and
       multiple callbacks may be registered; all are resumed / invoked. (A bare
       Twisted ``Deferred`` is single-consumer, so that backend wraps it
       per-awaiter inside ``__await__``.)

    3. **Callback timing is unspecified.** Callbacks may fire inline on the
       resolving thread (selector) or on a later loop iteration (asyncio).
       Callers must not assume callbacks have run when ``success`` / ``failure``
       returns. Code needing synchronous-resolution ordering uses a plain
       ``Future`` (e.g. the producer batch latch), not a backend future.

    ``__await__`` is the only backend-specific member; the callback core is
    portable and supplied by ``Future``.
    """

    is_done: bool
    value: Any
    exception: Any

    def __await__(self): ...
    def success(self, value: Any) -> 'BackendFuture': ...
    def failure(self, e: Any) -> 'BackendFuture': ...
    def add_callback(self, f: Callable, *args: Any, **kwargs: Any) -> 'BackendFuture': ...
    def add_errback(self, f: Callable, *args: Any, **kwargs: Any) -> 'BackendFuture': ...
    def add_both(self, f: Callable, *args: Any, **kwargs: Any) -> 'BackendFuture': ...
    def chain(self, future: 'BackendFuture') -> 'BackendFuture': ...
    def succeeded(self) -> bool: ...
    def failed(self) -> bool: ...
