"""The pluggable async-backend contract.

``NetBackend`` is the interface the rest of kafka-python depends on for its
event loop: the surface that ``KafkaProducer`` / ``KafkaConsumer`` /
``KafkaAdminClient`` (and the manager, cluster, connection, coordinator,
fetcher, sender) reach for through ``self._net`` / ``manager._net``.

``NetworkSelector`` (``kafka/net/selector.py``) is the reference
implementation; an asyncio backend (and eventually Twisted) implements the
same surface so it can be swapped in via ``net=`` without touching core code.

The :class:`BackendFuture` contract is the surface of the
loop-awaitable futures a backend hands out from ``net.create_future()``. The
selector's implementation is ``kafka.net.selector.SelectorFuture``; an asyncio
(and eventually Twisted) backend supplies its own.

Two things are intentionally **not** part of the contract:

* ``poll(timeout_ms, future=...)`` -- the legacy single-tick driver. Its only
  remaining caller is the ``KafkaNetClient`` compat shim
  (``kafka/net/compat.py``), which is selector-bound; asyncio has no bounded
  single-tick equivalent (``run_forever`` never returns, ``run_until_complete``
  runs to a specific future). New code does not use it.
* ``register_event`` / selector internals -- backend-private plumbing.
  ``unregister_event`` is included only because the transport calls it on the
  cleanup path.

Method families:

* **Lifecycle** -- ``start`` / ``stop`` / ``close`` / ``on_io_thread``.
* **Scheduling** -- ``call_soon`` / ``call_soon_threadsafe`` /
  ``call_soon_with_future`` / ``call_at`` / ``call_later`` / ``cancel``.
* **Awaitable primitives** -- ``sleep`` / ``wait_read`` / ``wait_write``.
  These return a backend-specific awaitable (a ``KernelEvent`` for the
  selector; an ``async def`` result for asyncio) -- core coroutines only
  ``await`` them.
* **Cross-thread bridge** -- ``run`` (schedule on the loop, block the caller).
* **Future factory** -- ``create_future`` (see ``BackendFuture``).
* **Cross-thread wake** -- ``wakeup``.
"""
from typing import Any, Callable, Optional, Protocol, runtime_checkable


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


@runtime_checkable
class NetBackend(Protocol):
    """Structural contract for a pluggable async event-loop backend.

    ``runtime_checkable`` so conformance can be asserted with ``isinstance``;
    note that only checks member *presence*, not signatures. ``NetworkSelector``
    satisfies this structurally (no explicit inheritance needed).
    """

    # --- lifecycle --------------------------------------------------------
    def start(self) -> None:
        """Spawn/attach the IO thread that runs the loop. Idempotent."""

    def stop(self, timeout_ms: Optional[float] = None) -> None:
        """Stop the loop and join the IO thread. Idempotent."""

    def close(self) -> None:
        """Stop (if running) and release loop resources. Idempotent."""

    def on_io_thread(self) -> bool:
        """True if the caller is running on this backend's IO thread."""

    # --- scheduling -------------------------------------------------------
    def call_soon(self, task: Any) -> Any:
        """Enqueue a coroutine/callable to run on the next loop iteration."""

    def call_soon_threadsafe(self, callback: Any) -> Any:
        """``call_soon`` from another thread; wakes the loop."""

    def call_soon_with_future(self, coro: Any, *args: Any) -> BackendFuture:
        """Schedule ``coro`` and return a future that resolves with its result."""

    def call_at(self, when: float, task: Any) -> Any:
        """Schedule ``task`` to run at absolute monotonic time ``when``."""

    def call_later(self, delay: float, task: Any) -> Any:
        """Schedule ``task`` to run after ``delay`` seconds."""

    def cancel(self, task: Any) -> None:
        """Cancel a scheduled task/timer previously returned by call_*."""

    # --- awaitable primitives (core coroutines only await these) ----------
    def sleep(self, delay: float) -> Any:
        """Awaitable that resolves after ``delay`` seconds."""

    def wait_read(self, fileobj: Any, timeout_at: Optional[float] = None) -> Any:
        """Awaitable that resolves when ``fileobj`` is readable."""

    def wait_write(self, fileobj: Any, timeout_at: Optional[float] = None) -> Any:
        """Awaitable that resolves when ``fileobj`` is writable."""

    # --- cross-thread bridge ---------------------------------------------
    def run(self, coro: Any, *args: Any) -> Any:
        """Schedule ``coro`` on the loop, block the calling thread, return/raise.

        Raises ``RuntimeError`` if called from the IO thread itself.
        """

    # --- future factory ---------------------------------------------------
    def create_future(self) -> BackendFuture:
        """Create a loop-awaitable future (see ``BackendFuture``)."""

    # --- misc -------------------------------------------------------------
    def wakeup(self) -> None:
        """Interrupt the loop's select() from another thread."""

    def unregister_event(self, fileobj: Any, event: Any) -> None:
        """Drop a registered read/write interest (transport cleanup path)."""
