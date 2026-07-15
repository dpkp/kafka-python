"""The pluggable async-backend contract.

``NetBackend`` is the interface the rest of kafka-python depends on for its
event loop: the surface that ``KafkaProducer`` / ``KafkaConsumer`` /
``KafkaAdminClient`` (and the manager, cluster, connection, coordinator,
fetcher, sender) reach for through ``self._net`` / ``manager._net``.

``NetworkSelector`` (``kafka/net/selector.py``) is the reference
implementation; an asyncio backend (and eventually Twisted) implements the
same surface so it can be swapped in via ``net=`` without touching core code.

The :class:`NetBackendFuture` contract is the surface of the
loop-awaitable futures a backend hands out from ``net.create_future()``. The
selector's implementation is ``kafka.net.selector.SelectorFuture``; an asyncio
(and eventually Twisted) backend supplies its own.

Networking is a **connection seam**, not fd-readiness. asyncio and Twisted own
the socket (DNS, connect, TLS, buffering) and drive protocol callbacks; they do
not expose portable fd-readiness (asyncio's Proactor loop has no ``add_reader``,
Twisted never exposes arbitrary-fd readiness). So the backend provides
``create_connection`` -- given a ``KafkaConnection`` (which already implements
the ``asyncio.Protocol`` surface) and an endpoint, it establishes the transport
and wires the two together. The returned object satisfies the small
:class:`Transport` protocol (the subset ``KafkaConnection`` drives).

Three things are intentionally **not** part of the contract:

* ``wait_read`` / ``wait_write`` / ``unregister_event`` -- the low-level
  fd-readiness primitives. They are the *selector's* private mechanism (used
  only inside ``kafka/net/transport.py`` + ``inet.py``, zero core callers) and
  do not port to asyncio/Twisted. The connection seam replaces them.
* ``poll(timeout_ms, future=...)`` -- the legacy single-tick driver. Its only
  remaining caller is the ``KafkaNetClient`` compat shim
  (``kafka/net/compat.py``), which is selector-bound; asyncio has no bounded
  single-tick equivalent.
* ``register_event`` / selector internals -- backend-private plumbing.

Method families:

* **Lifecycle** -- ``start`` / ``stop`` / ``close`` / ``on_io_thread``.
* **Scheduling** -- ``call_soon`` / ``call_soon_threadsafe`` /
  ``call_soon_with_future`` / ``call_at`` / ``call_later`` / ``cancel``.
* **Timing** -- ``sleep`` (backend-specific awaitable; core coroutines await it).
* **Connection** -- ``create_connection`` (returns a :class:`Transport`).
* **Cross-thread bridge** -- ``run`` (schedule on the loop, block the caller).
* **Future factory** -- ``create_future`` (see ``NetBackendFuture``).
* **Cross-thread wake** -- ``wakeup``.
"""
import importlib
from typing import Any, Callable, Optional, Protocol, Sequence, Tuple, runtime_checkable


@runtime_checkable
class NetBackendFuture(Protocol):
    """Contract for the awaitable futures returned by ``net.create_future()``.

    A pluggable async backend (the kafka.net selector, asyncio, Twisted, ...)
    returns its own future type from ``create_future()``. Core loop coroutines
    touch it only through this surface, so the type is interchangeable across
    backends. The selector's ``SelectorFuture`` is the reference implementation:
    it subclasses the thread-safe ``kafka.future.Future`` (the portable callback
    core) and adds ``__await__``. A plain ``Future`` is deliberately NOT a
    NetBackendFuture -- it has no ``__await__`` -- so awaiting a cross-thread
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
    def success(self, value: Any) -> 'NetBackendFuture': ...
    def failure(self, e: Any) -> 'NetBackendFuture': ...
    def add_callback(self, f: Callable, *args: Any, **kwargs: Any) -> 'NetBackendFuture': ...
    def add_errback(self, f: Callable, *args: Any, **kwargs: Any) -> 'NetBackendFuture': ...
    def add_both(self, f: Callable, *args: Any, **kwargs: Any) -> 'NetBackendFuture': ...
    def chain(self, future: 'NetBackendFuture') -> 'NetBackendFuture': ...
    def succeeded(self) -> bool: ...
    def failed(self) -> bool: ...


@runtime_checkable
class NetTransport(Protocol):
    """The transport surface used by NetProtocol / NetBackend.

    The subset of the ``asyncio.Transport`` / Twisted ``ITransport`` surface
    that ``KafkaConnection`` actually drives (plus ``last_activity``, which the
    manager reads to sweep idle connections). A backend's ``create_connection``
    builds one and wires it to the conn. The selector's ``KafkaTCPTransport``
    and an asyncio-transport adapter both satisfy it.
    """
    # Monotonic timestamp of the last read/write; used for idle-sweeping
    last_activity: float

    def write(self, data: bytes) -> None: ...
    def close(self) -> None: ...
    def abort(self, error: Any = None) -> None: ...
    def is_closing(self) -> bool: ...
    def pause_reading(self) -> None: ...
    def resume_reading(self) -> None: ...
    def host_port(self) -> Tuple[str, int]: ...
    def get_protocol(self) -> 'NetProtocol': ...
    def set_protocol(self, protocol: 'NetProtocol') -> None: ...


@runtime_checkable
class NetProtocol(Protocol):
    # Monotonic timestamp of the last read/write; used for idle-sweeping
    last_activity: float

    def connection_made(self, transport: NetTransport) -> None: ...
    def data_received(self, data: bytes) -> None: ...
    def connection_lost(self, exc: Any = None) -> None: ...
    def pause_writing(self) -> None: ...
    def resume_writing(self) -> None: ...


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

    def call_soon_with_future(self, coro: Any, *args: Any) -> NetBackendFuture:
        """Schedule ``coro`` and return a future that resolves with its result."""

    def call_at(self, when: float, task: Any) -> Any:
        """Schedule ``task`` to run at absolute monotonic time ``when``."""

    def call_later(self, delay: float, task: Any) -> Any:
        """Schedule ``task`` to run after ``delay`` seconds."""

    def cancel(self, task: Any) -> None:
        """Cancel a scheduled task/timer previously returned by call_*."""

    # --- timing (core coroutines await this) ------------------------------
    def sleep(self, delay: float) -> Any:
        """Awaitable that resolves after ``delay`` seconds."""

    # --- connection seam --------------------------------------------------
    async def create_connection(
        self,
        protocol: NetProtocol,
        host: str,
        port: int,
        *,
        ssl: Any = None,
        proxy_url: Optional[str] = None,
        socket_options: Sequence[Any] = (),
        timeout_at: Optional[float] = None,
    ) -> None:
        """Establish a connected :class:`Transport` to ``host:port`` and wire it.

        The backend owns DNS, connect, TLS and (where supported) proxying, builds
        a :class:`Transport`, and wires it to ``protocol`` by calling
        ``protocol.connection_made(transport)`` itself -- mirroring
        ``asyncio.loop.create_connection`` / Twisted, which own the socket and
        wire the protocol at connect time. Nothing is returned: the caller drives
        the connection through ``protocol`` (``conn.transport``), never a
        transport handle. ``protocol`` (a ``KafkaConnection``) may *refuse* the
        transport by raising from ``connection_made`` if it closed mid-connect;
        on that (or any) failure the backend closes the orphaned transport
        before propagating. Backends without native proxy support raise when
        ``proxy_url`` is set.
        """

    # --- cross-thread bridge ---------------------------------------------
    def run(self, coro: Any, *args: Any, timeout_ms: Optional[float] = None) -> Any:
        """Schedule ``coro`` on the loop, block the calling thread, return/raise.

        The blocking wait is bounded: if the coroutine does not complete within
        ``timeout_ms`` (or the backend's ``default_api_timeout_ms`` when None),
        plus a grace margin, ``KafkaTimeoutError`` is raised as a liveness
        backstop against a stalled IO loop.

        Raises ``RuntimeError`` if called from the IO thread itself.
        """

    # --- future factory ---------------------------------------------------
    def create_future(self) -> NetBackendFuture:
        """Create a loop-awaitable future (see ``NetBackendFuture``)."""

    # --- misc -------------------------------------------------------------
    def wakeup(self) -> None:
        """Interrupt the loop's select() from another thread."""


# --- backend selection ----------------------------------------------------

# name -> factory(**config) -> NetBackend. Populated by register_backend();
# 'selector' is always available, 'asyncio' registers itself in Step 4.
_BACKENDS = {}


def register_backend(name, factory):
    """Register a named backend factory for ``net='<name>'`` selection."""
    _BACKENDS[name] = factory


def register_backend_lazy(name, module, klass):
    """Lazy register a factory klass from module. Import is deferred until first use."""
    def lazy_backend(**configs):
        backend = getattr(importlib.import_module(module), klass)
        register_backend(name, backend)
        return backend(**configs)
    register_backend(name, lazy_backend)


def _detect_async_library():
    """Best-effort name of the async framework the caller is running under.

    Returns 'asyncio' / 'trio' / ... via sniffio if installed, else 'asyncio'
    if a running asyncio loop is detected, else None (plain sync context ->
    caller falls back to the default backend).
    """
    try:
        import sniffio
        try:
            return sniffio.current_async_library()
        except sniffio.AsyncLibraryNotFoundError:
            pass
    except ImportError:
        pass
    try:
        import asyncio
        asyncio.get_running_loop()
        return 'asyncio'
    except RuntimeError:
        return None


def resolve_backend(net, config):
    """Resolve the ``net`` config value to a concrete NetBackend instance.

    Precedence:
      1. an already-constructed NetBackend instance -> used as-is;
      2. a string name -> looked up in the registry (raises if unknown);
      3. None -> auto-detect a running async framework (sniffio / running
         asyncio loop) and use that backend *if registered*, otherwise fall
         back to the default ``NetworkSelector``.

    Note (Phase 1): auto-detect only selects the *implementation*; the backend
    still runs on its own IO thread and the public API still blocks the caller.
    """
    if isinstance(net, str):
        try:
            factory = _BACKENDS[net]
        except KeyError:
            raise ValueError('Unknown net backend %r (available: %s)'
                             % (net, sorted(_BACKENDS)))
        return factory(**config)
    if net is not None:
        if not isinstance(net, NetBackend):
            raise TypeError('net must be a NetBackend instance, a backend name, '
                            'or None; got %r' % (net,))
        return net
    # net is None: auto-detect, else default. Auto-detected-but-unregistered
    # backends fall back silently (an explicit name would have raised above).
    name = _detect_async_library()
    if name is None or name not in _BACKENDS:
        name = 'selector'
    return _BACKENDS[name](**config)


register_backend_lazy('selector', 'kafka.net.selector', 'NetworkSelector')
register_backend_lazy('asyncio', 'kafka.net.asyncio_backend', 'AsyncioBackend')
