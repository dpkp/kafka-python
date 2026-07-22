"""An asyncio-backed NetBackend (Phase 1: own daemon thread).

Runs a private ``asyncio`` event loop on a dedicated daemon thread, mirroring
``NetworkSelector``'s threading model, and implements the ``NetBackend``
contract on top of asyncio primitives. Selected via ``net='asyncio'`` or
auto-detected when constructed inside a running asyncio loop (see
``kafka.net.backend.resolve_backend``).

Phase 1 preserves the synchronous public API: ``run()`` blocks the calling
thread on the loop thread; it does not run on the caller's own loop.
"""
import asyncio
import inspect
import threading
import time

import kafka.errors as Errors
from kafka.future import Future


class AsyncioFuture(Future):
    """``create_future()`` result for the asyncio backend.

    Inherits ``kafka.future.Future``'s callback core and overrides only
    ``__await__`` to bridge to an ``asyncio.Future`` so an asyncio Task can
    await it. Per the NetBackendFuture contract this is created and resolved on
    the loop thread only; a fresh asyncio.Future is minted per awaiter, so
    fan-out (multiple awaiters / callbacks) is preserved.
    """
    __slots__ = ('_loop',)

    def __init__(self, loop):
        super().__init__()
        self._loop = loop

    def __await__(self):
        if not self.is_done:
            aio = self._loop.create_future()

            def _resolve(_=None):
                if aio.done():
                    return
                if self.exception is not None:
                    aio.set_exception(self.exception)
                else:
                    aio.set_result(self.value)

            self.add_both(_resolve)
            yield from aio.__await__()
        if self.exception:
            raise self.exception
        return self.value


class _DeferredHandle:
    """Cancelable handle for a timer/callback armed cross-thread.

    ``call_later``/``call_soon`` invoked off the loop thread schedule the real
    handle via ``call_soon_threadsafe``; this box lets the caller ``cancel()``
    synchronously whether or not the real handle has been armed yet.
    """
    __slots__ = ('_handle', '_cancelled')

    def __init__(self):
        self._handle = None
        self._cancelled = False

    def _arm(self, handle):
        if self._cancelled:
            handle.cancel()
        else:
            self._handle = handle

    def cancel(self):
        self._cancelled = True
        if self._handle is not None:
            self._handle.cancel()


class AsyncioBackend:
    def __init__(self, *, loop=None, loop_factory=None, **configs):
        # Loop acquisition is a strategy, not a hardcode (forward seam for a
        # native/Phase-2 mode). Default: own a fresh loop on our daemon thread.
        # loop_factory lets callers pick the loop implementation (e.g. uvloop)
        # while keeping the owned-thread model. loop= injects an existing loop
        # (not owned -- close() won't close it); in Phase 1 it is still run on
        # our own thread. True native reuse of a *running* caller loop is Phase 2.
        if loop is not None:
            self._loop = loop
            self._owns_loop = False
        else:
            self._loop = (loop_factory or asyncio.new_event_loop)()
            self._owns_loop = True
        self._io_thread = None
        self._closed = False
        self._client_id = configs.get('client_id') or 'kafka-python'
        # See NetworkSelector: default operation deadline + grace margin for the
        # cross-thread run() liveness backstop (#3121).
        self._default_api_timeout_ms = configs.get('default_api_timeout_ms') or 60000
        self._bridge_grace_ms = configs.get('bridge_grace_ms')
        if self._bridge_grace_ms is None:
            self._bridge_grace_ms = 5000
        # Strong refs to live tasks (asyncio only holds weak refs, so bare
        # tasks can be GC'd mid-flight); mirrors NetworkSelector._pending_tasks.
        self._pending = set()
        # Cross-thread run() waiters, failed on stop() so callers don't hang.
        self._pending_waiters = {}
        self._pending_waiters_lock = threading.Lock()

    # --- lifecycle --------------------------------------------------------
    def start(self):
        if self._io_thread is not None:
            return
        t = threading.Thread(target=self._run_forever,
                             name='kafka-io-%s' % self._client_id, daemon=True)
        self._io_thread = t
        t.start()

    def _run_forever(self):
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def on_io_thread(self):
        return self._io_thread is not None and threading.current_thread() is self._io_thread

    def stop(self, timeout_ms=None):
        if self._io_thread is None:
            return
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._io_thread.join(timeout_ms / 1000 if timeout_ms is not None else None)
        self._io_thread = None
        self._fail_pending_waiters(Errors.KafkaConnectionError('Event loop stopped'))

    def close(self):
        if self._closed:
            return
        self._closed = True
        if self._io_thread is not None:
            self.stop()
        if not self._loop.is_closed():
            pending = [t for t in self._pending if not t.done()]
            for t in pending:
                t.cancel()
            if pending:
                try:
                    self._loop.run_until_complete(
                        asyncio.gather(*pending, return_exceptions=True))
                except RuntimeError:
                    pass
            if self._owns_loop:
                self._loop.close()

    def _fail_pending_waiters(self, exc):
        with self._pending_waiters_lock:
            waiters = list(self._pending_waiters.items())
            self._pending_waiters.clear()
        for event, state in waiters:
            if state['exception'] is None:
                state['exception'] = exc
            event.set()

    # --- scheduling -------------------------------------------------------
    def _spawn(self, coro):
        task = self._loop.create_task(coro)
        self._pending.add(task)
        task.add_done_callback(self._pending.discard)
        return task

    def _schedule(self, task, args=()):
        """Run a coroutine / coroutine-function / callable on the loop thread.

        A plain callable that *returns* a coroutine (e.g. the manager's
        ``call_soon(lambda: self._connect(...))``) has that coroutine run too,
        mirroring NetworkSelector's Task, which steps into returned coroutines.
        """
        if inspect.iscoroutine(task):
            return self._spawn(task)
        if inspect.iscoroutinefunction(task):
            return self._spawn(task(*args))

        def _call():
            result = task(*args)
            if inspect.iscoroutine(result):
                self._spawn(result)
        return self._loop.call_soon(_call)

    def call_soon(self, task):
        # On the loop thread: schedule directly. Off it (or before start()):
        # route through call_soon_threadsafe so create_task/call_soon run on
        # the loop thread as asyncio requires.
        if self.on_io_thread():
            return self._schedule(task)
        box = _DeferredHandle()
        self._loop.call_soon_threadsafe(lambda: box._arm(self._schedule(task)))
        return box

    def call_soon_threadsafe(self, callback):
        if self._closed:
            raise RuntimeError('AsyncioBackend closed!')
        box = _DeferredHandle()
        self._loop.call_soon_threadsafe(lambda: box._arm(self._schedule(callback)))
        return box

    def _as_callback(self, task):
        if inspect.iscoroutinefunction(task):
            return lambda: self._spawn(task())
        if inspect.iscoroutine(task):
            return lambda: self._spawn(task)
        return task

    def call_at(self, when, task):
        # Selector uses time.monotonic()-based absolute `when`; convert to a
        # relative delay for asyncio's loop.time() base.
        return self.call_later(max(0.0, when - time.monotonic()), task)

    def call_later(self, delay, task):
        cb = self._as_callback(task)
        if self.on_io_thread():
            return self._loop.call_later(delay, cb)
        box = _DeferredHandle()
        self._loop.call_soon_threadsafe(
            lambda: box._arm(self._loop.call_later(delay, cb)))
        return box

    def cancel(self, task):
        if task is not None:
            task.cancel()

    def sleep(self, delay):
        return asyncio.sleep(delay)

    def wakeup(self):
        # asyncio has no select() to interrupt from user code; a no-op
        # threadsafe callback is enough to wake a blocked run_forever().
        try:
            self._loop.call_soon_threadsafe(lambda: None)
        except RuntimeError:
            pass

    # --- futures ----------------------------------------------------------
    def create_future(self):
        return AsyncioFuture(self._loop)

    async def _resolve_future(self, fut):
        """Await any kafka.future.Future (plain or AsyncioFuture) to its value."""
        if fut.is_done:
            if fut.exception is not None:
                raise fut.exception
            return fut.value
        aio = self._loop.create_future()

        def _cb(_=None):
            if aio.done():
                return
            if fut.exception is not None:
                aio.set_exception(fut.exception)
            else:
                aio.set_result(fut.value)

        fut.add_both(_cb)
        return await aio

    async def _invoke(self, coro, *args):
        """Invoke coro/awaitable/function and fully resolve the result.

        Mirrors NetworkSelector._invoke, but bridges any trailing kafka Future
        through _resolve_future (a plain Future isn't awaitable under asyncio).
        """
        if inspect.iscoroutinefunction(coro):
            result = await coro(*args)
        elif hasattr(coro, '__await__'):
            result = await coro
        else:
            result = coro(*args)
        if inspect.iscoroutine(result) or hasattr(result, '__await__'):
            result = await result
        while isinstance(result, Future):
            result = await self._resolve_future(result)
        return result

    def call_soon_with_future(self, coro, *args):
        if hasattr(coro, '__await__') and args:
            raise ValueError('initiated coroutine does not accept args')
        future = AsyncioFuture(self._loop)

        async def wrapper():
            try:
                future.success(await self._invoke(coro, *args))
            except BaseException as exc:
                future.failure(exc)

        self.call_soon(wrapper)
        return future

    # --- cross-thread bridge ---------------------------------------------
    def run(self, coro, *args, timeout_ms=None):
        if self._closed:
            raise RuntimeError('AsyncioBackend closed!')
        if self._io_thread is None:
            raise RuntimeError('AsyncioBackend not started; call start() first')
        if self.on_io_thread():
            raise RuntimeError(
                "Cannot block on net.run() from the IO thread itself. "
                "This typically happens when a synchronous rebalance listener "
                "(or another IO-thread callback) calls a blocking consumer/admin API. "
                "Use AsyncConsumerRebalanceListener and await the async variant, "
                "or move the blocking work to a worker thread.")
        op_ms = timeout_ms if timeout_ms is not None else self._default_api_timeout_ms
        deadline_secs = (op_ms + self._bridge_grace_ms) / 1000
        event = threading.Event()
        state = {'value': None, 'exception': None}

        async def waiter():
            try:
                state['value'] = await self._invoke(coro, *args)
            except BaseException as exc:
                if state['exception'] is None:
                    state['exception'] = exc
            finally:
                with self._pending_waiters_lock:
                    self._pending_waiters.pop(event, None)
                event.set()

        with self._pending_waiters_lock:
            self._pending_waiters[event] = state
        self.call_soon(waiter)
        if not event.wait(timeout=deadline_secs):
            # Loop never ran the coroutine to completion; leave the waiter
            # registered (its finally pops it) and surface a liveness timeout.
            name = getattr(coro, '__name__', None) or repr(coro)
            raise Errors.KafkaTimeoutError(
                'net.run(%s) did not complete within %d ms (+%d ms grace). The '
                'IO event loop may be stalled by blocking work on the IO thread '
                '(e.g. a synchronous rebalance listener/assignor).'
                % (name, op_ms, self._bridge_grace_ms))
        if state['exception'] is not None:
            raise state['exception']  # pylint: disable=raising-bad-type
        return state['value']

    # --- connection seam --------------------------------------------------
    async def getaddrinfo(self, host, port):
        return await self._loop.getaddrinfo(host, port)

    async def create_connection(self, protocol, host, port, *, ssl=None,
                                socket_options=(), timeout_at=None):
        server_hostname = host.rstrip('.') if ssl is not None else None
        adapter = _AsyncioProtocolAdapter(protocol, host, port, socket_options)
        connect = self._loop.create_connection(
            lambda: adapter, host, port, ssl=ssl, server_hostname=server_hostname)
        try:
            if timeout_at is not None:
                connect = asyncio.wait_for(connect, max(0.0, timeout_at - time.monotonic()))
            await connect
            if adapter.error is not None:
                raise adapter.error
        except asyncio.TimeoutError:
            raise Errors.KafkaConnectionError('Connection timed out')
        except Errors.KafkaError:
            raise
        except Exception as exc:  # noqa: BLE001  -- surface any connect error uniformly
            raise Errors.KafkaConnectionError('unable to connect to %s:%s: %s' % (host, port, exc))


class _AsyncioProtocolAdapter(asyncio.Protocol):
    """Thin asyncio.Protocol that wires a KafkaConnection to a wrapped transport."""

    def __init__(self, conn, host, port, socket_options=()):
        self._conn = conn
        self._host = host
        self._port = port
        self._socket_options = socket_options
        self.error = None
        self.transport = None       # the _AsyncioTransport wrapper

    def connection_made(self, aio_transport):
        sock = aio_transport.get_extra_info('socket')
        if sock is not None:
            for option in self._socket_options:
                try:
                    sock.setsockopt(*option)
                except OSError:
                    pass
        self.transport = _AsyncioTransport(aio_transport, self._host, self._port)
        try:
            self._conn.connection_made(self.transport)
        except Exception as exc:  # noqa: BLE001  -- conn refused (closed mid-connect)
            self.error = exc
            aio_transport.abort()

    def data_received(self, data):
        self.transport._bump_read()
        self._conn.data_received(data)

    def connection_lost(self, exc):
        self._conn.connection_lost(exc)

    def pause_writing(self):
        self._conn.pause_writing()

    def resume_writing(self):
        self._conn.resume_writing()


class _AsyncioTransport:
    """Transport returned by AsyncioBackend.create_connection.

    Wraps an asyncio transport + its protocol adapter and exposes the surface
    KafkaConnection / the manager drive (a superset of the NetBackend Transport
    protocol): write/close/abort/is_closing, pause/resume_reading, set/get
    protocol, host/host_port/get_peer, and last_activity for idle sweeping.
    """

    def __init__(self, transport, host, port):
        self._t = transport
        self.host = host
        self._port = port
        self._protocol = None
        self.last_write = time.monotonic()
        self.last_read = time.monotonic()

    @property
    def last_activity(self):
        return max(self.last_write, self.last_read)

    def _bump_read(self):
        self.last_read = time.monotonic()

    def get_protocol(self):
        return self._protocol

    def set_protocol(self, protocol):
        self._protocol = protocol

    def write(self, data):
        self.last_write = time.monotonic()
        self._t.write(data)
        return len(data)

    def close(self):
        self._t.close()

    def abort(self, error=None):
        self._t.abort()

    def is_closing(self):
        return self._t.is_closing()

    def pause_reading(self):
        try:
            self._t.pause_reading()
        except (RuntimeError, AttributeError):
            pass

    def resume_reading(self):
        try:
            self._t.resume_reading()
        except (RuntimeError, AttributeError):
            pass

    def get_peer(self):
        peer = self._t.get_extra_info('peername')
        return peer if peer is not None else (self.host, self._port)

    def host_port(self):
        return '%s:%s' % (self.host, self._port)
