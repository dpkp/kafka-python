import functools
import logging
import threading
from typing import Any, Callable, Protocol, runtime_checkable

from kafka.errors import RetriableError

log = logging.getLogger(__name__)


class Future:
    """Callback/errback future, and the reference implementation of the
    :class:`BackendFuture` contract.

    ``Future`` owns the portable callback core (``success`` / ``failure`` /
    ``add_callback`` / ``add_errback`` / ``add_both`` / ``chain`` and the
    ``is_done`` / ``value`` / ``exception`` state). A pluggable async backend
    returns its own awaitable from ``net.create_future()`` by subclassing
    ``Future`` and overriding only :meth:`__await__` — the single
    backend-specific hook. The selector backend uses ``Future`` directly
    (``__await__`` yields ``self``; the selector resumes the task on
    resolution). See :class:`BackendFuture` for the full contract.
    """
    __slots__ = ('is_done', 'value', 'exception', '_callbacks', '_errbacks', '_lock')
    error_on_callbacks = False # and errbacks

    def __init__(self):
        self.is_done = False
        self.value = None
        self.exception = None
        self._callbacks = []
        self._errbacks = []
        self._lock = threading.Lock()

    def succeeded(self):
        return self.is_done and self.exception is None

    def failed(self):
        return self.is_done and self.exception is not None

    def retriable(self):
        return self.is_done and isinstance(self.exception, RetriableError)

    def success(self, value):
        # Hot path: called once per produced record via the sender thread's
        # batch-completion callback chain. Kept tight: explicit acquire/
        # release (cheaper than `with`), callbacks snapshot under the lock,
        # dispatch outside the lock, inlined callback loop (avoids an extra
        # Python frame per completion).
        # Clearing the lists releases any reference cycle held via stored
        # bound methods (e.g. FutureProduceResult<->FutureRecordMetadata).
        lock = self._lock
        lock.acquire()
        self.value = value
        self.is_done = True
        callbacks = self._callbacks
        self._callbacks = None
        self._errbacks = None
        lock.release()
        if callbacks:
            error_on_callbacks = self.error_on_callbacks
            for f in callbacks:
                try:
                    f(value)
                except Exception as e:
                    log.exception('Error processing callback')
                    if error_on_callbacks:
                        raise e
        return self

    def failure(self, e):
        exception = e if type(e) is not type else e()
        if not isinstance(exception, BaseException):
            raise TypeError('future failed without an exception')
        lock = self._lock
        lock.acquire()
        self.exception = exception
        self.is_done = True
        errbacks = self._errbacks
        self._callbacks = None
        self._errbacks = None
        lock.release()
        if errbacks:
            error_on_callbacks = self.error_on_callbacks
            for f in errbacks:
                try:
                    f(exception)
                except Exception as err:
                    log.exception('Error processing errback')
                    if error_on_callbacks:
                        raise err
        return self

    def add_callback(self, f, *args, **kwargs):
        if args or kwargs:
            f = functools.partial(f, *args, **kwargs)
        lock = self._lock
        lock.acquire()
        if not self.is_done:
            self._callbacks.append(f)
            lock.release()
            return self
        lock.release()
        if self.exception is None:
            try:
                f(self.value)
            except Exception as e:
                log.exception('Error processing callback')
                if self.error_on_callbacks:
                    raise e
        return self

    def add_errback(self, f, *args, **kwargs):
        if args or kwargs:
            f = functools.partial(f, *args, **kwargs)
        lock = self._lock
        lock.acquire()
        if not self.is_done:
            self._errbacks.append(f)
            lock.release()
            return self
        lock.release()
        if self.exception is not None:
            try:
                f(self.exception)
            except Exception as e:
                log.exception('Error processing errback')
                if self.error_on_callbacks:
                    raise e
        return self

    def _add_cb_eb(self, cb, eb):
        """Register a (callback, errback) pair under a single lock acquire.

        Fast path for call sites that always register both a plain callback
        and errback with no ``*args``/``**kwargs``. Used on the producer hot
        path (``FutureRecordMetadata`` -> ``FutureProduceResult``) to halve
        the per-record lock-acquire count vs. calling ``add_callback()`` +
        ``add_errback()`` separately.
        """
        lock = self._lock
        lock.acquire()
        if not self.is_done:
            self._callbacks.append(cb)
            self._errbacks.append(eb)
            lock.release()
            return self
        lock.release()
        if self.exception is None:
            try:
                cb(self.value)
            except Exception as e:
                log.exception('Error processing callback')
                if self.error_on_callbacks:
                    raise e
        else:
            try:
                eb(self.exception)
            except Exception as e:
                log.exception('Error processing errback')
                if self.error_on_callbacks:
                    raise e
        return self

    def add_both(self, f, *args, **kwargs):
        self.add_callback(f, *args, **kwargs)
        self.add_errback(f, *args, **kwargs)
        return self

    def chain(self, future):
        self._add_cb_eb(future.success, future.failure)
        return self

    def __await__(self):
        # The single backend-specific hook (see BackendFuture). For the
        # selector backend the awaitable IS the Future: yield self and the
        # selector re-enqueues the awaiting task when this future resolves.
        # asyncio/Twisted backends override this to bridge to their native
        # awaitable, leaving the callback core above untouched.
        if not self.is_done:
            yield self
        if self.exception:
            raise self.exception
        return self.value


@runtime_checkable
class BackendFuture(Protocol):
    """Contract for the awaitable futures returned by ``net.create_future()``.

    A pluggable async backend (the kafka.net selector, asyncio, Twisted, ...)
    returns its own future type from ``create_future()``. Core loop coroutines
    touch it only through this surface, so the type is interchangeable across
    backends. :class:`Future` is the reference implementation and the base
    class backends subclass, overriding only ``__await__``.

    Pinned semantics — the three axes where backends could otherwise diverge:

    1. **Resolution thread.** A future from ``create_future()`` is created and
       resolved (``success`` / ``failure``) on the loop/IO thread only.
       Cross-thread handoffs (a user thread blocking on a loop result) use a
       plain thread-safe ``Future`` bridged via ``manager.wait_for`` /
       ``manager.run`` — never a backend future awaited directly. Backends
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
