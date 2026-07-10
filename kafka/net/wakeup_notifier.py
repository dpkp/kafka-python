import weakref


class WakeupNotifier:
    """await wakeup(timeout_secs) when either ``timeout_secs`` elapses or
    notify() is called -- whichever first. The notifier is safe to call
    from any thread (it routes through call_soon_threadsafe).

    Level-triggered: notify() arriving while no one is awaiting is latched
    and consumed by the next ``__call__``. This closes a lost-wakeup race
    where a caller's state mutation (e.g. ``cluster._need_update = True``)
    and its ``notify()`` happen between another task's pre-await state
    check and its ``await self._wakeup(...)``. Without latching, the
    notification arrives at the IO thread before the task has registered
    a future to signal, and the task would sleep for the full timeout
    despite work being ready.

    Used by the metadata refresh loop to sleep on its TTL while remaining
    interruptible by external callers (e.g. KafkaProducer / KafkaConsumer
    invoking cluster.request_update() from another thread).
    """
    def __init__(self, net):
        self._net = weakref.proxy(net)
        self._fut = None
        # Set by ``_wakeup`` when no awaiter is registered; consumed by the
        # next ``__call__``. All accesses run on the IO thread (notify
        # routes through call_soon_threadsafe), so no lock is needed.
        self._pending = False
        # Coalescing guard: True once a ``_wakeup`` has been scheduled via
        # ``notify()`` but has not yet run on the IO thread. Lets ``notify()``
        # skip the redundant ``call_soon_threadsafe`` (Task alloc + socketpair
        # write + selector wakeup) when a wake is already in flight. Set on
        # user threads, cleared by ``_wakeup`` on the IO thread; cross-thread
        # access is GIL-atomic and the check-then-set in ``notify()`` can at
        # worst schedule one redundant wake, never drop one (see ``notify``).
        self._scheduled = False

    def _wakeup(self):
        # Clear the coalescing guard first so a notify() racing with this
        # callback schedules a fresh wake rather than being dropped.
        self._scheduled = False
        if self._fut is not None and not self._fut.is_done:
            self._fut.success(None)
        else:
            self._pending = True

    async def __call__(self, timeout_secs=None):
        if self._fut is not None:
            raise RuntimeError('Concurrent access to WakeupNotifier!')
        self._fut = self._net.create_future()
        if self._pending:
            self._pending = False
            self._fut.success(None)
        if timeout_secs is not None:
            try:
                timer = self._net.call_later(timeout_secs, self._wakeup)
            except ReferenceError:
                return
        else:
            timer = None
        try:
            await self._fut
        finally:
            self._fut = None
            if timer is not None:
                self._net.cancel(timer)

    def notify(self):
        # Coalesce: if a _wakeup is already scheduled and not yet consumed,
        # skip. The state this notify() announces was mutated before this
        # call, and the already-pending _wakeup runs (and drains) later, so it
        # will observe that state -- no lost wakeup. We deliberately do NOT
        # skip based on ``self._fut is None``. ``_scheduled`` is cleared by
        # ``_wakeup`` the instant it runs, so a True value guarantees a wake
        # is still pending. The check-then-set is GIL-atomic per access; a
        # lost race between two threads only schedules one redundant wake.
        if self._scheduled:
            return
        self._scheduled = True
        try:
            self._net.call_soon_threadsafe(self._wakeup)
        except ReferenceError:
            self._scheduled = False
