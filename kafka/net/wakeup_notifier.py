import weakref

from kafka.future import Future


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

    def _wakeup(self):
        if self._fut is not None and not self._fut.is_done:
            self._fut.success(None)
        else:
            self._pending = True

    async def __call__(self, timeout_secs=None):
        if self._fut is not None:
            raise RuntimeError('Concurrent access to WakeupNotifier!')
        self._fut = Future()
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
            if timer is not None and not timer.is_done:
                try:
                    self._net.unschedule(timer)
                except (ValueError, RuntimeError, ReferenceError):
                    pass

    def notify(self):
        # Always queue _wakeup on the IO thread. Skipping the queue when
        # ``self._fut is None`` would re-introduce the lost-wakeup race:
        # the check could pass before another thread enters ``__call__``
        # and creates the future. Routing through the IO thread is one
        # call_soon_threadsafe (~microseconds) and lets ``_wakeup`` decide
        # under single-threaded semantics whether to signal or latch.
        try:
            self._net.call_soon_threadsafe(self._wakeup)
        except ReferenceError:
            pass
