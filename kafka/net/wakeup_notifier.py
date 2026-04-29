import weakref

from kafka.future import Future


class WakeupNotifier:
    """await wakeup(timeout_secs) when either ``timeout_secs`` elapses or
    notify() is called -- whichever first. The notifier is safe to call
    from any thread (it routes through call_soon_threadsafe).

    notify() is a noop unless there is a task currently waiting.

    Used by the metadata refresh loop to sleep on its TTL while remaining
    interruptible by external callers (e.g. KafkaProducer / KafkaConsumer
    invoking cluster.request_update() from another thread).
    """
    def __init__(self, net):
        self._net = weakref.proxy(net)
        self._fut = None

    def _wakeup(self):
        if self._fut is not None and not self._fut.is_done:
            self._fut.success(None)

    async def __call__(self, timeout_secs=None):
        self._fut = Future()
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
        if self._fut is not None:
            try:
                self._net.call_soon_threadsafe(self._wakeup)
            except ReferenceError:
                pass
