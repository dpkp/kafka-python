import functools
import logging
import threading

log = logging.getLogger(__name__)


class Future:
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
        try:
            return self.exception.retriable
        except AttributeError:
            return False

    def success(self, value):
        # Hot path: called once per produced record via the sender thread's
        # batch-completion callback chain. Kept tight: explicit acquire/
        # release (cheaper than `with`), callbacks snapshot under the lock,
        # dispatch outside the lock, inlined callback loop (avoids an extra
        # Python frame per completion).
        lock = self._lock
        lock.acquire()
        self.value = value
        self.is_done = True
        callbacks = self._callbacks
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
        assert isinstance(exception, BaseException), (
            'future failed without an exception')
        lock = self._lock
        lock.acquire()
        self.exception = exception
        self.is_done = True
        errbacks = self._errbacks
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
        if not self.is_done:
            yield self
        if self.exception:
            raise self.exception
        return self.value
