from __future__ import absolute_import

import functools
import logging
import threading

log = logging.getLogger(__name__)


class Future(object):
    error_on_callbacks = False # and errbacks

    def __init__(self):
        self.is_done = False
        self.value = None
        self.exception = None
        self._callbacks = []
        self._errbacks = []
        self._lock = threading.Lock()

    def succeeded(self):
        return self.is_done and not bool(self.exception)

    def failed(self):
        return self.is_done and bool(self.exception)

    def retriable(self):
        try:
            return self.exception.retriable
        except AttributeError:
            return False

    def success(self, value):
        assert not self.is_done, 'Future is already complete'
        with self._lock:
            self.value = value
            self.is_done = True
        if self._callbacks:
            self._call_backs('callback', self._callbacks, self.value)
        return self

    def failure(self, e):
        assert not self.is_done, 'Future is already complete'
        exception = e if type(e) is not type else e()
        assert isinstance(exception, BaseException), (
            'future failed without an exception')
        with self._lock:
            self.exception = exception
            self.is_done = True
        self._call_backs('errback', self._errbacks, self.exception)
        return self

    def add_callback(self, f, *args, **kwargs):
        if args or kwargs:
            f = functools.partial(f, *args, **kwargs)
        with self._lock:
            if not self.is_done:
                self._callbacks.append(f)
            elif self.succeeded():
                self._lock.release()
                self._call_backs('callback', [f], self.value)
                self._lock.acquire()
        return self

    def add_errback(self, f, *args, **kwargs):
        if args or kwargs:
            f = functools.partial(f, *args, **kwargs)
        with self._lock:
            if not self.is_done:
                self._errbacks.append(f)
            elif self.failed():
                self._lock.release()
                self._call_backs('errback', [f], self.exception)
                self._lock.acquire()
        return self

    def add_both(self, f, *args, **kwargs):
        self.add_callback(f, *args, **kwargs)
        self.add_errback(f, *args, **kwargs)
        return self

    def chain(self, future):
        self.add_callback(future.success)
        self.add_errback(future.failure)
        return self

    def _call_backs(self, back_type, backs, value):
        for f in backs:
            try:
                f(value)
            except Exception as e:
                log.exception('Error processing %s', back_type)
                if self.error_on_callbacks:
                    raise e
