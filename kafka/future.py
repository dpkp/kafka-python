import functools
import logging

import kafka.common as Errors

log = logging.getLogger(__name__)


class Future(object):
    def __init__(self):
        self.is_done = False
        self.value = None
        self.exception = None
        self._callbacks = []
        self._errbacks = []

    def succeeded(self):
        return self.is_done and not self.exception

    def failed(self):
        return self.is_done and self.exception

    def retriable(self):
        try:
            return self.exception.retriable
        except AttributeError:
            return False

    def success(self, value):
        if self.is_done:
            raise Errors.IllegalStateError('Invalid attempt to complete a'
                                           ' request future which is already'
                                           ' complete')
        self.value = value
        self.is_done = True
        for f in self._callbacks:
            try:
                f(value)
            except Exception:
                log.exception('Error processing callback')
        return self

    def failure(self, e):
        if self.is_done:
            raise Errors.IllegalStateError('Invalid attempt to complete a'
                                           ' request future which is already'
                                           ' complete')
        self.exception = e if type(e) is not type else e()
        self.is_done = True
        for f in self._errbacks:
            try:
                f(e)
            except Exception:
                log.exception('Error processing errback')
        return self

    def add_callback(self, f, *args, **kwargs):
        if args or kwargs:
            f = functools.partial(f, *args, **kwargs)
        if self.is_done and not self.exception:
            f(self.value)
        else:
            self._callbacks.append(f)
        return self

    def add_errback(self, f, *args, **kwargs):
        if args or kwargs:
            f = functools.partial(f, *args, **kwargs)
        if self.is_done and self.exception:
            f(self.exception)
        else:
            self._errbacks.append(f)
        return self

    def add_both(self, f, *args, **kwargs):
        self.add_callback(f, *args, **kwargs)
        self.add_errback(f, *args, **kwargs)
        return self

    def chain(self, future):
        self.add_callback(future.success)
        self.add_errback(future.failure)
        return self
