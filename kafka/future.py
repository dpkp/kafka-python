from kafka.common import RetriableError, IllegalStateError


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
        return isinstance(self.exception, RetriableError)

    def success(self, value):
        if self.is_done:
            raise IllegalStateError('Invalid attempt to complete a request future which is already complete')
        self.value = value
        self.is_done = True
        for f in self._callbacks:
            f(value)
        return self

    def failure(self, e):
        if self.is_done:
            raise IllegalStateError('Invalid attempt to complete a request future which is already complete')
        self.exception = e
        self.is_done = True
        for f in self._errbacks:
            f(e)
        return self

    def add_callback(self, f):
        if self.is_done and not self.exception:
            f(self.value)
        else:
            self._callbacks.append(f)
        return self

    def add_errback(self, f):
        if self.is_done and self.exception:
            f(self.exception)
        else:
            self._errbacks.append(f)
        return self
