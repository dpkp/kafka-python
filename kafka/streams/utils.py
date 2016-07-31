import threading


class AtomicInteger(object):
    def __init__(self, val=0):
        self._lock = threading.Lock()
        self._val = val

    def increment(self):
        with self._lock:
            self._val += 1
            return self._val

    def decrement(self):
        with self._lock:
            self._val -= 1
            return self._val

    def get(self):
        return self._val
