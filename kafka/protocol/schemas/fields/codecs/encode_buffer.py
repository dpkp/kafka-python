import threading


class EncodeBuffer:
    """Growable buffer for encode_into operations."""
    __slots__ = ('buf', 'pos')

    def __init__(self, size=65536):
        self.buf = bytearray(size)
        self.pos = 0

    def reset(self):
        """Reset position to 0 for reuse. The buffer retains its current size."""
        self.pos = 0

    def ensure(self, needed):
        if self.pos + needed > len(self.buf):
            new_size = max(len(self.buf) * 2, self.pos + needed)
            new_buf = bytearray(new_size)
            new_buf[:self.pos] = self.buf[:self.pos]
            self.buf = new_buf

    def result(self):
        # Return a bytearray slice (one copy) rather than bytes(self.buf[:pos])
        # (two copies — the slice creates a bytearray, then bytes() copies
        # again). Downstream consumers (protocol codec slice assignment,
        # socket.send) accept bytearray transparently.
        return self.buf[:self.pos]


class EncodeBufferPool:
    """Thread-local pool of reusable EncodeBuffer objects.

    Each thread gets its own buffer that grows to match the largest message
    encoded on that thread and stays that size — avoiding repeated allocation
    of large bytearrays.

    Usage:
        with EncodeBufferPool.acquire() as out:
            fast_encode(item, out)
            return out.result()
    """
    _local = threading.local()

    @classmethod
    def acquire(cls):
        """Return a context manager that provides a reset EncodeBuffer."""
        return _PooledBuffer(cls)

    @classmethod
    def _get(cls):
        buf = getattr(cls._local, 'buf', None)
        if buf is None:
            buf = EncodeBuffer()
            cls._local.buf = buf
        buf.reset()
        return buf

    @classmethod
    def _release(cls, buf):
        # Nothing to do — the buffer stays on the thread-local.
        # Future: could cap max size to prevent memory leaks from
        # one-off large messages.
        pass


class _PooledBuffer:
    """Context manager for EncodeBufferPool.acquire()."""
    __slots__ = ('_pool', '_buf')

    def __init__(self, pool):
        self._pool = pool
        self._buf = None

    def __enter__(self):
        self._buf = self._pool._get()
        return self._buf

    def __exit__(self, *exc):
        self._pool._release(self._buf)
        self._buf = None
        return False
