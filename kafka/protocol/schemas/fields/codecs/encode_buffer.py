import threading


class EncodeBuffer:
    """Growable byte buffer for the ``encode_into`` fast path.

    The encoders write primitives directly into ``buf`` at offset ``pos``
    rather than building and joining intermediate ``bytes`` objects. The
    buffer starts at a fixed size and grows on demand via :meth:`ensure`.

    Capacity contract
    -----------------
    Writing past the end of ``buf`` does not grow it automatically:

      * single-byte index writes (``buf[pos] = x``) raise ``IndexError``,
      * ``pack_into`` raises ``struct.error``,
      * slice assignment (``buf[pos:pos+n] = data``) silently *resizes* the
        bytearray, defeating the preallocation.

    Therefore **every writer must call** ``ensure(n)`` to reserve ``n`` bytes
    before writing ``n`` bytes at ``pos`` (where ``n`` is the maximum the write
    can consume - e.g. ``5`` for a varint32, a fixed field's ``size``, or
    ``len(payload)`` for variable data). See the codecs in ``types.py`` for the
    pattern, and ``CodegenContext.emit_reserve`` for the compiled equivalent.

    Reallocation note
    -----------------
    ``ensure`` may replace ``buf`` with a larger bytearray. Any caller (or
    generated code) that caches ``buf`` in a local **must re-read** ``self.buf``
    after a call that can grow it - including indirect growth through a nested
    ``encode_into`` / ``ensure``. Forgetting to re-read leaves writes targeting
    the old, discarded buffer (silent data loss) or raises out of range.
    """
    __slots__ = ('buf', 'pos')

    def __init__(self, size=65536):
        self.buf = bytearray(size)
        self.pos = 0

    def reset(self):
        """Reset position to 0 for reuse. The buffer retains its current size."""
        self.pos = 0

    def ensure(self, needed):
        """Guarantee at least ``needed`` writable bytes remain at ``pos``.

        Call this *before* writing ``needed`` bytes at ``self.pos``. If the
        current buffer cannot hold them it is grown (at least doubled) and the
        existing ``[:pos]`` content is preserved.

        WARNING: this may rebind ``self.buf`` to a new bytearray, so re-read
        ``self.buf`` afterwards if you hold a local reference to it (see the
        class docstring).
        """
        if self.pos + needed > len(self.buf):
            new_size = max(len(self.buf) * 2, self.pos + needed)
            new_buf = bytearray(new_size)
            new_buf[:self.pos] = self.buf[:self.pos]
            self.buf = new_buf

    def result(self):
        """Return the encoded bytes written so far (``buf[:pos]``)."""
        # Return a bytearray slice (one copy) rather than bytes(self.buf[:pos])
        # (two copies - the slice creates a bytearray, then bytes() copies
        # again). Downstream consumers (protocol codec slice assignment,
        # socket.send) accept bytearray transparently.
        return self.buf[:self.pos]


class EncodeBufferPool:
    """Thread-local pool of reusable EncodeBuffer objects.

    Each thread gets its own buffer that grows to match the largest message
    encoded on that thread and stays that size - avoiding repeated allocation
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
        # Nothing to do - the buffer stays on the thread-local.
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
