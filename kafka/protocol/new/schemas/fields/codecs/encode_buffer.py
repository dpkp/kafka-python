class EncodeBuffer:
    """Growable buffer for encode_into operations."""
    __slots__ = ('buf', 'pos')

    def __init__(self, size=65536):
        self.buf = bytearray(size)
        self.pos = 0

    def ensure(self, needed):
        if self.pos + needed > len(self.buf):
            new_size = max(len(self.buf) * 2, self.pos + needed)
            new_buf = bytearray(new_size)
            new_buf[:self.pos] = self.buf[:self.pos]
            self.buf = new_buf

    def result(self):
        return bytes(self.buf[:self.pos])
