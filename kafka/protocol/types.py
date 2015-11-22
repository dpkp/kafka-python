from struct import pack


class AbstractField(object):
    def __init__(self, name):
        self.name = name


class Int8(AbstractField):
    @classmethod
    def encode(cls, value):
        return pack('>b', value)


class Int16(AbstractField):
    @classmethod
    def encode(cls, value):
        return pack('>h', value)


class Int32(AbstractField):
    @classmethod
    def encode(cls, value):
        return pack('>i', value)


class Int64(AbstractField):
    @classmethod
    def encode(cls, value):
        return pack('>q', value)


class String(AbstractField):
    @classmethod
    def encode(cls, value):
        if value is None:
            return Int16.encode(-1)
        else:
            return Int16.encode(len(value)) + value


class Bytes(AbstractField):
    @classmethod
    def encode(cls, value):
        if value is None:
            return Int32.encode(-1)
        else:
            return Int32.encode(len(value)) + value


class Array(object):
    @classmethod
    def encode(cls, values):
        # Assume that values are already encoded
        return Int32.encode(len(values)) + b''.join(values)
