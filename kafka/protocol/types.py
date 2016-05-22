from __future__ import absolute_import

from struct import pack, unpack, error

from .abstract import AbstractType


def _pack(f, value):
    try:
        return pack(f, value)
    except error:
        raise ValueError(error)


def _unpack(f, data):
    try:
        (value,) = unpack(f, data)
        return value
    except error:
        raise ValueError(error)


class Int8(AbstractType):
    @classmethod
    def encode(cls, value):
        return _pack('>b', value)

    @classmethod
    def decode(cls, data):
        return _unpack('>b', data.read(1))


class Int16(AbstractType):
    @classmethod
    def encode(cls, value):
        return _pack('>h', value)

    @classmethod
    def decode(cls, data):
        return _unpack('>h', data.read(2))


class Int32(AbstractType):
    @classmethod
    def encode(cls, value):
        return _pack('>i', value)

    @classmethod
    def decode(cls, data):
        return _unpack('>i', data.read(4))


class Int64(AbstractType):
    @classmethod
    def encode(cls, value):
        return _pack('>q', value)

    @classmethod
    def decode(cls, data):
        return _unpack('>q', data.read(8))


class String(AbstractType):
    def __init__(self, encoding='utf-8'):
        self.encoding = encoding

    def encode(self, value):
        if value is None:
            return Int16.encode(-1)
        value = str(value).encode(self.encoding)
        return Int16.encode(len(value)) + value

    def decode(self, data):
        length = Int16.decode(data)
        if length < 0:
            return None
        value = data.read(length)
        if len(value) != length:
            raise ValueError('Buffer underrun decoding string')
        return value.decode(self.encoding)


class Bytes(AbstractType):
    @classmethod
    def encode(cls, value):
        if value is None:
            return Int32.encode(-1)
        else:
            return Int32.encode(len(value)) + value

    @classmethod
    def decode(cls, data):
        length = Int32.decode(data)
        if length < 0:
            return None
        value = data.read(length)
        if len(value) != length:
            raise ValueError('Buffer underrun decoding Bytes')
        return value


class Schema(AbstractType):
    def __init__(self, *fields):
        if fields:
            self.names, self.fields = zip(*fields)
        else:
            self.names, self.fields = (), ()

    def encode(self, item):
        if len(item) != len(self.fields):
            raise ValueError('Item field count does not match Schema')
        return b''.join([
            field.encode(item[i])
            for i, field in enumerate(self.fields)
        ])

    def decode(self, data):
        return tuple([field.decode(data) for field in self.fields])

    def __len__(self):
        return len(self.fields)

    def repr(self, value):
        key_vals = []
        try:
            for i in range(len(self)):
                try:
                    field_val = getattr(value, self.names[i])
                except AttributeError:
                    field_val = value[i]
                key_vals.append('%s=%s' % (self.names[i], self.fields[i].repr(field_val)))
            return '(' + ', '.join(key_vals) + ')'
        except:
            return repr(value)


class Array(AbstractType):
    def __init__(self, *array_of):
        if len(array_of) > 1:
            self.array_of = Schema(*array_of)
        elif len(array_of) == 1 and (isinstance(array_of[0], AbstractType) or
                                     issubclass(array_of[0], AbstractType)):
            self.array_of = array_of[0]
        else:
            raise ValueError('Array instantiated with no array_of type')

    def encode(self, items):
        return b''.join(
            [Int32.encode(len(items))] +
            [self.array_of.encode(item) for item in items]
        )

    def decode(self, data):
        length = Int32.decode(data)
        return [self.array_of.decode(data) for _ in range(length)]

    def repr(self, list_of_items):
        return '[' + ', '.join([self.array_of.repr(item) for item in list_of_items]) + ']'
