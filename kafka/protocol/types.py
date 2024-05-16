import struct
from struct import error

from kafka.protocol.abstract import AbstractType


def _pack(f, value):
    try:
        return f(value)
    except error as e:
        raise ValueError("Error encountered when attempting to convert value: "
                        "{!r} to struct format: '{}', hit error: {}"
                        .format(value, f, e))


def _unpack(f, data):
    try:
        (value,) = f(data)
        return value
    except error as e:
        raise ValueError("Error encountered when attempting to convert value: "
                        "{!r} to struct format: '{}', hit error: {}"
                        .format(data, f, e))


class Int8(AbstractType):
    _pack = struct.Struct('>b').pack
    _unpack = struct.Struct('>b').unpack

    @classmethod
    def encode(cls, value):
        return _pack(cls._pack, value)

    @classmethod
    def decode(cls, data):
        return _unpack(cls._unpack, data.read(1))


class Int16(AbstractType):
    _pack = struct.Struct('>h').pack
    _unpack = struct.Struct('>h').unpack

    @classmethod
    def encode(cls, value):
        return _pack(cls._pack, value)

    @classmethod
    def decode(cls, data):
        return _unpack(cls._unpack, data.read(2))


class Int32(AbstractType):
    _pack = struct.Struct('>i').pack
    _unpack = struct.Struct('>i').unpack

    @classmethod
    def encode(cls, value):
        return _pack(cls._pack, value)

    @classmethod
    def decode(cls, data):
        return _unpack(cls._unpack, data.read(4))


class Int64(AbstractType):
    _pack = struct.Struct('>q').pack
    _unpack = struct.Struct('>q').unpack

    @classmethod
    def encode(cls, value):
        return _pack(cls._pack, value)

    @classmethod
    def decode(cls, data):
        return _unpack(cls._unpack, data.read(8))


class Float64(AbstractType):
    _pack = struct.Struct('>d').pack
    _unpack = struct.Struct('>d').unpack

    @classmethod
    def encode(cls, value):
        return _pack(cls._pack, value)

    @classmethod
    def decode(cls, data):
        return _unpack(cls._unpack, data.read(8))


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

    @classmethod
    def repr(cls, value):
        return repr(value[:100] + b'...' if value is not None and len(value) > 100 else value)


class Boolean(AbstractType):
    _pack = struct.Struct('>?').pack
    _unpack = struct.Struct('>?').unpack

    @classmethod
    def encode(cls, value):
        return _pack(cls._pack, value)

    @classmethod
    def decode(cls, data):
        return _unpack(cls._unpack, data.read(1))


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
                key_vals.append(f'{self.names[i]}={self.fields[i].repr(field_val)}')
            return '(' + ', '.join(key_vals) + ')'
        except Exception:
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
        if items is None:
            return Int32.encode(-1)
        encoded_items = [self.array_of.encode(item) for item in items]
        return b''.join(
            [Int32.encode(len(encoded_items))] +
            encoded_items
        )

    def decode(self, data):
        length = Int32.decode(data)
        if length == -1:
            return None
        return [self.array_of.decode(data) for _ in range(length)]

    def repr(self, list_of_items):
        if list_of_items is None:
            return 'NULL'
        return '[' + ', '.join([self.array_of.repr(item) for item in list_of_items]) + ']'


class UnsignedVarInt32(AbstractType):
    @classmethod
    def decode(cls, data):
        value, i = 0, 0
        while True:
            b, = struct.unpack('B', data.read(1))
            if not (b & 0x80):
                break
            value |= (b & 0x7f) << i
            i += 7
            if i > 28:
                raise ValueError(f'Invalid value {value}')
        value |= b << i
        return value

    @classmethod
    def encode(cls, value):
        value &= 0xffffffff
        ret = b''
        while (value & 0xffffff80) != 0:
            b = (value & 0x7f) | 0x80
            ret += struct.pack('B', b)
            value >>= 7
        ret += struct.pack('B', value)
        return ret


class VarInt32(AbstractType):
    @classmethod
    def decode(cls, data):
        value = UnsignedVarInt32.decode(data)
        return (value >> 1) ^ -(value & 1)

    @classmethod
    def encode(cls, value):
        # bring it in line with the java binary repr
        value &= 0xffffffff
        return UnsignedVarInt32.encode((value << 1) ^ (value >> 31))


class VarInt64(AbstractType):
    @classmethod
    def decode(cls, data):
        value, i = 0, 0
        while True:
            b = data.read(1)
            if not (b & 0x80):
                break
            value |= (b & 0x7f) << i
            i += 7
            if i > 63:
                raise ValueError(f'Invalid value {value}')
        value |= b << i
        return (value >> 1) ^ -(value & 1)

    @classmethod
    def encode(cls, value):
        # bring it in line with the java binary repr
        value &= 0xffffffffffffffff
        v = (value << 1) ^ (value >> 63)
        ret = b''
        while (v & 0xffffffffffffff80) != 0:
            b = (value & 0x7f) | 0x80
            ret += struct.pack('B', b)
            v >>= 7
        ret += struct.pack('B', v)
        return ret


class CompactString(String):
    def decode(self, data):
        length = UnsignedVarInt32.decode(data) - 1
        if length < 0:
            return None
        value = data.read(length)
        if len(value) != length:
            raise ValueError('Buffer underrun decoding string')
        return value.decode(self.encoding)

    def encode(self, value):
        if value is None:
            return UnsignedVarInt32.encode(0)
        value = str(value).encode(self.encoding)
        return UnsignedVarInt32.encode(len(value) + 1) + value


class TaggedFields(AbstractType):
    @classmethod
    def decode(cls, data):
        num_fields = UnsignedVarInt32.decode(data)
        ret = {}
        if not num_fields:
            return ret
        prev_tag = -1
        for i in range(num_fields):
            tag = UnsignedVarInt32.decode(data)
            if tag <= prev_tag:
                raise ValueError(f'Invalid or out-of-order tag {tag}')
            prev_tag = tag
            size = UnsignedVarInt32.decode(data)
            val = data.read(size)
            ret[tag] = val
        return ret

    @classmethod
    def encode(cls, value):
        ret = UnsignedVarInt32.encode(len(value))
        for k, v in value.items():
            # do we allow for other data types ?? It could get complicated really fast
            assert isinstance(v, bytes), f'Value {v} is not a byte array'
            assert isinstance(k, int) and k > 0, f'Key {k} is not a positive integer'
            ret += UnsignedVarInt32.encode(k)
            ret += v
        return ret


class CompactBytes(AbstractType):
    @classmethod
    def decode(cls, data):
        length = UnsignedVarInt32.decode(data) - 1
        if length < 0:
            return None
        value = data.read(length)
        if len(value) != length:
            raise ValueError('Buffer underrun decoding Bytes')
        return value

    @classmethod
    def encode(cls, value):
        if value is None:
            return UnsignedVarInt32.encode(0)
        else:
            return UnsignedVarInt32.encode(len(value) + 1) + value


class CompactArray(Array):

    def encode(self, items):
        if items is None:
            return UnsignedVarInt32.encode(0)
        return b''.join(
            [UnsignedVarInt32.encode(len(items) + 1)] +
            [self.array_of.encode(item) for item in items]
        )

    def decode(self, data):
        length = UnsignedVarInt32.decode(data) - 1
        if length == -1:
            return None
        return [self.array_of.decode(data) for _ in range(length)]

