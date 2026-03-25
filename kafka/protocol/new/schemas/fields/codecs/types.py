from struct import error, pack, unpack
import uuid

class Int8:
    fmt = 'b'
    size = 1

    @classmethod
    def encode(cls, value, compact=False):
        return pack('>b', value)

    @classmethod
    def decode(cls, data, compact=False):
        return unpack('>b', data.read(1))[0]


class Int16:
    fmt = 'h'
    size = 2

    @classmethod
    def encode(cls, value, compact=False):
        return pack('>h', value)

    @classmethod
    def decode(cls, data, compact=False):
        return unpack('>h', data.read(2))[0]


class Int32:
    fmt = 'i'
    size = 4

    @classmethod
    def encode(cls, value, compact=False):
        return pack('>i', value)

    @classmethod
    def decode(cls, data, compact=False):
        return unpack('>i', data.read(4))[0]


class Int64:
    fmt = 'q'
    size = 8

    @classmethod
    def encode(cls, value, compact=False):
        return pack('>q', value)

    @classmethod
    def decode(cls, data, compact=False):
        return unpack('>q', data.read(8))[0]


class Float64:
    fmt = 'd'
    size = 8

    @classmethod
    def encode(cls, value, compact=False):
        return pack('>d', value)

    @classmethod
    def decode(cls, data, compact=False):
        return unpack('>d', data.read(8))[0]


class UUID:
    fmt = '16B'
    size = 16
    ZERO_UUID = uuid.UUID(int=0)

    @classmethod
    def encode(cls, value, compact=False):
        if value is None:
            value = cls.ZERO_UUID
        if isinstance(value, uuid.UUID):
            return value.bytes
        return uuid.UUID(value).bytes

    @classmethod
    def decode(cls, data, compact=False):
        val = uuid.UUID(bytes=data.read(16))
        if val == cls.ZERO_UUID:
            return None
        return val


class String:
    fmt = None # 'B' for compact, 'h' for standard
    size = 'variable'

    def __init__(self, encoding='utf-8'):
        self.encoding = encoding

    def encode(self, value, compact=False):
        if compact:
            if value is None:
                return UnsignedVarInt32.encode(0)
            value = str(value).encode(self.encoding)
            return UnsignedVarInt32.encode(len(value) + 1) + value
        if value is None:
            return Int16.encode(-1)
        value = str(value).encode(self.encoding)
        return Int16.encode(len(value)) + value

    def decode(self, data, compact=False):
        if compact:
            length = UnsignedVarInt32.decode(data) - 1
        else:
            length = Int16.decode(data)
        if length < 0:
            return None
        value = data.read(length)
        if len(value) != length:
            raise ValueError('Buffer underrun decoding string')
        return value.decode(self.encoding)


class Bytes:
    fmt = Int32.fmt
    size = 'variable'

    @classmethod
    def encode(cls, value, compact=False):
        if compact:
            if value is None:
                return UnsignedVarInt32.encode(0)
            return UnsignedVarInt32.encode(len(value) + 1) + value
        if value is None:
            return Int32.encode(-1)
        elif not isinstance(value, bytes):
            value = value.encode()
        return Int32.encode(len(value)) + value

    @classmethod
    def decode(cls, data, compact=False):
        if compact:
            length = UnsignedVarInt32.decode(data) - 1
        else:
            length = Int32.decode(data)
        if length < 0:
            return None
        value = data.read(length)
        if len(value) != length:
            raise ValueError('Buffer underrun decoding Bytes')
        return value


class Boolean:
    fmt = '?'
    size = 1

    @classmethod
    def encode(cls, value, compact=False):
        return pack('>?', value)

    @classmethod
    def decode(cls, data, compact=False):
        return unpack('>?', data.read(1))[0]


class UnsignedVarInt32:
    fmt = 'B'
    size = 'variable'

    @classmethod
    def decode(cls, data, compact=False):
        value = VarInt32.decode(data)
        return (value << 1) ^ (value >> 31)

    @classmethod
    def encode(cls, value, compact=False):
        return VarInt32.encode((value >> 1) ^ -(value & 1))


class VarInt32:
    fmt = 'B'
    size = 'variable'

    @classmethod
    def decode(cls, data, compact=False):
        value, i = 0, 0
        while True:
            b, = unpack('B', data.read(1))
            if not (b & 0x80):
                break
            value |= (b & 0x7f) << i
            i += 7
            if i > 28:
                raise ValueError('Invalid value {}'.format(value))
        value |= b << i
        return (value >> 1) ^ -(value & 1)

    @classmethod
    def encode(cls, value, compact=False):
        # bring it in line with the java binary repr
        value = (value << 1) ^ (value >> 31)
        value &= 0xffffffff
        ret = b''
        while (value & 0xffffff80) != 0:
            b = (value & 0x7f) | 0x80
            ret += pack('B', b)
            value >>= 7
        ret += pack('B', value)
        return ret


class VarInt64:
    fmt = 'B'
    size = 'variable'

    @classmethod
    def decode(cls, data, compact=False):
        value, i = 0, 0
        while True:
            b, = unpack('B', data.read(1))
            if not (b & 0x80):
                break
            value |= (b & 0x7f) << i
            i += 7
            if i > 63:
                raise ValueError('Invalid value {}'.format(value))
        value |= b << i
        return (value >> 1) ^ -(value & 1)

    @classmethod
    def encode(cls, value, compact=False):
        # bring it in line with the java binary repr
        value = (value << 1) ^ (value >> 63)
        value &= 0xffffffffffffffff
        ret = b''
        while (value & 0xffffffffffffff80) != 0:
            b = (value & 0x7f) | 0x80
            ret += pack('B', b)
            value >>= 7
        ret += pack('B', value)
        return ret


class BitField:
    fmt = 'I'
    size = 4

    @classmethod
    def decode(cls, data, compact=False):
        vals = cls.from_32_bit_field(unpack('>I', data.read(4))[0])
        if vals == {31}:
            vals = None
        return vals

    @classmethod
    def encode(cls, vals, compact=False):
        if vals is None:
            vals = {31}
        # to_32_bit_field returns unsigned val, so we need to
        # encode >I to avoid crash if/when byte 31 is set
        # (note that decode as signed still works fine)
        return pack('>I', cls.to_32_bit_field(vals))

    @classmethod
    def to_32_bit_field(cls, vals):
        value = 0
        for b in vals:
            assert 0 <= b < 32
            value |= 1 << b
        return value

    @classmethod
    def from_32_bit_field(cls, value):
        result = set()
        count = 0
        while value != 0:
            if (value & 1) != 0:
                result.add(count)
            count += 1
            value = (value & 0xFFFFFFFF) >> 1
        return result
