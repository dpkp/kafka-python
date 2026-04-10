from struct import error, pack, pack_into, unpack, unpack_from
import uuid

from .encode_buffer import EncodeBuffer


class FixedCodec:
    """Base class for fixed-size codecs. Subclasses define fmt and size.

    The Kafka protocol uses big-endian ('>') byte order for all fixed-size
    types. This prefix is applied here so subclasses only specify the type
    format character (e.g., 'i' for 32-bit signed int).
    """
    fmt = None  # e.g., 'i' — set by subclass
    size = None # e.g., 4 — set by subclass
    batchable = True  # Can be batched with adjacent FixedCodec fields

    def __init_subclass__(cls, **kw):
        super().__init_subclass__(**kw)
        if cls.fmt is not None:
            cls._be_fmt = '>' + cls.fmt

    @classmethod
    def encode(cls, value, compact=False):
        return pack(cls._be_fmt, value)

    @classmethod
    def encode_into(cls, out, value, compact=False):
        pack_into(cls._be_fmt, out.buf, out.pos, value)
        out.pos += cls.size

    @classmethod
    def decode(cls, data, compact=False):
        return unpack(cls._be_fmt, data.read(cls.size))[0]

    @classmethod
    def decode_from(cls, data, pos):
        """Decode from a buffer at pos. Returns (value, new_pos)."""
        return unpack_from(cls._be_fmt, data, pos)[0], pos + cls.size

    @classmethod
    def emit_encode_into(cls, ctx, val_expr, indent, compact=False):
        ctx.emit(indent, "pack_into('%s', buf, pos, %s)" % (cls._be_fmt, val_expr))
        ctx.emit(indent, 'pos += %d' % cls.size)

    @classmethod
    def emit_decode_from(cls, ctx, var_name, indent, compact=False):
        """Emit decode for a single fixed field (unbatched fallback)."""
        ctx.emit(indent, '%s = unpack_from("%s", data, pos)[0]' % (var_name, cls._be_fmt))
        ctx.emit(indent, 'pos += %d' % cls.size)


class Int8(FixedCodec):
    fmt = 'b'
    size = 1


class Int16(FixedCodec):
    fmt = 'h'
    size = 2


class Int32(FixedCodec):
    fmt = 'i'
    size = 4


class Int64(FixedCodec):
    fmt = 'q'
    size = 8


class Float64(FixedCodec):
    fmt = 'd'
    size = 8


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
    def encode_into(cls, out, value, compact=False):
        if value is None:
            value = cls.ZERO_UUID
        if isinstance(value, uuid.UUID):
            b = value.bytes
        else:
            b = uuid.UUID(value).bytes
        pos = out.pos
        out.buf[pos:pos+16] = b
        out.pos = pos + 16

    @classmethod
    def emit_encode_into(cls, ctx, val_expr, indent, compact=False):
        ctx.globs['_ZERO_UUID'] = cls.ZERO_UUID
        v = ctx.next_var('uv')
        ctx.emit(indent, '%s = %s' % (v, val_expr))
        ctx.emit(indent, 'if %s is None: %s = _ZERO_UUID' % (v, v))
        ctx.emit(indent, 'buf[pos:pos+16] = %s.bytes if hasattr(%s, "bytes") else __import__("uuid").UUID(%s).bytes' % (v, v, v))
        ctx.emit(indent, 'pos += 16')

    @classmethod
    def emit_decode_from(cls, ctx, var_name, indent, compact=False):
        ctx.globs['_ZERO_UUID'] = cls.ZERO_UUID
        ctx.globs['_uuid_cls'] = uuid.UUID
        ctx.emit(indent, '%s = _uuid_cls(bytes=bytes(data[pos:pos+16]))' % var_name)
        ctx.emit(indent, 'if %s == _ZERO_UUID: %s = None' % (var_name, var_name))
        ctx.emit(indent, 'pos += 16')

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

    def encode_into(self, out, value, compact=False):
        if compact:
            if value is None:
                UnsignedVarInt32.encode_into(out,0)
                return
            value = str(value).encode(self.encoding)
            UnsignedVarInt32.encode_into(out,len(value) + 1)
        else:
            if value is None:
                pack_into('>h', out.buf, out.pos, -1)
                out.pos += 2
                return
            value = str(value).encode(self.encoding)
            pack_into('>h', out.buf, out.pos, len(value))
            out.pos += 2
        n = len(value)
        pos = out.pos
        out.buf[pos:pos+n] = value
        out.pos = pos + n

    def emit_encode_into(self, ctx, val_expr, indent, compact=False):
        sv = ctx.next_var('sv')
        ctx.emit(indent, 'if %s is None:' % val_expr)
        if compact:
            ctx.emit(indent, '    buf[pos] = 0')
            ctx.emit(indent, '    pos += 1')
            ctx.emit(indent, 'else:')
            sn = ctx.next_var('sn')
            ctx.emit(indent, '    %s = str(%s).encode("utf-8")' % (sv, val_expr))
            ctx.emit(indent, '    %s = len(%s) + 1' % (sn, sv))
            UnsignedVarInt32.emit_encode_into(ctx, sn, indent + '    ')
            ctx.emit(indent, '    buf[pos:pos+len(%s)] = %s' % (sv, sv))
            ctx.emit(indent, '    pos += len(%s)' % sv)
        else:
            ctx.emit(indent, "    pack_into('>h', buf, pos, -1)")
            ctx.emit(indent, '    pos += 2')
            ctx.emit(indent, 'else:')
            ctx.emit(indent, '    %s = str(%s).encode("utf-8")' % (sv, val_expr))
            ctx.emit(indent, "    pack_into('>h', buf, pos, len(%s))" % sv)
            ctx.emit(indent, '    pos += 2')
            ctx.emit(indent, '    buf[pos:pos+len(%s)] = %s' % (sv, sv))
            ctx.emit(indent, '    pos += len(%s)' % sv)

    def emit_decode_from(self, ctx, var_name, indent, compact=False):
        ln = ctx.next_var('ln')
        if compact:
            UnsignedVarInt32.emit_decode_from(ctx, ln, indent)
            ctx.emit(indent, '%s -= 1' % ln)
        else:
            ctx.emit(indent, '%s = unpack_from(">h", data, pos)[0]' % ln)
            ctx.emit(indent, 'pos += 2')
        ctx.emit(indent, 'if %s < 0:' % ln)
        ctx.emit(indent, '    %s = None' % var_name)
        ctx.emit(indent, 'else:')
        ctx.emit(indent, '    %s = str(data[pos:pos+%s], "utf-8")' % (var_name, ln))
        ctx.emit(indent, '    pos += %s' % ln)

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
        if value is not None and not isinstance(value, (bytes, bytearray, memoryview)):
            value = value.encode()
        if compact:
            if value is None:
                return UnsignedVarInt32.encode(0)
            return UnsignedVarInt32.encode(len(value) + 1) + bytes(value)
        if value is None:
            return Int32.encode(-1)
        return Int32.encode(len(value)) + bytes(value)

    @classmethod
    def encode_into(cls, out, value, compact=False):
        if value is not None and not isinstance(value, (bytes, bytearray, memoryview)):
            value = value.encode()
        if compact:
            if value is None:
                UnsignedVarInt32.encode_into(out, 0)
                return
            UnsignedVarInt32.encode_into(out, len(value) + 1)
        else:
            if value is None:
                pack_into('>i', out.buf, out.pos, -1)
                out.pos += 4
                return
            pack_into('>i', out.buf, out.pos, len(value))
            out.pos += 4
        n = len(value)
        out.ensure(n)
        pos = out.pos
        out.buf[pos:pos+n] = value
        out.pos = pos + n

    @classmethod
    def emit_encode_into(cls, ctx, val_expr, indent, compact=False):
        bv = ctx.next_var('bv')
        bn = ctx.next_var('bn')
        if compact:
            ctx.emit(indent, '%s = %s' % (bv, val_expr))
            ctx.emit(indent, 'if %s is not None and not isinstance(%s, (bytes, bytearray, memoryview)): %s = %s.encode()' % (bv, bv, bv, bv))
            ctx.emit(indent, 'if %s is None:' % bv)
            ctx.emit(indent, '    buf[pos] = 0')
            ctx.emit(indent, '    pos += 1')
            ctx.emit(indent, 'else:')
            ctx.emit(indent, '    %s = len(%s)' % (bn, bv))
            sn = ctx.next_var('sn')
            ctx.emit(indent, '    %s = %s + 1' % (sn, bn))
            UnsignedVarInt32.emit_encode_into(ctx, sn, indent + '    ')
            ctx.emit(indent, '    out.pos = pos')
            ctx.emit(indent, '    out.ensure(%s)' % bn)
            ctx.emit(indent, '    buf = out.buf')
            ctx.emit(indent, '    buf[pos:pos+%s] = %s' % (bn, bv))
            ctx.emit(indent, '    pos += %s' % bn)
        else:
            ctx.emit(indent, '%s = %s' % (bv, val_expr))
            ctx.emit(indent, 'if %s is not None and not isinstance(%s, (bytes, bytearray, memoryview)): %s = %s.encode()' % (bv, bv, bv, bv))
            ctx.emit(indent, 'if %s is None:' % bv)
            ctx.emit(indent, "    pack_into('>i', buf, pos, -1)")
            ctx.emit(indent, '    pos += 4')
            ctx.emit(indent, 'else:')
            ctx.emit(indent, '    %s = len(%s)' % (bn, bv))
            ctx.emit(indent, "    pack_into('>i', buf, pos, %s)" % bn)
            ctx.emit(indent, '    pos += 4')
            ctx.emit(indent, '    out.pos = pos')
            ctx.emit(indent, '    out.ensure(%s)' % bn)
            ctx.emit(indent, '    buf = out.buf')
            ctx.emit(indent, '    buf[pos:pos+%s] = %s' % (bn, bv))
            ctx.emit(indent, '    pos += %s' % bn)

    @classmethod
    def emit_decode_from(cls, ctx, var_name, indent, compact=False):
        ln = ctx.next_var('ln')
        if compact:
            UnsignedVarInt32.emit_decode_from(ctx, ln, indent)
            ctx.emit(indent, '%s -= 1' % ln)
        else:
            ctx.emit(indent, '%s = unpack_from(">i", data, pos)[0]' % ln)
            ctx.emit(indent, 'pos += 4')
        ctx.emit(indent, 'if %s < 0:' % ln)
        ctx.emit(indent, '    %s = None' % var_name)
        ctx.emit(indent, 'else:')
        ctx.emit(indent, '    %s = bytes(data[pos:pos+%s])' % (var_name, ln))
        ctx.emit(indent, '    pos += %s' % ln)

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


class Boolean(FixedCodec):
    fmt = '?'
    size = 1


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

    @classmethod
    def encode_into(cls, out, value):
        buf = out.buf
        pos = out.pos
        while (value & 0xffffff80) != 0:
            buf[pos] = (value & 0x7f) | 0x80
            value >>= 7
            pos += 1
        buf[pos] = value
        out.pos = pos + 1

    @classmethod
    def emit_encode_into(cls, ctx, val_expr, indent, compact=False):
        ctx.emit(indent, 'while (%s & 0xffffff80) != 0:' % val_expr)
        ctx.emit(indent, '    buf[pos] = (%s & 0x7f) | 0x80' % val_expr)
        ctx.emit(indent, '    %s >>= 7' % val_expr)
        ctx.emit(indent, '    pos += 1')
        ctx.emit(indent, 'buf[pos] = %s' % val_expr)
        ctx.emit(indent, 'pos += 1')

    @classmethod
    def emit_decode_from(cls, ctx, var_name, indent):
        """Emit inline unsigned varint decode. Result in var_name, pos advanced."""
        b = ctx.next_var('b')
        shift = ctx.next_var('sh')
        ctx.emit(indent, '%s = 0' % var_name)
        ctx.emit(indent, '%s = 0' % shift)
        ctx.emit(indent, 'while True:')
        ctx.emit(indent, '    %s = data[pos]' % b)
        ctx.emit(indent, '    pos += 1')
        ctx.emit(indent, '    %s |= (%s & 0x7f) << %s' % (var_name, b, shift))
        ctx.emit(indent, '    if not (%s & 0x80): break' % b)
        ctx.emit(indent, '    %s += 7' % shift)


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
    def encode_into(cls, out, vals, compact=False):
        if vals is None:
            vals = {31}
        pack_into('>I', out.buf, out.pos, cls.to_32_bit_field(vals))
        out.pos += 4

    @classmethod
    def emit_encode_into(cls, ctx, val_expr, indent, compact=False):
        bf = ctx.next_var('bf')
        bfi = ctx.next_var('bfi')
        ctx.emit(indent, '%s = %s' % (bf, val_expr))
        ctx.emit(indent, 'if %s is None: %s = {31}' % (bf, bf))
        ctx.emit(indent, '%s = 0' % bfi)
        ctx.emit(indent, 'for _b in %s: %s |= 1 << _b' % (bf, bfi))
        ctx.emit(indent, "pack_into('>I', buf, pos, %s)" % bfi)
        ctx.emit(indent, 'pos += 4')

    @classmethod
    def emit_decode_from(cls, ctx, var_name, indent, compact=False):
        ctx.globs['_bitfield_from_32'] = cls.from_32_bit_field
        raw = ctx.next_var('bfr')
        ctx.emit(indent, '%s = unpack_from(">I", data, pos)[0]' % raw)
        ctx.emit(indent, 'pos += 4')
        ctx.emit(indent, '%s = _bitfield_from_32(%s)' % (var_name, raw))
        ctx.emit(indent, 'if %s == {31}: %s = None' % (var_name, var_name))

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
