import io
import uuid
import struct

import pytest

from kafka.protocol.schemas.fields.codecs.types import (
    Int8, Int16, Int32, Int64, Float64, Boolean, UUID,
    UnsignedInt16, UnsignedVarInt32, VarInt32, VarInt64,
    String, Bytes, BitField,
)


@pytest.mark.parametrize("cls, value, expected", [
    (Int8, 0, b'\x00'),
    (Int8, 127, b'\x7f'),
    (Int8, -128, b'\x80'),
    (Int16, 0, b'\x00\x00'),
    (Int16, 32767, b'\x7f\xff'),
    (Int16, -32768, b'\x80\x00'),
    (UnsignedInt16, 0, b'\x00\x00'),
    (UnsignedInt16, 32768, b'\x80\x00'),
    (UnsignedInt16, 65535, b'\xff\xff'),
    (Int32, 0, b'\x00\x00\x00\x00'),
    (Int32, 2147483647, b'\x7f\xff\xff\xff'),
    (Int32, -2147483648, b'\x80\x00\x00\x00'),
    (Int64, 0, b'\x00\x00\x00\x00\x00\x00\x00\x00'),
    (Int64, 9223372036854775807, b'\x7f\xff\xff\xff\xff\xff\xff\xff'),
    (Int64, -9223372036854775808, b'\x80\x00\x00\x00\x00\x00\x00\x00'),
    (Float64, 0.0, b'\x00\x00\x00\x00\x00\x00\x00\x00'),
    (Float64, 1.0, b'\x3f\xf0\x00\x00\x00\x00\x00\x00'),
    (Boolean, True, b'\x01'),
    (Boolean, False, b'\x00'),
])
def test_primitive_types(cls, value, expected):
    encoded = cls.encode(value)
    assert encoded == expected
    decoded = cls.decode(io.BytesIO(encoded))
    assert decoded == value


def test_uuid_type():
    val = uuid.uuid4()
    encoded = UUID.encode(val)
    assert len(encoded) == 16
    assert encoded == val.bytes
    decoded = UUID.decode(io.BytesIO(encoded))
    assert decoded == val

    # Test with string
    val_str = str(val)
    encoded = UUID.encode(val_str)
    assert encoded == val.bytes


def test_string_type():
    s = String()
    assert s.encode(None) == b'\xff\xff'
    assert s.decode(io.BytesIO(b'\xff\xff')) is None

    val = "foo"
    encoded = s.encode(val)
    assert encoded == b'\x00\x03foo'
    assert s.decode(io.BytesIO(encoded)) == val

    # Test custom encoding
    s_utf16 = String('utf-16')
    val = "好"
    encoded = s_utf16.encode(val)
    assert len(encoded) == 6
    decoded = s_utf16.decode(io.BytesIO(encoded))
    assert decoded == val


def test_bytes_type():
    assert Bytes.encode(None) == b'\xff\xff\xff\xff'
    assert Bytes.decode(io.BytesIO(b'\xff\xff\xff\xff')) is None

    val = b"foo"
    encoded = Bytes.encode(val)
    assert encoded == b'\x00\x00\x00\x03foo'
    assert Bytes.decode(io.BytesIO(encoded)) == val


def test_error_handling():
    with pytest.raises(struct.error):
        Int8.encode(1000) # Too large

    with pytest.raises(struct.error):
        Int16.encode(None)

    with pytest.raises(struct.error):
        Int8.decode(io.BytesIO(b'')) # Too short

    with pytest.raises(struct.error):
        UnsignedInt16.encode(-1) # Negative not allowed

    with pytest.raises(struct.error):
        UnsignedInt16.encode(65536) # Too large

    s = String()
    with pytest.raises(ValueError):
        s.decode(io.BytesIO(b'\x00\x05foo')) # length 5 but only 3 bytes


@pytest.mark.parametrize(('test_set',), [
    (set([0, 1, 5, 10]),),
    (set(range(15)),),
    (None,),
])
def test_bit_field(test_set):
    assert BitField.decode(io.BytesIO(BitField.encode(test_set))) == test_set


def test_bit_field_null():
    assert BitField.from_32_bit_field(-2147483648) == {31}
    assert BitField.decode(io.BytesIO(BitField.encode({31}))) is None


@pytest.mark.parametrize(('value','expected_encoded'), [
    (0, [0x00]),
    (-1, [0xFF, 0xFF, 0xFF, 0xFF, 0x0F]),
    (1, [0x01]),
    (63, [0x3F]),
    (-64, [0xC0, 0xFF, 0xFF, 0xFF, 0x0F]),
    (64, [0x40]),
    (8191, [0xFF, 0x3F]),
    (-8192, [0x80, 0xC0, 0xFF, 0xFF, 0x0F]),
    (8192, [0x80, 0x40]),
    (-8193, [0xFF, 0xBF, 0xFF, 0xFF, 0x0F]),
    (1048575, [0xFF, 0xFF, 0x3F]),
    (1048576, [0x80, 0x80, 0x40]),
    (2147483647, [0xFF, 0xFF, 0xFF, 0xFF, 0x07]),
    (-2147483648, [0x80, 0x80, 0x80, 0x80, 0x08]),
])
def test_unsigned_varint_serde(value, expected_encoded):
    value &= 0xffffffff
    encoded = UnsignedVarInt32.encode(value)
    assert encoded == b''.join(struct.pack('>B', x) for x in expected_encoded)
    assert value == UnsignedVarInt32.decode(io.BytesIO(encoded))


@pytest.mark.parametrize(('value','expected_encoded'), [
    (0, [0x00]),
    (-1, [0x01]),
    (1, [0x02]),
    (63, [0x7E]),
    (-64, [0x7F]),
    (64, [0x80, 0x01]),
    (-65, [0x81, 0x01]),
    (8191, [0xFE, 0x7F]),
    (-8192, [0xFF, 0x7F]),
    (8192, [0x80, 0x80, 0x01]),
    (-8193, [0x81, 0x80, 0x01]),
    (1048575, [0xFE, 0xFF, 0x7F]),
    (-1048576, [0xFF, 0xFF, 0x7F]),
    (1048576, [0x80, 0x80, 0x80, 0x01]),
    (-1048577, [0x81, 0x80, 0x80, 0x01]),
    (134217727, [0xFE, 0xFF, 0xFF, 0x7F]),
    (-134217728, [0xFF, 0xFF, 0xFF, 0x7F]),
    (134217728, [0x80, 0x80, 0x80, 0x80, 0x01]),
    (-134217729, [0x81, 0x80, 0x80, 0x80, 0x01]),
    (2147483647, [0xFE, 0xFF, 0xFF, 0xFF, 0x0F]),
    (-2147483648, [0xFF, 0xFF, 0xFF, 0xFF, 0x0F]),
])
def test_signed_varint_serde(value, expected_encoded):
    encoded = VarInt32.encode(value)
    assert encoded == b''.join(struct.pack('>B', x) for x in expected_encoded)
    assert value == VarInt32.decode(io.BytesIO(encoded))
