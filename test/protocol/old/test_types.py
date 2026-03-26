import io
import uuid
import struct

import pytest

from kafka.protocol.old.types import (
    Int8, Int16, Int32, Int64, Float64, Boolean, UUID,
    String, Bytes, Array
)


@pytest.mark.parametrize("cls, value, expected", [
    (Int8, 0, b'\x00'),
    (Int8, 127, b'\x7f'),
    (Int8, -128, b'\x80'),
    (Int16, 0, b'\x00\x00'),
    (Int16, 32767, b'\x7f\xff'),
    (Int16, -32768, b'\x80\x00'),
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


def test_array_type():
    arr = Array(Int32)
    assert arr.encode(None) == b'\xff\xff\xff\xff'
    assert arr.decode(io.BytesIO(b'\xff\xff\xff\xff')) is None

    val = [1, 2, 3]
    encoded = arr.encode(val)
    assert encoded == b'\x00\x00\x00\x03\x00\x00\x00\x01\x00\x00\x00\x02\x00\x00\x00\x03'
    assert arr.decode(io.BytesIO(encoded)) == val

    # Array of Schema
    arr_schema = Array(('f1', Int32), ('f2', Int32))
    val = [(1, 10), (2, 20)]
    encoded = arr_schema.encode(val)
    assert arr_schema.decode(io.BytesIO(encoded)) == val


def test_error_handling():
    with pytest.raises(ValueError, match="Error encountered when attempting to convert value: 1000 to struct format: '>b'"):
        Int8.encode(1000) # Too large

    with pytest.raises(ValueError, match="Error encountered when attempting to convert value: None to struct format: '>h'"):
        Int16.encode(None)

    with pytest.raises(ValueError, match="Error encountered when attempting to convert value: b'' to struct format: '>b'"):
        Int8.decode(io.BytesIO(b'')) # Too short

    s = String()
    with pytest.raises(ValueError):
        s.decode(io.BytesIO(b'\x00\x05foo')) # length 5 but only 3 bytes
