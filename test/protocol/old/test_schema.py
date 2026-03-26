import io

import pytest

from kafka.protocol.old.struct import Struct
from kafka.protocol.old.types import Schema, Int32, String, TaggedFields, Bytes


def test_schema_type():
    schema = Schema(('f1', Int32), ('f2', String()))
    val = (123, "bar")
    encoded = schema.encode(val)
    assert encoded == b'\x00\x00\x00\x7b\x00\x03bar'
    assert schema.decode(io.BytesIO(encoded)) == val

    with pytest.raises(ValueError):
        schema.encode((123,))


def test_schema_tagged_fields():
    schema = Schema(('f1', Int32), ('f2', String()), ('tags', TaggedFields))
    assert schema.has_tagged_fields()

    val = (123, "foo", {0: b'bar'})
    encoded = schema.encode(val)
    assert encoded == b'\x00\x00\x00\x7b\x00\x03foo\x01\x00\x03bar'
    assert schema.decode(io.BytesIO(encoded)) == val

    val = (123, "bar")
    encoded = schema.encode(val)
    assert encoded == b'\x00\x00\x00\x7b\x00\x03bar\x00'
    # empty tags always decodes to tuple w/o tag field
    assert schema.decode(io.BytesIO(encoded)) == val

    # Same encoding with various ways to construct
    assert schema.encode((123, "bar", {})) == encoded


@pytest.mark.parametrize('args, kwargs', [
    ((),{"f1": 123, "f2": "bar"}),
    ((),{"f1": 123, "f2": "bar", "tags": {}}),
    ((),{"f1": 123, "f2": "bar", "tags": None}),
    ((123, "bar"), {}),
    ((123, "bar", {}), {}),
    ((123, "bar", None), {}),
])
def test_struct(args, kwargs):
    schema = Schema(('f1', Int32), ('f2', String()), ('tags', TaggedFields))
    struct = type('TestStruct', (Struct,), {'SCHEMA': schema})
    encoded = b'\x00\x00\x00\x7b\x00\x03bar\x00'

    data = struct(*args, **kwargs)
    assert data.encode() == encoded
    assert struct.decode(encoded) == data


def test_bytes_struct():
    schema = Schema(('f1', Int32), ('f2', String()))
    struct = type('TestStruct', (Struct,), {'SCHEMA': schema})
    data = struct(f1=123, f2="bar")
    bytes_encoded = Bytes.encode(data)
    assert bytes_encoded[4:] == data.encode()
    assert bytes_encoded[:4] == Int32.encode(len(data.encode()))
