import io

import pytest

from kafka.protocol.types import Schema, Int32, String, TaggedFields


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

    val = (123, "bar")
    encoded = schema.encode(val)
    assert encoded == b'\x00\x00\x00\x7b\x00\x03bar\x00'
    assert schema.decode(io.BytesIO(encoded)) == val

    # Same encoding if empty tags are passed
    assert schema.encode((123, "bar", {})) == encoded
