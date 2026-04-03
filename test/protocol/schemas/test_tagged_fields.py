import io

import pytest

from kafka.protocol.schemas.fields import SimpleField, StructField
from kafka.protocol.schemas.fields.codecs import TaggedFields, UnsignedVarInt32


def test_tagged_fields():
    tags = TaggedFields([
        SimpleField({'name': 'foo', 'tag': 0, 'type': 'int16', 'versions': "0+"}),
        SimpleField({'name': 'bar', 'tag': 1, 'type': 'string', 'versions': "0+"}),
    ])
    val = {'foo': 2, 'bar': 'foobar'}
    encoded = tags.encode(val, version=0)
    # num_tags(2), tag(0), size(2), b'\x00\x02', tag(1), size(7), len(6+1), 'foobar'
    expected = (UnsignedVarInt32.encode(2) +
                UnsignedVarInt32.encode(0) + UnsignedVarInt32.encode(2) + b'\x00\x02' +
                UnsignedVarInt32.encode(1) + UnsignedVarInt32.encode(7) + UnsignedVarInt32.encode(7) + b'foobar')
    assert encoded == expected
    decoded = tags.decode(io.BytesIO(encoded), version=0)
    assert decoded == val


def test_tagged_fields_struct():
    tags = TaggedFields([
        StructField({'name': 'foo', 'tag': 0, 'type': 'Bar', 'versions': "0+", "fields": [
            {'name': 'bar', 'tag': 1, 'type': 'string', 'versions': "0+"},
        ]}),
    ])
    val = {'foo': {'bar': 'foobar'}}
    encoded = tags.encode(val, version=0)
    # num_tags(1), tag(0), size(8), len(6+1), 'foobar', empty tags(\x00)
    expected = (UnsignedVarInt32.encode(1) + UnsignedVarInt32.encode(0) + UnsignedVarInt32.encode(8) +
                UnsignedVarInt32.encode(7) + b'foobar' +
                UnsignedVarInt32.encode(0))
    assert encoded == expected
    decoded = tags.decode(io.BytesIO(encoded), version=0)
    assert decoded == val

