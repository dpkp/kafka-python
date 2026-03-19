import io

import pytest

from kafka.protocol.new.schemas.fields import SimpleField
from kafka.protocol.new.schemas.fields.codecs import TaggedFields, UnsignedVarInt32


def test_tagged_fields():
    tags = TaggedFields([
        SimpleField({'name': 'foo', 'tag': 0, 'type': 'string', 'versions': "0+"}),
        SimpleField({'name': 'bar', 'tag': 1, 'type': 'string', 'versions': "0+"}),
    ])
    val = {'foo': 'fizz', 'bar': 'foobar'}
    encoded = tags.encode(val, version=0)
    # length(2), tag(0), size(5), len(5), 'fizz', tag(1), size(7), len(7), 'foobar'
    expected = (UnsignedVarInt32.encode(2) +
                UnsignedVarInt32.encode(0) + UnsignedVarInt32.encode(5) + UnsignedVarInt32.encode(5) + b'fizz' +
                UnsignedVarInt32.encode(1) + UnsignedVarInt32.encode(7) + UnsignedVarInt32.encode(7) + b'foobar')
    assert encoded == expected
    decoded = tags.decode(io.BytesIO(encoded), version=0)
    assert decoded == val
