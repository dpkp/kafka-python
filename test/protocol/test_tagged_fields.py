import io

import pytest

from kafka.protocol.types import TaggedFields, UnsignedVarInt32


def test_tagged_fields():
    val = {0: b'fizz', 1: b'foobar'}
    encoded = TaggedFields.encode(val)
    # length(2), tag(0), size(4), 'fizz', tag(2), size(6), 'foobar'
    expected = (UnsignedVarInt32.encode(2) +
                UnsignedVarInt32.encode(0) + UnsignedVarInt32.encode(4) + b'fizz' +
                UnsignedVarInt32.encode(1) + UnsignedVarInt32.encode(6) + b'foobar')
    assert encoded == expected
    decoded = TaggedFields.decode(io.BytesIO(encoded))
    assert decoded == val
