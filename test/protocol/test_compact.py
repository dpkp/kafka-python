import io
import struct

import pytest

from kafka.protocol.types import CompactString, CompactArray, CompactBytes


def test_compact_data_structs():
    cs = CompactString()
    encoded = cs.encode(None)
    assert encoded == struct.pack('B', 0)
    decoded = cs.decode(io.BytesIO(encoded))
    assert decoded is None
    assert b'\x01' == cs.encode('')
    assert '' == cs.decode(io.BytesIO(b'\x01'))
    encoded = cs.encode("foobarbaz")
    assert cs.decode(io.BytesIO(encoded)) == "foobarbaz"

    arr = CompactArray(CompactString())
    assert arr.encode(None) == b'\x00'
    assert arr.decode(io.BytesIO(b'\x00')) is None
    enc = arr.encode([])
    assert enc == b'\x01'
    assert [] == arr.decode(io.BytesIO(enc))
    encoded = arr.encode(["foo", "bar", "baz", "quux"])
    assert arr.decode(io.BytesIO(encoded)) == ["foo", "bar", "baz", "quux"]

    enc = CompactBytes.encode(None)
    assert enc == b'\x00'
    assert CompactBytes.decode(io.BytesIO(b'\x00')) is None
    enc = CompactBytes.encode(b'')
    assert enc == b'\x01'
    assert CompactBytes.decode(io.BytesIO(b'\x01')) == b''
    enc = CompactBytes.encode(b'foo')
    assert CompactBytes.decode(io.BytesIO(enc)) == b'foo'


