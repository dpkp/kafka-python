#pylint: skip-file
import io
import struct

import pytest

from kafka.protocol.api import RequestHeader
from kafka.protocol.fetch import FetchRequest, FetchResponse
from kafka.protocol.find_coordinator import FindCoordinatorRequest
from kafka.protocol.metadata import MetadataRequest
from kafka.protocol.types import Int16, Int32, Int64, String, UnsignedVarInt32, CompactString, CompactArray, CompactBytes, BitField


def test_encode_message_header():
    expect = b''.join([
        struct.pack('>h', 10),             # API Key
        struct.pack('>h', 0),              # API Version
        struct.pack('>i', 4),              # Correlation Id
        struct.pack('>h', len('client3')), # Length of clientId
        b'client3',                        # ClientId
    ])

    req = FindCoordinatorRequest[0]('foo')
    header = RequestHeader(req, correlation_id=4, client_id='client3')
    assert header.encode() == expect


def test_decode_fetch_response_partial():
    encoded = b''.join([
        Int32.encode(1),               # Num Topics (Array)
        String('utf-8').encode('foobar'),
        Int32.encode(2),               # Num Partitions (Array)
        Int32.encode(0),               # Partition id
        Int16.encode(0),               # Error Code
        Int64.encode(1234),            # Highwater offset
        Int32.encode(52),              # MessageSet size
        Int64.encode(0),               # Msg Offset
        Int32.encode(18),              # Msg Size
        struct.pack('>i', 1474775406), # CRC
        struct.pack('>bb', 0, 0),      # Magic, flags
        struct.pack('>i', 2),          # Length of key
        b'k1',                         # Key
        struct.pack('>i', 2),          # Length of value
        b'v1',                         # Value

        Int64.encode(1),               # Msg Offset
        struct.pack('>i', 24),         # Msg Size (larger than remaining MsgSet size)
        struct.pack('>i', -16383415),  # CRC
        struct.pack('>bb', 0, 0),      # Magic, flags
        struct.pack('>i', 2),          # Length of key
        b'k2',                         # Key
        struct.pack('>i', 8),          # Length of value
        b'ar',                         # Value (truncated)
        Int32.encode(1),
        Int16.encode(0),
        Int64.encode(2345),
        Int32.encode(52),              # MessageSet size
        Int64.encode(0),               # Msg Offset
        Int32.encode(18),              # Msg Size
        struct.pack('>i', 1474775406), # CRC
        struct.pack('>bb', 0, 0),      # Magic, flags
        struct.pack('>i', 2),          # Length of key
        b'k1',                         # Key
        struct.pack('>i', 2),          # Length of value
        b'v1',                         # Value

        Int64.encode(1),               # Msg Offset
        struct.pack('>i', 24),         # Msg Size (larger than remaining MsgSet size)
        struct.pack('>i', -16383415),  # CRC
        struct.pack('>bb', 0, 0),      # Magic, flags
        struct.pack('>i', 2),          # Length of key
        b'k2',                         # Key
        struct.pack('>i', 8),          # Length of value
        b'ar',                         # Value (truncated)
    ])
    resp = FetchResponse[0].decode(io.BytesIO(encoded))
    assert len(resp.topics) == 1
    topic, partitions = resp.topics[0]
    assert topic == 'foobar'
    assert len(partitions) == 2

    #m1 = MessageSet.decode(
    #    partitions[0][3], bytes_to_read=len(partitions[0][3]))
    #assert len(m1) == 2
    #assert m1[1] == (None, None, PartialMessage())


def test_struct_unrecognized_kwargs():
    try:
        _mr = MetadataRequest[0](topicz='foo')
        assert False, 'Structs should not allow unrecognized kwargs'
    except ValueError:
        pass


def test_struct_missing_kwargs():
    fr = FetchRequest[0](max_wait_time=100)
    assert fr.min_bytes is None


def test_unsigned_varint_serde():
    pairs = {
        0: [0],
        -1: [0xff, 0xff, 0xff, 0xff, 0x0f],
        1: [1],
        63: [0x3f],
        -64: [0xc0, 0xff, 0xff, 0xff, 0x0f],
        64: [0x40],
        8191: [0xff, 0x3f],
        -8192: [0x80, 0xc0, 0xff, 0xff, 0x0f],
        8192: [0x80, 0x40],
        -8193: [0xff, 0xbf, 0xff, 0xff, 0x0f],
        1048575: [0xff, 0xff, 0x3f],

    }
    for value, expected_encoded in pairs.items():
        value &= 0xffffffff
        encoded = UnsignedVarInt32.encode(value)
        assert encoded == b''.join(struct.pack('>B', x) for x in expected_encoded)
        assert value == UnsignedVarInt32.decode(io.BytesIO(encoded))


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


@pytest.mark.parametrize(('test_set',), [
    (set([0, 1, 5, 10, 31]),),
    (set(range(32)),),
])
def test_bit_field(test_set):
    assert BitField.decode(io.BytesIO(BitField.encode(test_set))) == test_set
