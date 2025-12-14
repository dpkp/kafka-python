#pylint: skip-file
import io
import struct

import pytest

from kafka.protocol.fetch import FetchResponse
from kafka.protocol.types import Int16, Int32, Int64, String


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
