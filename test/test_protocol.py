#pylint: skip-file
import io
import struct

import pytest
import six

from kafka.protocol.api import RequestHeader
from kafka.protocol.commit import GroupCoordinatorRequest
from kafka.protocol.fetch import FetchResponse
from kafka.protocol.message import Message, MessageSet, PartialMessage
from kafka.protocol.types import Int16, Int32, Int64, String


def test_create_message():
    payload = b'test'
    key = b'key'
    msg = Message(payload, key=key)
    assert msg.magic == 0
    assert msg.attributes == 0
    assert msg.key == key
    assert msg.value == payload


def test_encode_message_v0():
    message = Message(b'test', key=b'key')
    encoded = message.encode()
    expect = b''.join([
        struct.pack('>i', -1427009701), # CRC
        struct.pack('>bb', 0, 0),       # Magic, flags
        struct.pack('>i', 3),           # Length of key
        b'key',                         # key
        struct.pack('>i', 4),           # Length of value
        b'test',                        # value
    ])
    assert encoded == expect


def test_encode_message_v1():
    message = Message(b'test', key=b'key', magic=1, timestamp=1234)
    encoded = message.encode()
    expect = b''.join([
        struct.pack('>i', 1331087195),  # CRC
        struct.pack('>bb', 1, 0),       # Magic, flags
        struct.pack('>q', 1234),        # Timestamp
        struct.pack('>i', 3),           # Length of key
        b'key',                         # key
        struct.pack('>i', 4),           # Length of value
        b'test',                        # value
    ])
    assert encoded == expect


def test_decode_message():
    encoded = b''.join([
        struct.pack('>i', -1427009701), # CRC
        struct.pack('>bb', 0, 0),       # Magic, flags
        struct.pack('>i', 3),           # Length of key
        b'key',                         # key
        struct.pack('>i', 4),           # Length of value
        b'test',                        # value
    ])
    decoded_message = Message.decode(encoded)
    msg = Message(b'test', key=b'key')
    msg.encode() # crc is recalculated during encoding
    assert decoded_message == msg


def test_encode_message_set():
    messages = [
        Message(b'v1', key=b'k1'),
        Message(b'v2', key=b'k2')
    ]
    encoded = MessageSet.encode([(0, msg.encode())
                                 for msg in messages])
    expect = b''.join([
        struct.pack('>q', 0),          # MsgSet Offset
        struct.pack('>i', 18),         # Msg Size
        struct.pack('>i', 1474775406), # CRC
        struct.pack('>bb', 0, 0),      # Magic, flags
        struct.pack('>i', 2),          # Length of key
        b'k1',                          # Key
        struct.pack('>i', 2),          # Length of value
        b'v1',                          # Value

        struct.pack('>q', 0),          # MsgSet Offset
        struct.pack('>i', 18),         # Msg Size
        struct.pack('>i', -16383415),  # CRC
        struct.pack('>bb', 0, 0),      # Magic, flags
        struct.pack('>i', 2),          # Length of key
        b'k2',                          # Key
        struct.pack('>i', 2),          # Length of value
        b'v2',                          # Value
    ])
    expect = struct.pack('>i', len(expect)) + expect
    assert encoded == expect


def test_decode_message_set():
    encoded = b''.join([
        struct.pack('>q', 0),          # MsgSet Offset
        struct.pack('>i', 18),         # Msg Size
        struct.pack('>i', 1474775406), # CRC
        struct.pack('>bb', 0, 0),      # Magic, flags
        struct.pack('>i', 2),          # Length of key
        b'k1',                         # Key
        struct.pack('>i', 2),          # Length of value
        b'v1',                         # Value

        struct.pack('>q', 1),          # MsgSet Offset
        struct.pack('>i', 18),         # Msg Size
        struct.pack('>i', -16383415),  # CRC
        struct.pack('>bb', 0, 0),      # Magic, flags
        struct.pack('>i', 2),          # Length of key
        b'k2',                         # Key
        struct.pack('>i', 2),          # Length of value
        b'v2',                         # Value
    ])

    msgs = MessageSet.decode(encoded, bytes_to_read=len(encoded))
    assert len(msgs) == 2
    msg1, msg2 = msgs

    returned_offset1, message1_size, decoded_message1 = msg1
    returned_offset2, message2_size, decoded_message2 = msg2

    assert returned_offset1 == 0
    message1 = Message(b'v1', key=b'k1')
    message1.encode()
    assert decoded_message1 == message1

    assert returned_offset2 == 1
    message2 = Message(b'v2', key=b'k2')
    message2.encode()
    assert decoded_message2 == message2


def test_encode_message_header():
    expect = b''.join([
        struct.pack('>h', 10),             # API Key
        struct.pack('>h', 0),              # API Version
        struct.pack('>i', 4),              # Correlation Id
        struct.pack('>h', len('client3')), # Length of clientId
        b'client3',                        # ClientId
    ])

    req = GroupCoordinatorRequest[0]('foo')
    header = RequestHeader(req, correlation_id=4, client_id='client3')
    assert header.encode() == expect


def test_decode_message_set_partial():
    encoded = b''.join([
        struct.pack('>q', 0),          # Msg Offset
        struct.pack('>i', 18),         # Msg Size
        struct.pack('>i', 1474775406), # CRC
        struct.pack('>bb', 0, 0),      # Magic, flags
        struct.pack('>i', 2),          # Length of key
        b'k1',                         # Key
        struct.pack('>i', 2),          # Length of value
        b'v1',                         # Value

        struct.pack('>q', 1),          # Msg Offset
        struct.pack('>i', 24),         # Msg Size (larger than remaining MsgSet size)
        struct.pack('>i', -16383415),  # CRC
        struct.pack('>bb', 0, 0),      # Magic, flags
        struct.pack('>i', 2),          # Length of key
        b'k2',                         # Key
        struct.pack('>i', 8),          # Length of value
        b'ar',                         # Value (truncated)
    ])

    msgs = MessageSet.decode(encoded, bytes_to_read=len(encoded))
    assert len(msgs) == 2
    msg1, msg2 = msgs

    returned_offset1, message1_size, decoded_message1 = msg1
    returned_offset2, message2_size, decoded_message2 = msg2

    assert returned_offset1 == 0
    message1 = Message(b'v1', key=b'k1')
    message1.encode()
    assert decoded_message1 == message1

    assert returned_offset2 is None
    assert message2_size is None
    assert decoded_message2 == PartialMessage()


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
    m1 = partitions[0][3]
    assert len(m1) == 2
    assert m1[1] == (None, None, PartialMessage())
