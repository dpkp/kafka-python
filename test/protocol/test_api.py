import struct

import pytest

from kafka.protocol.api import RequestHeader
from kafka.protocol.fetch import FetchRequest
from kafka.protocol.find_coordinator import FindCoordinatorRequest
from kafka.protocol.metadata import MetadataRequest


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


def test_struct_unrecognized_kwargs():
    try:
        _mr = MetadataRequest[0](topicz='foo')
        assert False, 'Structs should not allow unrecognized kwargs'
    except ValueError:
        pass


def test_struct_missing_kwargs():
    fr = FetchRequest[0](max_wait_time=100)
    assert fr.min_bytes is None
