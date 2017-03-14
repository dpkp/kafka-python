# pylint: skip-file
from __future__ import absolute_import

import io
import platform

import pytest

from kafka.producer.buffer import MessageSetBuffer
from kafka.protocol.message import Message, MessageSet


def test_buffer_close():
    records = MessageSetBuffer(io.BytesIO(), 100000)
    orig_msg = Message(b'foobar')
    records.append(1234, orig_msg)
    records.close()

    msgset = MessageSet.decode(records.buffer())
    assert len(msgset) == 1
    (offset, size, msg) = msgset[0]
    assert offset == 1234 
    assert msg == orig_msg

    # Closing again should work fine
    records.close()

    msgset = MessageSet.decode(records.buffer())
    assert len(msgset) == 1
    (offset, size, msg) = msgset[0]
    assert offset == 1234
    assert msg == orig_msg


@pytest.mark.parametrize('compression', [
    'gzip',
    'snappy',
    pytest.mark.skipif(platform.python_implementation() == 'PyPy',
                       reason='python-lz4 crashes on older versions of pypy')('lz4'),
])
def test_compressed_buffer_close(compression):
    records = MessageSetBuffer(io.BytesIO(), 100000, compression_type=compression)
    orig_msg = Message(b'foobar')
    records.append(1234, orig_msg)
    records.close()

    msgset = MessageSet.decode(records.buffer())
    assert len(msgset) == 1
    (offset, size, msg) = msgset[0]
    assert offset == 0
    assert msg.is_compressed()

    msgset = msg.decompress()
    (offset, size, msg) = msgset[0]
    assert not msg.is_compressed()
    assert offset == 1234
    assert msg == orig_msg

    # Closing again should work fine
    records.close()

    msgset = MessageSet.decode(records.buffer())
    assert len(msgset) == 1
    (offset, size, msg) = msgset[0]
    assert offset == 0
    assert msg.is_compressed()

    msgset = msg.decompress()
    (offset, size, msg) = msgset[0]
    assert not msg.is_compressed()
    assert offset == 1234
    assert msg == orig_msg
