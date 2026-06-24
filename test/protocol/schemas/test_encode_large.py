"""Regression tests for encoding records/messages that grow the EncodeBuffer.

These cover the buffer-overflow family of bugs in the optimized (codegen)
encode path. The EncodeBuffer starts at 64KB and grows on demand; when a
payload forces a grow that lands exactly at the buffer end (zero trailing
slack), the *next* write used to run off the end:

  * UnsignedVarInt32 / tagged-field count  -> IndexError   (the reported bug)
  * fixed fields (int32 partition index, ...) -> struct.error
  * arrays of fixed primitives ([]int32)      -> struct.error (no Bytes needed)
  * stale `buf` local after a tagged-field encode reallocated the buffer

Every writer must now reserve via EncodeBuffer.ensure() (runtime) or
CodegenContext.emit_reserve() (codegen) before writing.
"""
import io

import pytest

from kafka.protocol.schemas.fields.codecs.encode_buffer import EncodeBuffer
from kafka.protocol.schemas.fields.codecs import types as T
from kafka.protocol.producer.produce import ProduceRequest
from kafka.protocol.consumer.group import OffsetFetchRequest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _reference_encode(m):
    flexible = m.flexible_version_q(m.API_VERSION)
    return bytes(m._struct.encode(
        m, version=m.API_VERSION, compact=flexible, tagged=flexible))


def _codegen_encode(m, buf=None):
    flexible = m.flexible_version_q(m.API_VERSION)
    fn = m._struct.compiled_encode_into(
        m.API_VERSION, compact=flexible, tagged=flexible)
    out = buf if buf is not None else EncodeBuffer()
    fn(m, out)
    return bytes(out.result())


def _full_buffer(size=16):
    """An EncodeBuffer whose pos sits exactly at the end (zero slack)."""
    b = EncodeBuffer(size=size)
    b.pos = size
    return b


# ---------------------------------------------------------------------------
# Unit: writing into a completely-full buffer must grow, not crash, and the
# bytes written must match the reference encode().
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('codec,value', [
    (T.Int8, 5),
    (T.Int16, 1234),
    (T.Int32, 1234567),
    (T.Int64, 2 ** 40),
    (T.UnsignedInt16, 65000),
    (T.Float64, 3.14159),
    (T.Boolean, True),
    (T.BitField, {1, 5, 30}),
    (T.UUID, None),
])
def test_fixed_codec_encode_into_grows_full_buffer(codec, value):
    out = _full_buffer()
    start = out.pos
    codec.encode_into(out, value)
    assert bytes(out.buf[start:out.pos]) == bytes(codec.encode(value))


def test_unsigned_varint32_encode_into_grows_full_buffer():
    # The originally-reported crash: buf[pos] = ... on a full buffer.
    for value in (0, 1, 127, 128, 16384, 2 ** 28):
        out = _full_buffer()
        start = out.pos
        T.UnsignedVarInt32.encode_into(out, value)
        assert out.pos > start  # wrote at least one byte without IndexError


@pytest.mark.parametrize('compact', [False, True])
def test_bytes_encode_into_grows_full_buffer(compact):
    out = _full_buffer()
    start = out.pos
    payload = b'z' * 5000
    T.Bytes.encode_into(out, payload, compact=compact)
    assert bytes(out.buf[start:out.pos]) == bytes(T.Bytes.encode(payload, compact=compact))


@pytest.mark.parametrize('compact', [False, True])
def test_string_encode_into_grows_full_buffer(compact):
    out = _full_buffer()
    start = out.pos
    s = T.String('utf-8')
    value = 'hello' * 1000  # < int16 length limit for the non-compact case
    s.encode_into(out, value, compact=compact)
    assert bytes(out.buf[start:out.pos]) == bytes(s.encode(value, compact=compact))


def test_ensure_reallocates_and_preserves_content():
    out = EncodeBuffer(size=8)
    out.buf[:4] = b'\x01\x02\x03\x04'
    out.pos = 4
    out.ensure(100)              # forces a grow
    assert len(out.buf) >= 104
    assert bytes(out.buf[:4]) == b'\x01\x02\x03\x04'


# ---------------------------------------------------------------------------
# End-to-end: compiled encode of large messages must be byte-for-byte equal to
# the reference encode and must decode back. These are the scenarios that
# crashed before the fix.
# ---------------------------------------------------------------------------

def _produce_v9(partitions):
    return ProduceRequest[9](
        transactional_id=None, acks=1, timeout_ms=1000,
        topic_data=[('t', partitions)])


def _produce_v3(partitions):
    return ProduceRequest[3](
        transactional_id=None, acks=1, timeout_ms=1000,
        topic_data=[('t', partitions)])


@pytest.mark.parametrize('size', [
    65530, 65532, 65536, 65537,        # around the initial 64KB buffer
    131068, 131072, 131074,            # around the first doubling (zero-slack)
    200000,
])
def test_produce_v9_large_records_parity(size):
    """Flexible Produce: large records followed by a tagged-field count.

    This is the originally-reported IndexError path.
    """
    m = _produce_v9([(0, b'X' * size)])
    assert _codegen_encode(m) == _reference_encode(m)


def test_produce_v9_multi_partition_large_records():
    """Two large partitions: the second partition's int32 index is written

    right after the first partition's tagged-field encode reallocated the
    buffer (the stale-`buf` regression). Must round-trip.
    """
    m = _produce_v9([(0, b'R' * 200_000), (1, b'S' * 200_000)])
    data = _codegen_encode(m)
    assert data == _reference_encode(m)
    decoded = ProduceRequest[9].decode(io.BytesIO(data), version=9)
    assert len(decoded.topic_data[0].partition_data) == 2


def test_produce_v3_multi_partition_large_records():
    """Non-flexible Produce (no tagged fields): the fixed-field-after-Bytes path."""
    m = _produce_v3([(0, b'R' * 200_000), (1, b'S' * 10)])
    assert _codegen_encode(m) == _reference_encode(m)


@pytest.mark.parametrize('count', [16000, 40000, 100000])
def test_offset_fetch_large_int32_array_parity(count):
    """A []int32 large enough to overflow 64KB - no Bytes, no tagged fields.

    Exercises the array fixed-element fast path's bulk reservation.
    """
    m = OffsetFetchRequest[1](group_id='g', topics=[('t', list(range(count)))])
    assert _codegen_encode(m) == _reference_encode(m)


def test_buffer_reuse_across_messages():
    """A pooled buffer that grew for a big message must still encode correctly
    when reused (reset) for subsequent messages of varying size."""
    out = EncodeBuffer()
    big = _produce_v9([(0, b'R' * 200_000)])
    small = _produce_v9([(0, b'hi'), (1, None)])
    for m in (big, small, big, small):
        out.reset()
        assert _codegen_encode(m, buf=out) == _reference_encode(m)
