import pytest
from kafka.record.legacy_records import (
    LegacyRecordBatch, LegacyRecordBatchBuilder
)
from kafka.protocol.message import Message


@pytest.mark.parametrize("magic", [0, 1])
def test_read_write_serde_v0_v1_no_compression(magic):
    builder = LegacyRecordBatchBuilder(
        magic=magic, compression_type=0, batch_size=9999999)
    builder.append(
        0, timestamp=9999999, key=b"test", value=b"Super")
    buffer = builder.build()

    batch = LegacyRecordBatch(bytes(buffer), magic)
    msgs = list(batch)
    assert len(msgs) == 1
    msg = msgs[0]

    assert msg.offset == 0
    assert msg.timestamp == (9999999 if magic else None)
    assert msg.timestamp_type == (0 if magic else None)
    assert msg.key == b"test"
    assert msg.value == b"Super"
    assert msg.checksum == (-2095076219 if magic else 278251978) & 0xffffffff


@pytest.mark.parametrize("compression_type", [
    Message.CODEC_GZIP,
    Message.CODEC_SNAPPY,
    Message.CODEC_LZ4
])
@pytest.mark.parametrize("magic", [0, 1])
def test_read_write_serde_v0_v1_with_compression(compression_type, magic):
    builder = LegacyRecordBatchBuilder(
        magic=magic, compression_type=compression_type, batch_size=9999999)
    for offset in range(10):
        builder.append(
            offset, timestamp=9999999, key=b"test", value=b"Super")
    buffer = builder.build()

    batch = LegacyRecordBatch(bytes(buffer), magic)
    msgs = list(batch)

    expected_checksum = (-2095076219 if magic else 278251978) & 0xffffffff
    for offset, msg in enumerate(msgs):
        assert msg.offset == offset
        assert msg.timestamp == (9999999 if magic else None)
        assert msg.timestamp_type == (0 if magic else None)
        assert msg.key == b"test"
        assert msg.value == b"Super"
        assert msg.checksum == expected_checksum


@pytest.mark.parametrize("magic", [0, 1])
def test_written_bytes_equals_size_in_bytes(magic):
    key = b"test"
    value = b"Super"
    builder = LegacyRecordBatchBuilder(
        magic=magic, compression_type=0, batch_size=9999999)

    size_in_bytes = builder.size_in_bytes(
        0, timestamp=9999999, key=key, value=value)

    pos = builder.size()
    builder.append(0, timestamp=9999999, key=key, value=value)

    assert builder.size() - pos == size_in_bytes


@pytest.mark.parametrize("magic", [0, 1])
def test_estimate_size_in_bytes_bigger_than_batch(magic):
    key = b"Super Key"
    value = b"1" * 100
    estimate_size = LegacyRecordBatchBuilder.estimate_size_in_bytes(
        magic, compression_type=0, key=key, value=value)

    builder = LegacyRecordBatchBuilder(
        magic=magic, compression_type=0, batch_size=9999999)
    builder.append(
        0, timestamp=9999999, key=key, value=value)
    buf = builder.build()
    assert len(buf) <= estimate_size, \
        "Estimate should always be upper bound"
