# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import pytest
from mock import patch
import kafka.codec
from kafka.record.default_records import (
    DefaultRecordBatch, DefaultRecordBatchBuilder
)
from kafka.errors import UnsupportedCodecError


@pytest.mark.parametrize("compression_type", [
    DefaultRecordBatch.CODEC_NONE,
    DefaultRecordBatch.CODEC_GZIP,
    DefaultRecordBatch.CODEC_SNAPPY,
    DefaultRecordBatch.CODEC_LZ4
])
def test_read_write_serde_v2(compression_type):
    builder = DefaultRecordBatchBuilder(
        magic=2, compression_type=compression_type, is_transactional=1,
        producer_id=123456, producer_epoch=123, base_sequence=9999,
        batch_size=999999)
    headers = [("header1", b"aaa"), ("header2", b"bbb")]
    for offset in range(10):
        builder.append(
            offset, timestamp=9999999, key=b"test", value=b"Super",
            headers=headers)
    buffer = builder.build()
    reader = DefaultRecordBatch(bytes(buffer))
    msgs = list(reader)

    assert reader.is_transactional is True
    assert reader.compression_type == compression_type
    assert reader.magic == 2
    assert reader.timestamp_type == 0
    assert reader.base_offset == 0
    for offset, msg in enumerate(msgs):
        assert msg.offset == offset
        assert msg.timestamp == 9999999
        assert msg.key == b"test"
        assert msg.value == b"Super"
        assert msg.headers == headers


def test_written_bytes_equals_size_in_bytes_v2():
    key = b"test"
    value = b"Super"
    headers = [("header1", b"aaa"), ("header2", b"bbb"), ("xx", None)]
    builder = DefaultRecordBatchBuilder(
        magic=2, compression_type=0, is_transactional=0,
        producer_id=-1, producer_epoch=-1, base_sequence=-1,
        batch_size=999999)

    size_in_bytes = builder.size_in_bytes(
        0, timestamp=9999999, key=key, value=value, headers=headers)

    pos = builder.size()
    meta = builder.append(
        0, timestamp=9999999, key=key, value=value, headers=headers)

    assert builder.size() - pos == size_in_bytes
    assert meta.size == size_in_bytes


def test_estimate_size_in_bytes_bigger_than_batch_v2():
    key = b"Super Key"
    value = b"1" * 100
    headers = [("header1", b"aaa"), ("header2", b"bbb")]
    estimate_size = DefaultRecordBatchBuilder.estimate_size_in_bytes(
        key, value, headers)

    builder = DefaultRecordBatchBuilder(
        magic=2, compression_type=0, is_transactional=0,
        producer_id=-1, producer_epoch=-1, base_sequence=-1,
        batch_size=999999)
    builder.append(
        0, timestamp=9999999, key=key, value=value, headers=headers)
    buf = builder.build()
    assert len(buf) <= estimate_size, \
        "Estimate should always be upper bound"


def test_default_batch_builder_validates_arguments():
    builder = DefaultRecordBatchBuilder(
        magic=2, compression_type=0, is_transactional=0,
        producer_id=-1, producer_epoch=-1, base_sequence=-1,
        batch_size=999999)

    # Key should not be str
    with pytest.raises(TypeError):
        builder.append(
            0, timestamp=9999999, key="some string", value=None, headers=[])

    # Value should not be str
    with pytest.raises(TypeError):
        builder.append(
            0, timestamp=9999999, key=None, value="some string", headers=[])

    # Timestamp should be of proper type
    with pytest.raises(TypeError):
        builder.append(
            0, timestamp="1243812793", key=None, value=b"some string",
            headers=[])

    # Offset of invalid type
    with pytest.raises(TypeError):
        builder.append(
            "0", timestamp=9999999, key=None, value=b"some string", headers=[])

    # Ok to pass value as None
    builder.append(
        0, timestamp=9999999, key=b"123", value=None, headers=[])

    # Timestamp can be None
    builder.append(
        1, timestamp=None, key=None, value=b"some string", headers=[])

    # Ok to pass offsets in not incremental order. This should not happen thou
    builder.append(
        5, timestamp=9999999, key=b"123", value=None, headers=[])

    # Check record with headers
    builder.append(
        6, timestamp=9999999, key=b"234", value=None, headers=[("hkey", b"hval")])

    # in case error handling code fails to fix inner buffer in builder
    assert len(builder.build()) == 124


def test_default_correct_metadata_response():
    builder = DefaultRecordBatchBuilder(
        magic=2, compression_type=0, is_transactional=0,
        producer_id=-1, producer_epoch=-1, base_sequence=-1,
        batch_size=1024 * 1024)
    meta = builder.append(
        0, timestamp=9999999, key=b"test", value=b"Super", headers=[])

    assert meta.offset == 0
    assert meta.timestamp == 9999999
    assert meta.crc is None
    assert meta.size == 16
    assert repr(meta) == (
        "DefaultRecordMetadata(offset=0, size={}, timestamp={})"
        .format(meta.size, meta.timestamp)
    )


def test_default_batch_size_limit():
    # First message can be added even if it's too big
    builder = DefaultRecordBatchBuilder(
        magic=2, compression_type=0, is_transactional=0,
        producer_id=-1, producer_epoch=-1, base_sequence=-1,
        batch_size=1024)

    meta = builder.append(
        0, timestamp=None, key=None, value=b"M" * 2000, headers=[])
    assert meta.size > 0
    assert meta.crc is None
    assert meta.offset == 0
    assert meta.timestamp is not None
    assert len(builder.build()) > 2000

    builder = DefaultRecordBatchBuilder(
        magic=2, compression_type=0, is_transactional=0,
        producer_id=-1, producer_epoch=-1, base_sequence=-1,
        batch_size=1024)
    meta = builder.append(
        0, timestamp=None, key=None, value=b"M" * 700, headers=[])
    assert meta is not None
    meta = builder.append(
        1, timestamp=None, key=None, value=b"M" * 700, headers=[])
    assert meta is None
    meta = builder.append(
        2, timestamp=None, key=None, value=b"M" * 700, headers=[])
    assert meta is None
    assert len(builder.build()) < 1000


@pytest.mark.parametrize("compression_type,name,checker_name", [
    (DefaultRecordBatch.CODEC_GZIP, "gzip", "has_gzip"),
    (DefaultRecordBatch.CODEC_SNAPPY, "snappy", "has_snappy"),
    (DefaultRecordBatch.CODEC_LZ4, "lz4", "has_lz4")
])
@pytest.mark.parametrize("magic", [0, 1])
def test_unavailable_codec(magic, compression_type, name, checker_name):
    builder = DefaultRecordBatchBuilder(
        magic=2, compression_type=compression_type, is_transactional=0,
        producer_id=-1, producer_epoch=-1, base_sequence=-1,
        batch_size=1024)
    builder.append(0, timestamp=None, key=None, value=b"M" * 2000, headers=[])
    correct_buffer = builder.build()

    with patch.object(kafka.codec, checker_name) as mocked:
        mocked.return_value = False
        # Check that builder raises error
        builder = DefaultRecordBatchBuilder(
            magic=2, compression_type=compression_type, is_transactional=0,
            producer_id=-1, producer_epoch=-1, base_sequence=-1,
            batch_size=1024)
        error_msg = "Libraries for {} compression codec not found".format(name)
        with pytest.raises(UnsupportedCodecError, match=error_msg):
            builder.append(0, timestamp=None, key=None, value=b"M", headers=[])
            builder.build()

        # Check that reader raises same error
        batch = DefaultRecordBatch(bytes(correct_buffer))
        with pytest.raises(UnsupportedCodecError, match=error_msg):
            list(batch)
