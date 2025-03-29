# pylint: skip-file
from __future__ import absolute_import

import pytest
import io

from kafka.errors import KafkaTimeoutError
from kafka.producer.future import FutureRecordMetadata, RecordMetadata
from kafka.producer.record_accumulator import RecordAccumulator, ProducerBatch
from kafka.record.memory_records import MemoryRecordsBuilder
from kafka.structs import TopicPartition


def test_producer_batch_producer_id():
    tp = TopicPartition('foo', 0)
    records = MemoryRecordsBuilder(
        magic=2, compression_type=0, batch_size=100000)
    batch = ProducerBatch(tp, records, io.BytesIO())
    assert batch.producer_id == -1
    batch.records.set_producer_state(123, 456, 789)
    assert batch.producer_id == 123
    records.close()
    assert batch.producer_id == 123

@pytest.mark.parametrize("magic", [0, 1, 2])
def test_producer_batch_try_append(magic):
    tp = TopicPartition('foo', 0)
    records = MemoryRecordsBuilder(
        magic=magic, compression_type=0, batch_size=100000)
    batch = ProducerBatch(tp, records, io.BytesIO())
    assert batch.record_count == 0
    future = batch.try_append(0, b'key', b'value', [])
    assert isinstance(future, FutureRecordMetadata)
    assert not future.is_done
    batch.done(base_offset=123, timestamp_ms=456, log_start_offset=0)
    assert future.is_done
    # record-level checksum only provided in v0/v1 formats; payload includes magic-byte
    if magic == 0:
        checksum = 592888119
    elif magic == 1:
        checksum = 213653215
    else:
        checksum = None

    expected_metadata = RecordMetadata(
        topic=tp[0], partition=tp[1], topic_partition=tp,
        offset=123, timestamp=456, log_start_offset=0,
        checksum=checksum,
        serialized_key_size=3, serialized_value_size=5, serialized_header_size=-1)
    assert future.value == expected_metadata

def test_producer_batch_retry():
    tp = TopicPartition('foo', 0)
    records = MemoryRecordsBuilder(
        magic=2, compression_type=0, batch_size=100000)
    batch = ProducerBatch(tp, records, io.BytesIO())
    assert not batch.in_retry()
    batch.set_retry()
    assert batch.in_retry()

def test_producer_batch_maybe_expire():
    tp = TopicPartition('foo', 0)
    records = MemoryRecordsBuilder(
        magic=2, compression_type=0, batch_size=100000)
    batch = ProducerBatch(tp, records, io.BytesIO(), now=1)
    future = batch.try_append(0, b'key', b'value', [], now=2)
    request_timeout_ms = 5000
    retry_backoff_ms = 200
    linger_ms = 1000
    is_full = True
    batch.maybe_expire(request_timeout_ms, retry_backoff_ms, linger_ms, is_full, now=20)
    assert batch.is_done
    assert future.is_done
    assert future.failed()
    assert isinstance(future.exception, KafkaTimeoutError)
