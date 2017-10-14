import pytest
from kafka.record import MemoryRecords
from kafka.errors import CorruptRecordException

record_batch_data_v1 = [
    # First Message value == "123"
    b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x19G\x86(\xc2\x01\x00\x00'
    b'\x00\x01^\x18g\xab\xae\xff\xff\xff\xff\x00\x00\x00\x03123',
    # Second Message value == ""
    b'\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x16\xef\x98\xc9 \x01\x00'
    b'\x00\x00\x01^\x18g\xaf\xc0\xff\xff\xff\xff\x00\x00\x00\x00',
    # Third Message value == ""
    b'\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x16_\xaf\xfb^\x01\x00\x00'
    b'\x00\x01^\x18g\xb0r\xff\xff\xff\xff\x00\x00\x00\x00',
    # Fourth Message value = "123"
    b'\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x19\xa8\x12W \x01\x00\x00'
    b'\x00\x01^\x18g\xb8\x03\xff\xff\xff\xff\x00\x00\x00\x03123'
]

# This is real live data from Kafka 10 broker
record_batch_data_v0 = [
    # First Message value == "123"
    b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x11\xfe\xb0\x1d\xbf\x00'
    b'\x00\xff\xff\xff\xff\x00\x00\x00\x03123',
    # Second Message value == ""
    b'\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x0eyWH\xe0\x00\x00\xff'
    b'\xff\xff\xff\x00\x00\x00\x00',
    # Third Message value == ""
    b'\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00\x00\x0eyWH\xe0\x00\x00\xff'
    b'\xff\xff\xff\x00\x00\x00\x00',
    # Fourth Message value = "123"
    b'\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x11\xfe\xb0\x1d\xbf\x00'
    b'\x00\xff\xff\xff\xff\x00\x00\x00\x03123'
]


def test_memory_records_v1():
    data_bytes = b"".join(record_batch_data_v1) + b"\x00" * 4
    records = MemoryRecords(data_bytes)

    assert records.size_in_bytes() == 146
    assert records.valid_bytes() == 142

    assert records.has_next() is True
    batch = records.next_batch()
    recs = list(batch)
    assert len(recs) == 1
    assert recs[0].value == b"123"
    assert recs[0].key is None
    assert recs[0].timestamp == 1503648000942
    assert recs[0].timestamp_type == 0
    assert recs[0].checksum == 1199974594 & 0xffffffff

    assert records.next_batch() is not None
    assert records.next_batch() is not None
    assert records.next_batch() is not None

    assert records.has_next() is False
    assert records.next_batch() is None
    assert records.next_batch() is None


def test_memory_records_v0():
    data_bytes = b"".join(record_batch_data_v0)
    records = MemoryRecords(data_bytes + b"\x00" * 4)

    assert records.size_in_bytes() == 114
    assert records.valid_bytes() == 110

    records = MemoryRecords(data_bytes)

    assert records.has_next() is True
    batch = records.next_batch()
    recs = list(batch)
    assert len(recs) == 1
    assert recs[0].value == b"123"
    assert recs[0].key is None
    assert recs[0].timestamp is None
    assert recs[0].timestamp_type is None
    assert recs[0].checksum == -22012481 & 0xffffffff

    assert records.next_batch() is not None
    assert records.next_batch() is not None
    assert records.next_batch() is not None

    assert records.has_next() is False
    assert records.next_batch() is None
    assert records.next_batch() is None


def test_memory_records_corrupt():
    records = MemoryRecords(b"")
    assert records.size_in_bytes() == 0
    assert records.valid_bytes() == 0
    assert records.has_next() is False

    records = MemoryRecords(b"\x00\x00\x00")
    assert records.size_in_bytes() == 3
    assert records.valid_bytes() == 0
    assert records.has_next() is False

    records = MemoryRecords(
        b"\x00\x00\x00\x00\x00\x00\x00\x03"  # Offset=3
        b"\x00\x00\x00\x03"  # Length=3
        b"\xfe\xb0\x1d",  # Some random bytes
    )
    with pytest.raises(CorruptRecordException):
        records.next_batch()
