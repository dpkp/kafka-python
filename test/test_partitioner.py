from __future__ import absolute_import

import pytest


from kafka.partitioner import DefaultPartitioner, murmur2
from kafka.partitioner.fnv1a_32 import FNV1a32Partitioner, _get_twos_complement_32bit, _get_fnv1a_32


def test_default_partitioner():
    partitioner = DefaultPartitioner()
    all_partitions = available = list(range(100))
    # partitioner should return the same partition for the same key
    p1 = partitioner(b'foo', all_partitions, available)
    p2 = partitioner(b'foo', all_partitions, available)
    assert p1 == p2
    assert p1 in all_partitions

    # when key is None, choose one of available partitions
    assert partitioner(None, all_partitions, [123]) == 123

    # with fallback to all_partitions
    assert partitioner(None, all_partitions, []) in all_partitions


@pytest.mark.parametrize("bytes_payload,partition_number", [
    (b'', 681), (b'a', 524), (b'ab', 434), (b'abc', 107), (b'123456789', 566),
    (b'\x00 ', 742)
])
def test_murmur2_java_compatibility(bytes_payload, partition_number):
    partitioner = DefaultPartitioner()
    all_partitions = available = list(range(1000))
    # compare with output from Kafka's org.apache.kafka.clients.producer.Partitioner
    assert partitioner(bytes_payload, all_partitions, available) == partition_number


def test_murmur2_not_ascii():
    # Verify no regression of murmur2() bug encoding py2 bytes that don't ascii encode
    murmur2(b'\xa4')
    murmur2(b'\x81' * 1000)


@pytest.mark.parametrize("key,partitions,available,expected", [
    (b"123", [0, 1, 2], [0, 1, 2], 2),
    (b"123", [0, 1], [0, 1], 1),
    (b"123", [0], [0], 0),
    (b"f232oo3232", [0, 1, 2, 3], [0, 1, 2, 3], 2),
    (b"f232oo3232", [0, 1], [0, 1], 0),
    (b"f232oo3232", [0], [0], 0),
])
def test_fnv1a_32_partitioner(key, partitions, available, expected):
    partitioner = FNV1a32Partitioner()
    out = partitioner(key, partitions, available)
    assert out == expected
