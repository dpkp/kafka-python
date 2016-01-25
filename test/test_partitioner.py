import pytest
import six

from kafka.partitioner import Murmur2Partitioner
from kafka.partitioner.default import DefaultPartitioner


def test_default_partitioner():
    partitioner = DefaultPartitioner()
    all_partitions = list(range(100))
    available = all_partitions
    # partitioner should return the same partition for the same key
    p1 = partitioner(b'foo', all_partitions, available)
    p2 = partitioner(b'foo', all_partitions, available)
    assert p1 == p2
    assert p1 in all_partitions

    # when key is None, choose one of available partitions
    assert partitioner(None, all_partitions, [123]) == 123

    # with fallback to all_partitions
    assert partitioner(None, all_partitions, []) in all_partitions


def test_hash_bytes():
    p = Murmur2Partitioner(range(1000))
    assert p.partition(bytearray(b'test')) == p.partition(b'test')
    

def test_hash_encoding():
    p = Murmur2Partitioner(range(1000))
    assert p.partition('test') == p.partition(u'test')


def test_murmur2_java_compatibility():
    p = Murmur2Partitioner(range(1000))
    # compare with output from Kafka's org.apache.kafka.clients.producer.Partitioner
    assert p.partition(b'') == 681
    assert p.partition(b'a') == 524
    assert p.partition(b'ab') == 434
    assert p.partition(b'abc') == 107
    assert p.partition(b'123456789') == 566
    assert p.partition(b'\x00 ') == 742
