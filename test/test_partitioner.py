import six
from . import unittest

from kafka.partitioner import (Murmur2Partitioner)

class TestMurmurPartitioner(unittest.TestCase):
    def test_hash_bytes(self):
        p = Murmur2Partitioner(range(1000))
        self.assertEqual(p.partition(bytearray(b'test')), p.partition(b'test'))

    def test_hash_encoding(self):
        p = Murmur2Partitioner(range(1000))
        self.assertEqual(p.partition('test'), p.partition(u'test'))

    def test_murmur2_java_compatibility(self):
        p = Murmur2Partitioner(range(1000))
        # compare with output from Kafka's org.apache.kafka.clients.producer.Partitioner
        self.assertEqual(681, p.partition(b''))
        self.assertEqual(524, p.partition(b'a'))
        self.assertEqual(434, p.partition(b'ab'))
        self.assertEqual(107, p.partition(b'abc'))
        self.assertEqual(566, p.partition(b'123456789'))
        self.assertEqual(742, p.partition(b'\x00 '))
