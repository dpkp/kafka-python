import struct

from six.moves import xrange
from . import unittest

from kafka.codec import (
    has_snappy, gzip_encode, gzip_decode,
    snappy_encode, snappy_decode
)

from test.testutil import random_string

class TestCodec(unittest.TestCase):
    def test_gzip(self):
        for i in xrange(1000):
            s1 = random_string(100)
            s2 = gzip_decode(gzip_encode(s1))
            self.assertEqual(s1, s2)

    @unittest.skipUnless(has_snappy(), "Snappy not available")
    def test_snappy(self):
        for i in xrange(1000):
            s1 = random_string(100)
            s2 = snappy_decode(snappy_encode(s1))
            self.assertEqual(s1, s2)

    @unittest.skipUnless(has_snappy(), "Snappy not available")
    def test_snappy_detect_xerial(self):
        import kafka as kafka1
        _detect_xerial_stream = kafka1.codec._detect_xerial_stream

        header = b'\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01Some extra bytes'
        false_header = b'\x01SNAPPY\x00\x00\x00\x01\x00\x00\x00\x01'
        random_snappy = snappy_encode(b'SNAPPY' * 50)
        short_data = b'\x01\x02\x03\x04'

        self.assertTrue(_detect_xerial_stream(header))
        self.assertFalse(_detect_xerial_stream(b''))
        self.assertFalse(_detect_xerial_stream(b'\x00'))
        self.assertFalse(_detect_xerial_stream(false_header))
        self.assertFalse(_detect_xerial_stream(random_snappy))
        self.assertFalse(_detect_xerial_stream(short_data))

    @unittest.skipUnless(has_snappy(), "Snappy not available")
    def test_snappy_decode_xerial(self):
        header = b'\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01'
        random_snappy = snappy_encode(b'SNAPPY' * 50)
        block_len = len(random_snappy)
        random_snappy2 = snappy_encode(b'XERIAL' * 50)
        block_len2 = len(random_snappy2)

        to_test = header \
            + struct.pack('!i', block_len) + random_snappy \
            + struct.pack('!i', block_len2) + random_snappy2 \

        self.assertEqual(snappy_decode(to_test), (b'SNAPPY' * 50) + (b'XERIAL' * 50))

    @unittest.skipUnless(has_snappy(), "Snappy not available")
    def test_snappy_encode_xerial(self):
        to_ensure = (
            b'\x82SNAPPY\x00\x00\x00\x00\x01\x00\x00\x00\x01'
            b'\x00\x00\x00\x18'
            b'\xac\x02\x14SNAPPY\xfe\x06\x00\xfe\x06\x00\xfe\x06\x00\xfe\x06\x00\x96\x06\x00'
            b'\x00\x00\x00\x18'
            b'\xac\x02\x14XERIAL\xfe\x06\x00\xfe\x06\x00\xfe\x06\x00\xfe\x06\x00\x96\x06\x00'
        )

        to_test = (b'SNAPPY' * 50) + (b'XERIAL' * 50)

        compressed = snappy_encode(to_test, xerial_compatible=True, xerial_blocksize=300)
        self.assertEqual(compressed, to_ensure)

