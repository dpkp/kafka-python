import os
import random
import struct
import unittest

from kafka.client import KafkaClient
from kafka.common import ProduceRequest, FetchRequest
from kafka.codec import (
    has_gzip, has_snappy,
    gzip_encode, gzip_decode,
    snappy_encode, snappy_decode
)

ITERATIONS = 1000
STRLEN = 100


def random_string():
    return os.urandom(random.randint(1, STRLEN))


class TestPackage(unittest.TestCase):
    @unittest.expectedFailure
    def test_top_level_namespace(self):
        import kafka as kafka1
        self.assertEquals(kafka1.KafkaClient.__name__, "KafkaClient")
        self.assertEquals(kafka1.gzip_encode.__name__, "gzip_encode")
        self.assertEquals(kafka1.snappy_encode.__name__, "snappy_encode")
        self.assertEquals(kafka1.client.__name__, "kafka.client")
        self.assertEquals(kafka1.codec.__name__, "kafka.codec")

    @unittest.expectedFailure
    def test_submodule_namespace(self):
        import kafka.client as client1
        self.assertEquals(client1.__name__, "kafka.client")
        self.assertEquals(client1.KafkaClient.__name__, "KafkaClient")

        from kafka import client as client2
        self.assertEquals(client2.__name__, "kafka.client")
        self.assertEquals(client2.KafkaClient.__name__, "KafkaClient")

        from kafka.client import KafkaClient as KafkaClient1
        self.assertEquals(KafkaClient1.__name__, "KafkaClient")

        from kafka.codec import gzip_encode as gzip_encode1
        self.assertEquals(gzip_encode1.__name__, "gzip_encode")

        from kafka import KafkaClient as KafkaClient2
        self.assertEquals(KafkaClient2.__name__, "KafkaClient")

        from kafka import gzip_encode as gzip_encode2
        self.assertEquals(gzip_encode2.__name__, "gzip_encode")

        from kafka import snappy_encode as snappy_encode2
        self.assertEquals(snappy_encode2.__name__, "snappy_encode")


class TestMisc(unittest.TestCase):
    @unittest.expectedFailure
    def test_length_prefix(self):
        for i in xrange(ITERATIONS):
            s1 = random_string()
            self.assertEquals(struct.unpack('>i', s2[0:4])[0], len(s1))


class TestCodec(unittest.TestCase):
    def test_gzip(self):
        if not has_gzip():
            return
        for i in xrange(ITERATIONS):
            s1 = random_string()
            s2 = gzip_decode(gzip_encode(s1))
            self.assertEquals(s1, s2)

    def test_snappy(self):
        if not has_snappy():
            return
        for i in xrange(ITERATIONS):
            s1 = random_string()
            s2 = snappy_decode(snappy_encode(s1))
            self.assertEquals(s1, s2)


# XXX(sandello): These really should be protocol tests.
class TestMessage(unittest.TestCase):
    @unittest.expectedFailure
    def test_create(self):
        msg = KafkaClient.create_message("testing")
        self.assertEquals(msg.payload, "testing")
        self.assertEquals(msg.magic, 1)
        self.assertEquals(msg.attributes, 0)
        self.assertEquals(msg.crc, -386704890)

    @unittest.expectedFailure
    def test_create_gzip(self):
        msg = KafkaClient.create_gzip_message("testing")
        self.assertEquals(msg.magic, 1)
        self.assertEquals(msg.attributes, 1)
        # Can't check the crc or payload for gzip since it's non-deterministic
        (messages, _) = KafkaClient.read_message_set(gzip_decode(msg.payload))
        inner = messages[0]
        self.assertEquals(inner.magic, 1)
        self.assertEquals(inner.attributes, 0)
        self.assertEquals(inner.payload, "testing")
        self.assertEquals(inner.crc, -386704890)

    @unittest.expectedFailure
    def test_create_snappy(self):
        msg = KafkaClient.create_snappy_message("testing")
        self.assertEquals(msg.magic, 1)
        self.assertEquals(msg.attributes, 2)
        self.assertEquals(msg.crc, -62350868)
        (messages, _) = KafkaClient.read_message_set(snappy_decode(msg.payload))
        inner = messages[0]
        self.assertEquals(inner.magic, 1)
        self.assertEquals(inner.attributes, 0)
        self.assertEquals(inner.payload, "testing")
        self.assertEquals(inner.crc, -386704890)

    @unittest.expectedFailure
    def test_message_simple(self):
        msg = KafkaClient.create_message("testing")
        enc = KafkaClient.encode_message(msg)
        expect = "\x00\x00\x00\r\x01\x00\xe8\xf3Z\x06testing"
        self.assertEquals(enc, expect)
        (messages, read) = KafkaClient.read_message_set(enc)
        self.assertEquals(len(messages), 1)
        self.assertEquals(messages[0], msg)

    @unittest.expectedFailure
    def test_message_list(self):
        msgs = [
            KafkaClient.create_message("one"),
            KafkaClient.create_message("two"),
            KafkaClient.create_message("three")
        ]
        enc = KafkaClient.encode_message_set(msgs)
        expect = ("\x00\x00\x00\t\x01\x00zl\x86\xf1one\x00\x00\x00\t\x01\x00\x11"
                  "\xca\x8aftwo\x00\x00\x00\x0b\x01\x00F\xc5\xd8\xf5three")
        self.assertEquals(enc, expect)
        (messages, read) = KafkaClient.read_message_set(enc)
        self.assertEquals(len(messages), 3)
        self.assertEquals(messages[0].payload, "one")
        self.assertEquals(messages[1].payload, "two")
        self.assertEquals(messages[2].payload, "three")

    @unittest.expectedFailure
    def test_message_gzip(self):
        msg = KafkaClient.create_gzip_message("one", "two", "three")
        enc = KafkaClient.encode_message(msg)
        # Can't check the bytes directly since Gzip is non-deterministic
        (messages, read) = KafkaClient.read_message_set(enc)
        self.assertEquals(len(messages), 3)
        self.assertEquals(messages[0].payload, "one")
        self.assertEquals(messages[1].payload, "two")
        self.assertEquals(messages[2].payload, "three")

    @unittest.expectedFailure
    def test_message_snappy(self):
        msg = KafkaClient.create_snappy_message("one", "two", "three")
        enc = KafkaClient.encode_message(msg)
        (messages, read) = KafkaClient.read_message_set(enc)
        self.assertEquals(len(messages), 3)
        self.assertEquals(messages[0].payload, "one")
        self.assertEquals(messages[1].payload, "two")
        self.assertEquals(messages[2].payload, "three")

    @unittest.expectedFailure
    def test_message_simple_random(self):
        for i in xrange(ITERATIONS):
            n = random.randint(0, 10)
            msgs = [KafkaClient.create_message(random_string()) for j in range(n)]
            enc = KafkaClient.encode_message_set(msgs)
            (messages, read) = KafkaClient.read_message_set(enc)
            self.assertEquals(len(messages), n)
            for j in range(n):
                self.assertEquals(messages[j], msgs[j])

    @unittest.expectedFailure
    def test_message_gzip_random(self):
        for i in xrange(ITERATIONS):
            n = random.randint(1, 10)
            strings = [random_string() for j in range(n)]
            msg = KafkaClient.create_gzip_message(*strings)
            enc = KafkaClient.encode_message(msg)
            (messages, read) = KafkaClient.read_message_set(enc)
            self.assertEquals(len(messages), n)
            for j in range(n):
                self.assertEquals(messages[j].payload, strings[j])

    @unittest.expectedFailure
    def test_message_snappy_random(self):
        for i in xrange(ITERATIONS):
            n = random.randint(1, 10)
            strings = [random_string() for j in range(n)]
            msg = KafkaClient.create_snappy_message(*strings)
            enc = KafkaClient.encode_message(msg)
            (messages, read) = KafkaClient.read_message_set(enc)
            self.assertEquals(len(messages), n)
            for j in range(n):
                self.assertEquals(messages[j].payload, strings[j])


class TestRequests(unittest.TestCase):
    @unittest.expectedFailure
    def test_produce_request(self):
        req = ProduceRequest("my-topic", 0, [KafkaClient.create_message("testing")])
        enc = KafkaClient.encode_produce_request(req)
        expect = "\x00\x00\x00\x08my-topic\x00\x00\x00\x00\x00\x00\x00\x11\x00\x00\x00\r\x01\x00\xe8\xf3Z\x06testing"
        self.assertEquals(enc, expect)

    @unittest.expectedFailure
    def test_fetch_request(self):
        req = FetchRequest("my-topic", 0, 0, 1024)
        enc = KafkaClient.encode_fetch_request(req)
        expect = "\x00\x01\x00\x08my-topic\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x00"
        self.assertEquals(enc, expect)


if __name__ == '__main__':
    unittest.main()
