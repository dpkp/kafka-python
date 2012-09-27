import binascii
import unittest

from kafka import KafkaClient

class TestMessage(unittest.TestCase):
    def test_message_simple(self):
        msg = KafkaClient.create_message("testing")
        enc = KafkaClient.encode_message(msg)
        expect = "\x00\x00\x00\r\x01\x00\xe8\xf3Z\x06testing"
        self.assertEquals(enc, expect)
        (messages, read) = KafkaClient.read_message_set(enc)
        self.assertEquals(len(messages), 1)
        self.assertEquals(messages[0], msg)

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
         

    def test_message_gzip(self):
        msg = KafkaClient.create_gzip_message("one", "two", "three")
        enc = KafkaClient.encode_message(msg)
        # Can't check the bytes directly since Gzip is non-deterministic
        (messages, read) = KafkaClient.read_message_set(enc)
        self.assertEquals(len(messages), 3)
        self.assertEquals(messages[0].payload, "one")
        self.assertEquals(messages[1].payload, "two")
        self.assertEquals(messages[2].payload, "three")

if __name__ == '__main__':
    unittest.main()
