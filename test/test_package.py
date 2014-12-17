from . import unittest

class TestPackage(unittest.TestCase):
    def test_top_level_namespace(self):
        import kafka as kafka1
        self.assertEqual(kafka1.KafkaClient.__name__, "KafkaClient")
        self.assertEqual(kafka1.client.__name__, "kafka.client")
        self.assertEqual(kafka1.codec.__name__, "kafka.codec")

    def test_submodule_namespace(self):
        import kafka.client as client1
        self.assertEqual(client1.__name__, "kafka.client")
        self.assertEqual(client1.KafkaClient.__name__, "KafkaClient")

        from kafka import client as client2
        self.assertEqual(client2.__name__, "kafka.client")
        self.assertEqual(client2.KafkaClient.__name__, "KafkaClient")

        from kafka.client import KafkaClient as KafkaClient1
        self.assertEqual(KafkaClient1.__name__, "KafkaClient")

        from kafka.codec import gzip_encode as gzip_encode1
        self.assertEqual(gzip_encode1.__name__, "gzip_encode")

        from kafka import KafkaClient as KafkaClient2
        self.assertEqual(KafkaClient2.__name__, "KafkaClient")

        from kafka.codec import snappy_encode
        self.assertEqual(snappy_encode.__name__, "snappy_encode")
