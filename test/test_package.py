from . import unittest


class TestPackage(unittest.TestCase):
    def test_top_level_namespace(self):
        import kafka as kafka1
        self.assertEqual(kafka1.KafkaConsumer.__name__, "KafkaConsumer")
        self.assertEqual(kafka1.consumer.__name__, "kafka.consumer")
        self.assertEqual(kafka1.codec.__name__, "kafka.codec")

    def test_submodule_namespace(self):
        import kafka.client as client1
        self.assertEqual(client1.__name__, "kafka.client")

        from kafka import client as client2
        self.assertEqual(client2.__name__, "kafka.client")

        from kafka.client import SimpleClient as SimpleClient1
        self.assertEqual(SimpleClient1.__name__, "SimpleClient")

        from kafka.codec import gzip_encode as gzip_encode1
        self.assertEqual(gzip_encode1.__name__, "gzip_encode")

        from kafka import SimpleClient as SimpleClient2
        self.assertEqual(SimpleClient2.__name__, "SimpleClient")

        from kafka.codec import snappy_encode
        self.assertEqual(snappy_encode.__name__, "snappy_encode")
