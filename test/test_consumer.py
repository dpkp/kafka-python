import unittest2

from mock import MagicMock

from kafka.consumer import SimpleConsumer

class TestKafkaConsumer(unittest2.TestCase):
    def test_non_integer_partitions(self):
        with self.assertRaises(AssertionError):
            SimpleConsumer(MagicMock(), 'group', 'topic', partitions = [ '0' ])
