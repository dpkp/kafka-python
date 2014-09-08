
from mock import MagicMock
from . import unittest

from kafka.consumer import SimpleConsumer

class TestKafkaConsumer(unittest.TestCase):
    def test_non_integer_partitions(self):
        with self.assertRaises(AssertionError):
            SimpleConsumer(MagicMock(), 'group', 'topic', partitions = [ '0' ])
