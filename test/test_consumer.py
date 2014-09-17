
from mock import MagicMock
from . import unittest

from kafka import SimpleConsumer, KafkaConsumer
from kafka.common import KafkaConfigurationError

class TestKafkaConsumer(unittest.TestCase):
    def test_non_integer_partitions(self):
        with self.assertRaises(AssertionError):
            SimpleConsumer(MagicMock(), 'group', 'topic', partitions = [ '0' ])

    def test_broker_list_required(self):
        with self.assertRaises(KafkaConfigurationError):
            KafkaConsumer()
