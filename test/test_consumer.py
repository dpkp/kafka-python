
from mock import MagicMock, patch
from . import unittest

from kafka import SimpleConsumer, KafkaConsumer, MultiProcessConsumer
from kafka.common import KafkaConfigurationError

class TestKafkaConsumer(unittest.TestCase):
    def test_non_integer_partitions(self):
        with self.assertRaises(AssertionError):
            SimpleConsumer(MagicMock(), 'group', 'topic', partitions = [ '0' ])

    def test_broker_list_required(self):
        with self.assertRaises(KafkaConfigurationError):
            KafkaConsumer()

class TestMultiProcessConsumer(unittest.TestCase):
    def test_partition_list(self):
        client = MagicMock()
        partitions = (0,)
        with patch.object(MultiProcessConsumer, 'fetch_last_known_offsets') as fetch_last_known_offsets:
            consumer = MultiProcessConsumer(client, 'testing-group', 'testing-topic', partitions=partitions)
            self.assertEqual(fetch_last_known_offsets.call_args[0], (partitions,) )
        self.assertEqual(client.get_partition_ids_for_topic.call_count, 0) # pylint: disable=no-member
