import os
import random
import struct
import unittest2

from mock import MagicMock, patch

from kafka import KafkaClient
from kafka.consumer import SimpleConsumer
from kafka.common import (
    ProduceRequest, BrokerMetadata, PartitionMetadata,
    TopicAndPartition, KafkaUnavailableError,
    LeaderUnavailableError, PartitionUnavailableError
)
from kafka.protocol import (
    create_message, KafkaProtocol
)

class TestKafkaConsumer(unittest2.TestCase):
    def test_non_integer_partitions(self):
        with self.assertRaises(AssertionError):
            consumer = SimpleConsumer(MagicMock(), 'group', 'topic', partitions = [ '0' ])
