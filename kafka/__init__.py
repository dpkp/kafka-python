__title__ = 'kafka'
from kafka.version import __version__
__author__ = 'Dana Powers'
__license__ = 'Apache License 2.0'
__copyright__ = 'Copyright 2026 Dana Powers, David Arthur, and Contributors'

# Set default logging handler to avoid "No handler found" warnings.
import logging

logging.getLogger(__name__).addHandler(logging.NullHandler())


from kafka.admin import KafkaAdminClient
from kafka.consumer import KafkaConsumer
from kafka.consumer.subscription_state import (
    AsyncConsumerRebalanceListener, ConsumerRebalanceListener,
)
from kafka.producer import KafkaProducer
from kafka.serializer import DefaultSerializer, JsonSerializer, Serializer, Deserializer
from kafka.structs import (
    ConsumerGroupMetadata, OffsetAndMetadata,
    TopicPartition, TopicPartitionReplica,
)
from kafka.protocol.consumer import IsolationLevel, OffsetSpec


__all__ = [
    'KafkaAdminClient', 'KafkaConsumer', 'KafkaProducer',
    'AsyncConsumerRebalanceListener', 'ConsumerRebalanceListener',
    'DefaultSerializer', 'JsonSerializer', 'Serializer', 'Deserializer',
    'ConsumerGroupMetadata', 'OffsetAndMetadata',
    'TopicPartition', 'TopicPartitionReplica',
    'IsolationLevel', 'OffsetSpec',
]
