from __future__ import absolute_import

__title__ = 'kafka'
from .version import __version__
__author__ = 'Dana Powers'
__license__ = 'Apache License 2.0'
__copyright__ = 'Copyright 2016 Dana Powers, David Arthur, and Contributors'

# Set default logging handler to avoid "No handler found" warnings.
import logging
try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

logging.getLogger(__name__).addHandler(NullHandler())


from kafka.consumer import KafkaConsumer
from kafka.consumer.subscription_state import ConsumerRebalanceListener
from kafka.producer import KafkaProducer
from kafka.conn import BrokerConnection
from kafka.protocol import (
    create_message, create_gzip_message, create_snappy_message)
from kafka.partitioner import RoundRobinPartitioner, HashedPartitioner, Murmur2Partitioner
from kafka.structs import TopicPartition, OffsetAndMetadata
from kafka.serializer import Serializer, Deserializer

# To be deprecated when KafkaProducer interface is released
from kafka.client import SimpleClient
from kafka.producer import SimpleProducer, KeyedProducer

# deprecated in favor of KafkaConsumer
from kafka.consumer import SimpleConsumer, MultiProcessConsumer


import warnings
class KafkaClient(SimpleClient):
    def __init__(self, *args, **kwargs):
        warnings.warn('The legacy KafkaClient interface has been moved to'
                      ' kafka.SimpleClient - this import will break in a'
                      ' future release', DeprecationWarning)
        super(KafkaClient, self).__init__(*args, **kwargs)


__all__ = [
    'KafkaConsumer', 'KafkaProducer', 'KafkaClient', 'BrokerConnection',
    'SimpleClient', 'SimpleProducer', 'KeyedProducer',
    'RoundRobinPartitioner', 'HashedPartitioner',
    'create_message', 'create_gzip_message', 'create_snappy_message',
    'SimpleConsumer', 'MultiProcessConsumer',
]
