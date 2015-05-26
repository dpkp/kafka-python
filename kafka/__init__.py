__title__ = 'kafka'
# Use setuptools to get version from setup.py
import pkg_resources
__version__ = pkg_resources.require('kafka-python')[0].version
__author__ = 'David Arthur'
__license__ = 'Apache License 2.0'
__copyright__ = 'Copyright 2015, David Arthur under Apache License, v2.0'

from kafka.client import KafkaClient
from kafka.conn import KafkaConnection
from kafka.protocol import (
    create_message, create_gzip_message, create_snappy_message
)
from kafka.producer import SimpleProducer, KeyedProducer, KafkaProducer
from kafka.partitioner import RoundRobinPartitioner, HashedPartitioner
from kafka.consumer import SimpleConsumer, MultiProcessConsumer, KafkaConsumer

__all__ = [
    'KafkaClient', 'KafkaConnection', 'SimpleProducer', 'KeyedProducer',
    'RoundRobinPartitioner', 'HashedPartitioner', 'SimpleConsumer',
    'MultiProcessConsumer', 'create_message', 'create_gzip_message',
    'create_snappy_message', 'KafkaConsumer', 'KafkaProducer'
]
