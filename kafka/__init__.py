__title__ = 'kafka'
__version__ = '0.9.0'
__author__ = 'David Arthur'
__license__ = 'Apache License 2.0'
__copyright__ = 'Copyright 2012, David Arthur under Apache License, v2.0'

from kafka.client import KafkaClient
from kafka.conn import KafkaConnection
from kafka.protocol import (
    create_message, create_gzip_message, create_snappy_message
)
from kafka.producer import SimpleProducer, KeyedProducer
from kafka.partitioner import RoundRobinPartitioner, HashedPartitioner
from kafka.consumer import SimpleConsumer, MultiProcessConsumer

class Kafka081Client(KafkaClient):
    server_version = "0.8.1"

class Kafka080Client(KafkaClient):
    server_version = "0.8.0"

    def simple_consumer(self, group, topic, **kwargs):
        assert not kwargs.get('auto_commit')
        kwargs['auto_commit'] = False

        return super(Kafka080Client, self).simple_consumer(group, topic, **kwargs)

    def multiprocess_consumer(self, group, topic, **kwargs):
        assert not kwargs.get('auto_commit')
        kwargs['auto_commit'] = False

        return super(Kafka080Client, self).multiprocess_consumer(group, topic, **kwargs)

__all__ = [
    'KafkaClient', 'Kafka080Client', 'Kafka081Client', 'KafkaConnection', 'SimpleProducer', 'KeyedProducer',
    'RoundRobinPartitioner', 'HashedPartitioner', 'SimpleConsumer',
    'MultiProcessConsumer', 'create_message', 'create_gzip_message',
    'create_snappy_message'
]
