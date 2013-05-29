__title__ = 'kafka'
__version__ = '0.2-alpha'
__author__ = 'David Arthur'
__license__ = 'Apache License 2.0'
__copyright__ = 'Copyright 2012, David Arthur under Apache License, v2.0'

from kafka.client import KafkaClient
from kafka.conn import KafkaConnection
from kafka.protocol import (
    create_message, create_gzip_message, create_snappy_message
)
from kafka.producer import SimpleProducer
from kafka.consumer import SimpleConsumer

__all__ = [
    'KafkaClient', 'KafkaConnection', 'SimpleProducer', 'SimpleConsumer',
    'create_message', 'create_gzip_message', 'create_snappy_message'
]
