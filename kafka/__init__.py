__title__ = 'kafka'
__version__ = '0.1-alpha'
__author__ = 'David Arthur'
__license__ = 'Apache License 2.0'
__copyright__ = 'Copyright 2012, David Arthur under Apache License, v2.0'

from .client import (
    KafkaClient, KafkaException,
    Message, ProduceRequest, FetchRequest, OffsetRequest
)
from .codec import gzip_encode, gzip_decode
from .codec import snappy_encode, snappy_decode
