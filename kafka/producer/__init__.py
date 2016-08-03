from __future__ import absolute_import

from .kafka import KafkaProducer
from .simple import SimpleProducer
from .keyed import KeyedProducer

__all__ = [
    'KafkaProducer',
    'SimpleProducer', 'KeyedProducer' # deprecated
]
