from __future__ import absolute_import

from .struct import Struct
from .types import Array, Boolean, Int16, Int32, Schema, String


class MetadataResponse_v0(Struct):
    API_KEY = 3
    API_VERSION = 0
    SCHEMA = Schema(
        ('brokers', Array(
            ('node_id', Int32),
            ('host', String('utf-8')),
            ('port', Int32))),
        ('topics', Array(
            ('error_code', Int16),
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('error_code', Int16),
                ('partition', Int32),
                ('leader', Int32),
                ('replicas', Array(Int32)),
                ('isr', Array(Int32))))))
    )


class MetadataResponse_v1(Struct):
    API_KEY = 3
    API_VERSION = 1
    SCHEMA = Schema(
        ('brokers', Array(
            ('node_id', Int32),
            ('host', String('utf-8')),
            ('port', Int32),
            ('rack', String('utf-8')))),
        ('controller_id', Int32),
        ('topics', Array(
            ('error_code', Int16),
            ('topic', String('utf-8')),
            ('is_internal', Boolean),
            ('partitions', Array(
                ('error_code', Int16),
                ('partition', Int32),
                ('leader', Int32),
                ('replicas', Array(Int32)),
                ('isr', Array(Int32))))))
    )


class MetadataRequest_v0(Struct):
    API_KEY = 3
    API_VERSION = 0
    RESPONSE_TYPE = MetadataResponse_v0
    SCHEMA = Schema(
        ('topics', Array(String('utf-8'))) # Empty Array (len 0) for all topics
    )


class MetadataRequest_v1(Struct):
    API_KEY = 3
    API_VERSION = 1
    RESPONSE_TYPE = MetadataResponse_v1
    SCHEMA = Schema(
        ('topics', Array(String('utf-8'))) # Null Array (len -1) for all topics
    )


MetadataRequest = [MetadataRequest_v0, MetadataRequest_v1]
MetadataResponse = [MetadataResponse_v0, MetadataResponse_v1]
