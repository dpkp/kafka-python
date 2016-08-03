from __future__ import absolute_import

from .message import MessageSet
from .struct import Struct
from .types import Int16, Int32, Int64, String, Array, Schema


class ProduceResponse_v0(Struct):
    API_KEY = 0
    API_VERSION = 0
    SCHEMA = Schema(
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('error_code', Int16),
                ('offset', Int64)))))
    )


class ProduceResponse_v1(Struct):
    API_KEY = 0
    API_VERSION = 1
    SCHEMA = Schema(
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('error_code', Int16),
                ('offset', Int64))))),
        ('throttle_time_ms', Int32)
    )


class ProduceResponse_v2(Struct):
    API_KEY = 0
    API_VERSION = 2
    SCHEMA = Schema(
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('error_code', Int16),
                ('offset', Int64),
                ('timestamp', Int64))))),
        ('throttle_time_ms', Int32)
    )


class ProduceRequest_v0(Struct):
    API_KEY = 0
    API_VERSION = 0
    RESPONSE_TYPE = ProduceResponse_v0
    SCHEMA = Schema(
        ('required_acks', Int16),
        ('timeout', Int32),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('messages', MessageSet)))))
    )


class ProduceRequest_v1(Struct):
    API_KEY = 0
    API_VERSION = 1
    RESPONSE_TYPE = ProduceResponse_v1
    SCHEMA = ProduceRequest_v0.SCHEMA


class ProduceRequest_v2(Struct):
    API_KEY = 0
    API_VERSION = 2
    RESPONSE_TYPE = ProduceResponse_v2
    SCHEMA = ProduceRequest_v1.SCHEMA


ProduceRequest = [ProduceRequest_v0, ProduceRequest_v1, ProduceRequest_v2]
ProduceResponse = [ProduceResponse_v0, ProduceResponse_v1, ProduceResponse_v2]
