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


ProduceRequest = [ProduceRequest_v0]
ProduceResponse = [ProduceResponse_v0]
