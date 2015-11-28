from .struct import Struct
from .types import Array, Int16, Int32, Schema, String


class MetadataRequest(Struct):
    API_KEY = 3
    API_VERSION = 0
    SCHEMA = Schema(
        ('topics', Array(String('utf-8')))
    )


class MetadataResponse(Struct):
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
