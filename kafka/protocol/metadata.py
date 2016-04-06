from .struct import Struct
from .types import Array, Int16, Int32, Schema, String


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


class MetadataRequest_v0(Struct):
    API_KEY = 3
    API_VERSION = 0
    RESPONSE_TYPE = MetadataResponse_v0
    SCHEMA = Schema(
        ('topics', Array(String('utf-8')))
    )


MetadataRequest = [MetadataRequest_v0]
MetadataResponse = [MetadataResponse_v0]
