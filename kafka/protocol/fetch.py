from .message import MessageSet
from .struct import Struct
from .types import Array, Int16, Int32, Int64, Schema, String


class FetchResponse(Struct):
    SCHEMA = Schema(
        ('topics', Array(
            ('topics', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('error_code', Int16),
                ('highwater_offset', Int64),
                ('message_set', MessageSet)))))
    )


class FetchRequest(Struct):
    API_KEY = 1
    API_VERSION = 0
    RESPONSE_TYPE = FetchResponse
    SCHEMA = Schema(
        ('replica_id', Int32),
        ('max_wait_time', Int32),
        ('min_bytes', Int32),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('offset', Int64),
                ('max_bytes', Int32)))))
    )
