from .struct import Struct
from .types import Array, Int16, Int32, Int64, Schema, String

class OffsetResetStrategy(object):
    LATEST = -1
    EARLIEST = -2
    NONE = 0


class OffsetResponse(Struct):
    SCHEMA = Schema(
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('error_code', Int16),
                ('offsets', Array(Int64))))))
    )


class OffsetRequest(Struct):
    API_KEY = 2
    API_VERSION = 0
    RESPONSE_TYPE = OffsetResponse
    SCHEMA = Schema(
        ('replica_id', Int32),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('time', Int64),
                ('max_offsets', Int32)))))
    )
    DEFAULTS = {
        'replica_id': -1
    }
