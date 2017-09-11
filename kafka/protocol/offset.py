from __future__ import absolute_import

from .api import Request, Response
from .types import Array, Int8, Int16, Int32, Int64, Schema, String

UNKNOWN_OFFSET = -1


class OffsetResetStrategy(object):
    LATEST = -1
    EARLIEST = -2
    NONE = 0


class OffsetResponse_v0(Response):
    API_KEY = 2
    API_VERSION = 0
    SCHEMA = Schema(
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('error_code', Int16),
                ('offsets', Array(Int64))))))
    )

class OffsetResponse_v1(Response):
    API_KEY = 2
    API_VERSION = 1
    SCHEMA = Schema(
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('error_code', Int16),
                ('timestamp', Int64),
                ('offset', Int64)))))
    )


class OffsetResponse_v2(Response):
    API_KEY = 2
    API_VERSION = 2
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('error_code', Int16),
                ('timestamp', Int64),
                ('offset', Int64)))))
    )


class OffsetRequest_v0(Request):
    API_KEY = 2
    API_VERSION = 0
    RESPONSE_TYPE = OffsetResponse_v0
    SCHEMA = Schema(
        ('replica_id', Int32),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('timestamp', Int64),
                ('max_offsets', Int32)))))
    )
    DEFAULTS = {
        'replica_id': -1
    }

class OffsetRequest_v1(Request):
    API_KEY = 2
    API_VERSION = 1
    RESPONSE_TYPE = OffsetResponse_v1
    SCHEMA = Schema(
        ('replica_id', Int32),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('timestamp', Int64)))))
    )
    DEFAULTS = {
        'replica_id': -1
    }


class OffsetRequest_v2(Request):
    API_KEY = 2
    API_VERSION = 2
    RESPONSE_TYPE = OffsetResponse_v2
    SCHEMA = Schema(
        ('replica_id', Int32),
        ('isolation_level', Int8),  # <- added isolation_level
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('timestamp', Int64)))))
    )
    DEFAULTS = {
        'replica_id': -1
    }


OffsetRequest = [OffsetRequest_v0, OffsetRequest_v1, OffsetRequest_v2]
OffsetResponse = [OffsetResponse_v0, OffsetResponse_v1, OffsetResponse_v2]
