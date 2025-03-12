from __future__ import absolute_import

from kafka.protocol.api import Request, Response
from kafka.protocol.types import Array, Int8, Int16, Int32, Int64, Schema, String

UNKNOWN_OFFSET = -1


class OffsetResetStrategy(object):
    LATEST = -1
    EARLIEST = -2
    NONE = 0


class ListOffsetsResponse_v0(Response):
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

class ListOffsetsResponse_v1(Response):
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


class ListOffsetsResponse_v2(Response):
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


class ListOffsetsResponse_v3(Response):
    """
    on quota violation, brokers send out responses before throttling
    """
    API_KEY = 2
    API_VERSION = 3
    SCHEMA = ListOffsetsResponse_v2.SCHEMA


class ListOffsetsResponse_v4(Response):
    """
    Add leader_epoch to response
    """
    API_KEY = 2
    API_VERSION = 4
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('error_code', Int16),
                ('timestamp', Int64),
                ('offset', Int64),
                ('leader_epoch', Int32)))))
    )


class ListOffsetsResponse_v5(Response):
    """
    adds a new error code, OFFSET_NOT_AVAILABLE
    """
    API_KEY = 2
    API_VERSION = 5
    SCHEMA = ListOffsetsResponse_v4.SCHEMA


class ListOffsetsRequest_v0(Request):
    API_KEY = 2
    API_VERSION = 0
    RESPONSE_TYPE = ListOffsetsResponse_v0
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

class ListOffsetsRequest_v1(Request):
    API_KEY = 2
    API_VERSION = 1
    RESPONSE_TYPE = ListOffsetsResponse_v1
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


class ListOffsetsRequest_v2(Request):
    API_KEY = 2
    API_VERSION = 2
    RESPONSE_TYPE = ListOffsetsResponse_v2
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


class ListOffsetsRequest_v3(Request):
    API_KEY = 2
    API_VERSION = 3
    RESPONSE_TYPE = ListOffsetsResponse_v3
    SCHEMA = ListOffsetsRequest_v2.SCHEMA
    DEFAULTS = {
        'replica_id': -1
    }


class ListOffsetsRequest_v4(Request):
    """
    Add current_leader_epoch to request
    """
    API_KEY = 2
    API_VERSION = 4
    RESPONSE_TYPE = ListOffsetsResponse_v4
    SCHEMA = Schema(
        ('replica_id', Int32),
        ('isolation_level', Int8),  # <- added isolation_level
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('current_leader_epoch', Int32),
                ('timestamp', Int64)))))
    )
    DEFAULTS = {
        'replica_id': -1
    }


class ListOffsetsRequest_v5(Request):
    API_KEY = 2
    API_VERSION = 5
    RESPONSE_TYPE = ListOffsetsResponse_v5
    SCHEMA = ListOffsetsRequest_v4.SCHEMA
    DEFAULTS = {
        'replica_id': -1
    }


ListOffsetsRequest = [
    ListOffsetsRequest_v0, ListOffsetsRequest_v1, ListOffsetsRequest_v2,
    ListOffsetsRequest_v3, ListOffsetsRequest_v4, ListOffsetsRequest_v5,
]
ListOffsetsResponse = [
    ListOffsetsResponse_v0, ListOffsetsResponse_v1, ListOffsetsResponse_v2,
    ListOffsetsResponse_v3, ListOffsetsResponse_v4, ListOffsetsResponse_v5,
]
