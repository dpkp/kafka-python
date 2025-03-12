from __future__ import absolute_import

from kafka.protocol.api import Request, Response
from kafka.protocol.types import Array, CompactArray, CompactString, Int16, Int32, Int64, Schema, String, TaggedFields


class OffsetForLeaderEpochResponse_v0(Response):
    API_KEY = 23
    API_VERSION = 0
    SCHEMA = Schema(
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('error_code', Int16),
                ('partition', Int32),
                ('end_offset', Int64))))))


class OffsetForLeaderEpochResponse_v1(Response):
    API_KEY = 23
    API_VERSION = 1
    SCHEMA = Schema(
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('error_code', Int16),
                ('partition', Int32),
                ('leader_epoch', Int32),
                ('end_offset', Int64))))))


class OffsetForLeaderEpochResponse_v2(Response):
    API_KEY = 23
    API_VERSION = 2
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('error_code', Int16),
                ('partition', Int32),
                ('leader_epoch', Int32),
                ('end_offset', Int64))))))


class OffsetForLeaderEpochResponse_v3(Response):
    API_KEY = 23
    API_VERSION = 3
    SCHEMA = OffsetForLeaderEpochResponse_v2.SCHEMA


class OffsetForLeaderEpochResponse_v4(Response):
    API_KEY = 23
    API_VERSION = 4
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('topics', CompactArray(
            ('topic', CompactString('utf-8')),
            ('partitions', CompactArray(
                ('error_code', Int16),
                ('partition', Int32),
                ('leader_epoch', Int32),
                ('end_offset', Int64),
                ('tags', TaggedFields))),
            ('tags', TaggedFields))),
        ('tags', TaggedFields))


class OffsetForLeaderEpochRequest_v0(Request):
    API_KEY = 23
    API_VERSION = 0
    RESPONSE_TYPE = OffsetForLeaderEpochResponse_v0
    SCHEMA = Schema(
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('leader_epoch', Int32))))))


class OffsetForLeaderEpochRequest_v1(Request):
    API_KEY = 23
    API_VERSION = 1
    RESPONSE_TYPE = OffsetForLeaderEpochResponse_v1
    SCHEMA = OffsetForLeaderEpochRequest_v0.SCHEMA


class OffsetForLeaderEpochRequest_v2(Request):
    API_KEY = 23
    API_VERSION = 2
    RESPONSE_TYPE = OffsetForLeaderEpochResponse_v2
    SCHEMA = Schema(
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('current_leader_epoch', Int32),
                ('leader_epoch', Int32))))))


class OffsetForLeaderEpochRequest_v3(Request):
    API_KEY = 23
    API_VERSION = 3
    RESPONSE_TYPE = OffsetForLeaderEpochResponse_v3
    SCHEMA = Schema(
        ('replica_id', Int32),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('current_leader_epoch', Int32),
                ('leader_epoch', Int32))))))


class OffsetForLeaderEpochRequest_v4(Request):
    API_KEY = 23
    API_VERSION = 4
    RESPONSE_TYPE = OffsetForLeaderEpochResponse_v4
    SCHEMA = Schema(
        ('replica_id', Int32),
        ('topics', CompactArray(
            ('topic', CompactString('utf-8')),
            ('partitions', CompactArray(
                ('partition', Int32),
                ('current_leader_epoch', Int32),
                ('leader_epoch', Int32),
                ('tags', TaggedFields))),
            ('tags', TaggedFields))),
        ('tags', TaggedFields))

OffsetForLeaderEpochRequest = [
    OffsetForLeaderEpochRequest_v0, OffsetForLeaderEpochRequest_v1,
    OffsetForLeaderEpochRequest_v2, OffsetForLeaderEpochRequest_v3,
    OffsetForLeaderEpochRequest_v4,
]
OffsetForLeaderEpochResponse = [
    OffsetForLeaderEpochResponse_v0, OffsetForLeaderEpochResponse_v1,
    OffsetForLeaderEpochResponse_v2, OffsetForLeaderEpochResponse_v3,
    OffsetForLeaderEpochResponse_v4,
]
