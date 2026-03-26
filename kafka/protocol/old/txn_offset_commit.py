from .api import Request, Response
from .types import Array, Int16, Int32, Int64, Schema, String


class TxnOffsetCommitResponse_v0(Response):
    API_KEY = 28
    API_VERSION = 0
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('topics', Array(
            ('name', String('utf-8')),
            ('partitions', Array(
                ('partition_index', Int32),
                ('error_code', Int16))))))


class TxnOffsetCommitResponse_v1(Response):
    API_KEY = 28
    API_VERSION = 1
    SCHEMA = TxnOffsetCommitResponse_v0.SCHEMA


class TxnOffsetCommitResponse_v2(Response):
    API_KEY = 28
    API_VERSION = 2
    SCHEMA = TxnOffsetCommitResponse_v1.SCHEMA


class TxnOffsetCommitRequest_v0(Request):
    API_KEY = 28
    API_VERSION = 0
    SCHEMA = Schema(
        ('transactional_id', String('utf-8')),
        ('group_id', String('utf-8')),
        ('producer_id', Int64),
        ('producer_epoch', Int16),
        ('topics', Array(
            ('name', String('utf-8')),
            ('partitions', Array(
                ('partition_index', Int32),
                ('committed_offset', Int64),
                ('committed_metadata', String('utf-8')))))))


class TxnOffsetCommitRequest_v1(Request):
    API_KEY = 28
    API_VERSION = 1
    SCHEMA = TxnOffsetCommitRequest_v0.SCHEMA


class TxnOffsetCommitRequest_v2(Request):
    API_KEY = 28
    API_VERSION = 2
    SCHEMA = Schema(
        ('transactional_id', String('utf-8')),
        ('group_id', String('utf-8')),
        ('producer_id', Int64),
        ('producer_epoch', Int16),
        ('topics', Array(
            ('name', String('utf-8')),
            ('partitions', Array(
                ('partition_index', Int32),
                ('committed_offset', Int64),
                ('committed_leader_epoch', Int32),
                ('committed_metadata', String('utf-8')))))))


TxnOffsetCommitRequest = [
    TxnOffsetCommitRequest_v0, TxnOffsetCommitRequest_v1, TxnOffsetCommitRequest_v2, 
]
TxnOffsetCommitResponse = [
    TxnOffsetCommitResponse_v0, TxnOffsetCommitResponse_v1, TxnOffsetCommitResponse_v2, 
]
