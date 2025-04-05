from __future__ import absolute_import

from kafka.protocol.api import Request, Response
from kafka.protocol.types import Array, Int16, Int32, Int64, Schema, String


class AddPartitionsToTxnResponse_v0(Response):
    API_KEY = 24
    API_VERSION = 0
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('results', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('error_code', Int16))))))


class AddPartitionsToTxnResponse_v1(Response):
    API_KEY = 24
    API_VERSION = 1
    SCHEMA = AddPartitionsToTxnResponse_v0.SCHEMA


class AddPartitionsToTxnResponse_v2(Response):
    API_KEY = 24
    API_VERSION = 2
    SCHEMA = AddPartitionsToTxnResponse_v1.SCHEMA


class AddPartitionsToTxnRequest_v0(Request):
    API_KEY = 24
    API_VERSION = 0
    RESPONSE_TYPE = AddPartitionsToTxnResponse_v0
    SCHEMA = Schema(
        ('transactional_id', String('utf-8')),
        ('producer_id', Int64),
        ('producer_epoch', Int16),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(Int32)))))


class AddPartitionsToTxnRequest_v1(Request):
    API_KEY = 24
    API_VERSION = 1
    RESPONSE_TYPE = AddPartitionsToTxnResponse_v1
    SCHEMA = AddPartitionsToTxnRequest_v0.SCHEMA


class AddPartitionsToTxnRequest_v2(Request):
    API_KEY = 24
    API_VERSION = 2
    RESPONSE_TYPE = AddPartitionsToTxnResponse_v2
    SCHEMA = AddPartitionsToTxnRequest_v1.SCHEMA


AddPartitionsToTxnRequest = [
    AddPartitionsToTxnRequest_v0, AddPartitionsToTxnRequest_v1, AddPartitionsToTxnRequest_v2,
]
AddPartitionsToTxnResponse = [
    AddPartitionsToTxnResponse_v0, AddPartitionsToTxnResponse_v1, AddPartitionsToTxnResponse_v2,
]
