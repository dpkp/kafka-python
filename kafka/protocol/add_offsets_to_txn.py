from __future__ import absolute_import

from kafka.protocol.api import Request, Response
from kafka.protocol.types import Int16, Int32, Int64, Schema, String


class AddOffsetsToTxnResponse_v0(Response):
    API_KEY = 25
    API_VERSION = 0
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('error_code', Int16),
    )


class AddOffsetsToTxnResponse_v1(Response):
    API_KEY = 25
    API_VERSION = 1
    SCHEMA = AddOffsetsToTxnResponse_v0.SCHEMA


class AddOffsetsToTxnResponse_v2(Response):
    API_KEY = 25
    API_VERSION = 2
    SCHEMA = AddOffsetsToTxnResponse_v1.SCHEMA


class AddOffsetsToTxnRequest_v0(Request):
    API_KEY = 25
    API_VERSION = 0
    RESPONSE_TYPE = AddOffsetsToTxnResponse_v0
    SCHEMA = Schema(
        ('transactional_id', String('utf-8')),
        ('producer_id', Int64),
        ('producer_epoch', Int16),
        ('group_id', String('utf-8')),
    )


class AddOffsetsToTxnRequest_v1(Request):
    API_KEY = 25
    API_VERSION = 1
    RESPONSE_TYPE = AddOffsetsToTxnResponse_v1
    SCHEMA = AddOffsetsToTxnRequest_v0.SCHEMA


class AddOffsetsToTxnRequest_v2(Request):
    API_KEY = 25
    API_VERSION = 2
    RESPONSE_TYPE = AddOffsetsToTxnResponse_v2
    SCHEMA = AddOffsetsToTxnRequest_v1.SCHEMA


AddOffsetsToTxnRequest = [
    AddOffsetsToTxnRequest_v0, AddOffsetsToTxnRequest_v1, AddOffsetsToTxnRequest_v2,
]
AddOffsetsToTxnResponse = [
    AddOffsetsToTxnResponse_v0, AddOffsetsToTxnResponse_v1, AddOffsetsToTxnResponse_v2,
]
