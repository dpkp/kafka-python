from __future__ import absolute_import

from kafka.protocol.api import Request, Response
from kafka.protocol.types import Boolean, Int16, Int32, Int64, Schema, String


class EndTxnResponse_v0(Response):
    API_KEY = 26
    API_VERSION = 0
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('error_code', Int16),
    )


class EndTxnResponse_v1(Response):
    API_KEY = 26
    API_VERSION = 1
    SCHEMA = EndTxnResponse_v0.SCHEMA


class EndTxnResponse_v2(Response):
    API_KEY = 26
    API_VERSION = 2
    SCHEMA = EndTxnResponse_v1.SCHEMA


class EndTxnRequest_v0(Request):
    API_KEY = 26
    API_VERSION = 0
    RESPONSE_TYPE = EndTxnResponse_v0
    SCHEMA = Schema(
        ('transactional_id', String('utf-8')),
        ('producer_id', Int64),
        ('producer_epoch', Int16),
        ('committed', Boolean))


class EndTxnRequest_v1(Request):
    API_KEY = 26
    API_VERSION = 1
    RESPONSE_TYPE = EndTxnResponse_v1
    SCHEMA = EndTxnRequest_v0.SCHEMA


class EndTxnRequest_v2(Request):
    API_KEY = 26
    API_VERSION = 2
    RESPONSE_TYPE = EndTxnResponse_v2
    SCHEMA = EndTxnRequest_v1.SCHEMA


EndTxnRequest = [
    EndTxnRequest_v0, EndTxnRequest_v1, EndTxnRequest_v2,
]
EndTxnResponse = [
    EndTxnResponse_v0, EndTxnResponse_v1, EndTxnResponse_v2,
]
