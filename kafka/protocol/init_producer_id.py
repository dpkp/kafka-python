from __future__ import absolute_import

from kafka.protocol.api import Request, Response
from kafka.protocol.types import Int16, Int32, Int64, Schema, String


class InitProducerIdResponse_v0(Response):
    API_KEY = 22
    API_VERSION = 0
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('error_code', Int16),
        ('producer_id', Int64),
        ('producer_epoch', Int16),
    )


class InitProducerIdResponse_v1(Response):
    API_KEY = 22
    API_VERSION = 1
    SCHEMA = InitProducerIdResponse_v0.SCHEMA


class InitProducerIdRequest_v0(Request):
    API_KEY = 22
    API_VERSION = 0
    RESPONSE_TYPE = InitProducerIdResponse_v0
    SCHEMA = Schema(
        ('transactional_id', String('utf-8')),
        ('transaction_timeout_ms', Int32),
    )


class InitProducerIdRequest_v1(Request):
    API_KEY = 22
    API_VERSION = 1
    RESPONSE_TYPE = InitProducerIdResponse_v1
    SCHEMA = InitProducerIdRequest_v0.SCHEMA


InitProducerIdRequest = [
    InitProducerIdRequest_v0, InitProducerIdRequest_v1,
]
InitProducerIdResponse = [
    InitProducerIdResponse_v0, InitProducerIdResponse_v1,
]
