from __future__ import absolute_import

from kafka.protocol.api import Request, Response
from kafka.protocol.types import Int8, Int16, Int32, Schema, String


class FindCoordinatorResponse_v0(Response):
    API_KEY = 10
    API_VERSION = 0
    SCHEMA = Schema(
        ('error_code', Int16),
        ('coordinator_id', Int32),
        ('host', String('utf-8')),
        ('port', Int32)
    )


class FindCoordinatorResponse_v1(Response):
    API_KEY = 10
    API_VERSION = 1
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('error_code', Int16),
        ('error_message', String('utf-8')),
        ('coordinator_id', Int32),
        ('host', String('utf-8')),
        ('port', Int32)
    )


class FindCoordinatorResponse_v2(Response):
    API_KEY = 10
    API_VERSION = 2
    SCHEMA = FindCoordinatorResponse_v1.SCHEMA


class FindCoordinatorRequest_v0(Request):
    API_KEY = 10
    API_VERSION = 0
    RESPONSE_TYPE = FindCoordinatorResponse_v0
    SCHEMA = Schema(
        ('consumer_group', String('utf-8'))
    )


class FindCoordinatorRequest_v1(Request):
    API_KEY = 10
    API_VERSION = 1
    RESPONSE_TYPE = FindCoordinatorResponse_v1
    SCHEMA = Schema(
        ('coordinator_key', String('utf-8')),
        ('coordinator_type', Int8) # 0: consumer, 1: transaction
    )


class FindCoordinatorRequest_v2(Request):
    API_KEY = 10
    API_VERSION = 2
    RESPONSE_TYPE = FindCoordinatorResponse_v2
    SCHEMA = FindCoordinatorRequest_v1.SCHEMA


FindCoordinatorRequest = [FindCoordinatorRequest_v0, FindCoordinatorRequest_v1, FindCoordinatorRequest_v2]
FindCoordinatorResponse = [FindCoordinatorResponse_v0, FindCoordinatorResponse_v1, FindCoordinatorResponse_v2]
