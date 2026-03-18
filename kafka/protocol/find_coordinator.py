from kafka.protocol.api import Request, Response
from kafka.protocol.types import Int8, Int16, Int32, Schema, String


class FindCoordinatorResponse_v0(Response):
    API_KEY = 10
    API_VERSION = 0
    SCHEMA = Schema(
        ('error_code', Int16),
        ('node_id', Int32),
        ('host', String('utf-8')),
        ('port', Int32)
    )
    ALIASES = {
        'coordinator_id': 'node_id',
    }


class FindCoordinatorResponse_v1(Response):
    API_KEY = 10
    API_VERSION = 1
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('error_code', Int16),
        ('error_message', String('utf-8')),
        ('node_id', Int32),
        ('host', String('utf-8')),
        ('port', Int32)
    )
    ALIASES = FindCoordinatorResponse_v0.ALIASES


class FindCoordinatorResponse_v2(Response):
    API_KEY = 10
    API_VERSION = 2
    SCHEMA = FindCoordinatorResponse_v1.SCHEMA
    ALIASES = FindCoordinatorResponse_v1.ALIASES


class FindCoordinatorRequest_v0(Request):
    API_KEY = 10
    API_VERSION = 0
    SCHEMA = Schema(
        ('key', String('utf-8'))
    )
    ALIASES = {
        'consumer_group': 'key',
    }


class FindCoordinatorRequest_v1(Request):
    API_KEY = 10
    API_VERSION = 1
    SCHEMA = Schema(
        ('key', String('utf-8')),
        ('key_type', Int8) # 0: consumer, 1: transaction
    )
    ALIASES = {
        'coordinator_key': 'key',
        'coordinator_type': 'key_type',
    }


class FindCoordinatorRequest_v2(Request):
    API_KEY = 10
    API_VERSION = 2
    SCHEMA = FindCoordinatorRequest_v1.SCHEMA
    ALIASES = FindCoordinatorRequest_v1.ALIASES


FindCoordinatorRequest = [FindCoordinatorRequest_v0, FindCoordinatorRequest_v1, FindCoordinatorRequest_v2]
FindCoordinatorResponse = [FindCoordinatorResponse_v0, FindCoordinatorResponse_v1, FindCoordinatorResponse_v2]
