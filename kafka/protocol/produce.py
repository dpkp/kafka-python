from __future__ import absolute_import

from .api import Request, Response
from .message import MessageSet
from .types import Int16, Int32, Int64, String, Array, Schema


class ProduceResponse_v0(Response):
    API_KEY = 0
    API_VERSION = 0
    SCHEMA = Schema(
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('error_code', Int16),
                ('offset', Int64)))))
    )


class ProduceResponse_v1(Response):
    API_KEY = 0
    API_VERSION = 1
    SCHEMA = Schema(
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('error_code', Int16),
                ('offset', Int64))))),
        ('throttle_time_ms', Int32)
    )


class ProduceResponse_v2(Response):
    API_KEY = 0
    API_VERSION = 2
    SCHEMA = Schema(
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('error_code', Int16),
                ('offset', Int64),
                ('timestamp', Int64))))),
        ('throttle_time_ms', Int32)
    )


class ProduceResponse_v3(Response):
    API_KEY = 0
    API_VERSION = 3
    SCHEMA = ProduceResponse_v2.SCHEMA


class ProduceRequest_v0(Request):
    API_KEY = 0
    API_VERSION = 0
    RESPONSE_TYPE = ProduceResponse_v0
    SCHEMA = Schema(
        ('required_acks', Int16),
        ('timeout', Int32),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('messages', MessageSet)))))
    )

    def expect_response(self):
        if self.required_acks == 0: # pylint: disable=no-member
            return False
        return True


class ProduceRequest_v1(Request):
    API_KEY = 0
    API_VERSION = 1
    RESPONSE_TYPE = ProduceResponse_v1
    SCHEMA = ProduceRequest_v0.SCHEMA

    def expect_response(self):
        if self.required_acks == 0: # pylint: disable=no-member
            return False
        return True


class ProduceRequest_v2(Request):
    API_KEY = 0
    API_VERSION = 2
    RESPONSE_TYPE = ProduceResponse_v2
    SCHEMA = ProduceRequest_v1.SCHEMA

    def expect_response(self):
        if self.required_acks == 0: # pylint: disable=no-member
            return False
        return True


class ProduceRequest_v3(Request):
    API_KEY = 0
    API_VERSION = 3
    RESPONSE_TYPE = ProduceResponse_v3
    SCHEMA = Schema(
        ('transactional_id', String('utf-8')),
        ('required_acks', Int16),
        ('timeout', Int32),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('messages', MessageSet)))))
    )

    def expect_response(self):
        if self.required_acks == 0: # pylint: disable=no-member
            return False
        return True


ProduceRequest = [
    ProduceRequest_v0, ProduceRequest_v1, ProduceRequest_v2,
    ProduceRequest_v3
]
ProduceResponse = [
    ProduceResponse_v0, ProduceResponse_v1, ProduceResponse_v2,
    ProduceResponse_v2
]
