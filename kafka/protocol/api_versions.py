from __future__ import absolute_import

from kafka.protocol.api import Request, Response
from kafka.protocol.types import Array, Int16, Int32, Schema


class BaseApiVersionsResponse(Response):
    API_KEY = 18
    API_VERSION = 0
    SCHEMA = Schema(
        ('error_code', Int16),
        ('api_versions', Array(
            ('api_key', Int16),
            ('min_version', Int16),
            ('max_version', Int16)))
    )

    @classmethod
    def decode(cls, data):
        if isinstance(data, bytes):
            data = BytesIO(data)
        # Check error_code, decode as v0 if any error
        curr = data.tell()
        err = Int16.decode(data)
        data.seek(curr)
        if err != 0:
            return ApiVersionsResponse_v0.decode(data)
        return super(BaseApiVersionsResponse, cls).decode(data)


class ApiVersionsResponse_v0(Response):
    API_KEY = 18
    API_VERSION = 0
    SCHEMA = Schema(
        ('error_code', Int16),
        ('api_versions', Array(
            ('api_key', Int16),
            ('min_version', Int16),
            ('max_version', Int16)))
    )


class ApiVersionsResponse_v1(BaseApiVersionsResponse):
    API_KEY = 18
    API_VERSION = 1
    SCHEMA = Schema(
        ('error_code', Int16),
        ('api_versions', Array(
            ('api_key', Int16),
            ('min_version', Int16),
            ('max_version', Int16))),
        ('throttle_time_ms', Int32)
    )


class ApiVersionsResponse_v2(BaseApiVersionsResponse):
    API_KEY = 18
    API_VERSION = 2
    SCHEMA = ApiVersionsResponse_v1.SCHEMA


class ApiVersionsRequest_v0(Request):
    API_KEY = 18
    API_VERSION = 0
    RESPONSE_TYPE = ApiVersionsResponse_v0
    SCHEMA = Schema()


class ApiVersionsRequest_v1(Request):
    API_KEY = 18
    API_VERSION = 1
    RESPONSE_TYPE = ApiVersionsResponse_v1
    SCHEMA = ApiVersionsRequest_v0.SCHEMA


class ApiVersionsRequest_v2(Request):
    API_KEY = 18
    API_VERSION = 2
    RESPONSE_TYPE = ApiVersionsResponse_v1
    SCHEMA = ApiVersionsRequest_v0.SCHEMA


ApiVersionsRequest = [
    ApiVersionsRequest_v0, ApiVersionsRequest_v1, ApiVersionsRequest_v2,
]
ApiVersionsResponse = [
    ApiVersionsResponse_v0, ApiVersionsResponse_v1, ApiVersionsResponse_v2,
]


OLD_BROKER_API_VERSIONS = {
    (0, 8, 0): {
        0: (0, 0),
        1: (0, 0),
        2: (0, 0),
        3: (0, 0),
    },
    (0, 8, 1): {  # adds offset commit + fetch
        0: (0, 0),
        1: (0, 0),
        2: (0, 0),
        3: (0, 0),
        8: (0, 0),
        9: (0, 0),
    },
    (0, 8, 2): {  # adds find coordinator
        0: (0, 0),
        1: (0, 0),
        2: (0, 0),
        3: (0, 0),
        8: (0, 1),
        9: (0, 1),
        10: (0, 0),
    },
    (0, 9): {  # adds group management (join/sync/leave/heartbeat)
        0: (0, 1),
        1: (0, 1),
        2: (0, 0),
        3: (0, 0),
        8: (0, 2),
        9: (0, 1),
        10: (0, 0),
        11: (0, 0),
        12: (0, 0),
        13: (0, 0),
        14: (0, 0),
        15: (0, 0),
        16: (0, 0),
    },
    (0, 10): { # adds sasl + api versions [pulled from api_versions response]
        0: (0, 2),
        1: (0, 2),
        2: (0, 0),
        3: (0, 1),
        4: (0, 0),
        5: (0, 0),
        6: (0, 2),
        7: (1, 1),
        8: (0, 2),
        9: (0, 1),
        10: (0, 0),
        11: (0, 0),
        12: (0, 0),
        13: (0, 0),
        14: (0, 0),
        15: (0, 0),
        16: (0, 0),
        17: (0, 0),
        18: (0, 0),
    },
}
