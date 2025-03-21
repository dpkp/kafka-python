from __future__ import absolute_import

from io import BytesIO

from kafka.protocol.api import Request, Response
from kafka.protocol.types import Array, CompactArray, CompactString, Int16, Int32, Schema, TaggedFields


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


class ApiVersionsResponse_v3(BaseApiVersionsResponse):
    API_KEY = 18
    API_VERSION = 3
    SCHEMA = Schema(
        ('error_code', Int16),
        ('api_versions', CompactArray(
            ('api_key', Int16),
            ('min_version', Int16),
            ('max_version', Int16),
            ('_tagged_fields', TaggedFields))),
        ('throttle_time_ms', Int32),
        ('_tagged_fields', TaggedFields)
    )
    # Note: ApiVersions Response does not send FLEXIBLE_VERSION header!


class ApiVersionsResponse_v4(BaseApiVersionsResponse):
    API_KEY = 18
    API_VERSION = 4
    SCHEMA = ApiVersionsResponse_v3.SCHEMA


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
    RESPONSE_TYPE = ApiVersionsResponse_v2
    SCHEMA = ApiVersionsRequest_v1.SCHEMA


class ApiVersionsRequest_v3(Request):
    API_KEY = 18
    API_VERSION = 3
    RESPONSE_TYPE = ApiVersionsResponse_v3
    SCHEMA = Schema(
        ('client_software_name', CompactString('utf-8')),
        ('client_software_version', CompactString('utf-8')),
        ('_tagged_fields', TaggedFields)
    )
    FLEXIBLE_VERSION = True


class ApiVersionsRequest_v4(Request):
    API_KEY = 18
    API_VERSION = 4
    RESPONSE_TYPE = ApiVersionsResponse_v4
    SCHEMA = ApiVersionsRequest_v3.SCHEMA
    FLEXIBLE_VERSION = True


ApiVersionsRequest = [
    ApiVersionsRequest_v0, ApiVersionsRequest_v1, ApiVersionsRequest_v2,
    ApiVersionsRequest_v3, ApiVersionsRequest_v4,
]
ApiVersionsResponse = [
    ApiVersionsResponse_v0, ApiVersionsResponse_v1, ApiVersionsResponse_v2,
    ApiVersionsResponse_v3, ApiVersionsResponse_v4,
]
