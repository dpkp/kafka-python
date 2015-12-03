from .struct import Struct
from .types import Array, Bytes, Int16, Schema, String


class ListGroupsResponse(Struct):
    SCHEMA = Schema(
        ('error_code', Int16),
        ('groups', Array(
            ('group', String('utf-8')),
            ('protocol_type', String('utf-8'))))
    )


class ListGroupsRequest(Struct):
    API_KEY = 16
    API_VERSION = 0
    RESPONSE_TYPE = ListGroupsResponse
    SCHEMA = Schema()


class DescribeGroupsResponse(Struct):
    SCHEMA = Schema(
        ('groups', Array(
            ('error_code', Int16),
            ('group', String('utf-8')),
            ('state', String('utf-8')),
            ('protocol_type', String('utf-8')),
            ('protocol', String('utf-8')),
            ('members', Array(
                ('member_id', String('utf-8')),
                ('client_id', String('utf-8')),
                ('client_host', String('utf-8')),
                ('member_metadata', Bytes),
                ('member_assignment', Bytes)))))
    )


class DescribeGroupsRequest(Struct):
    API_KEY = 15
    API_VERSION = 0
    RESPONSE_TYPE = DescribeGroupsResponse
    SCHEMA = Schema(
        ('groups', Array(String('utf-8')))
    )
