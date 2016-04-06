from .struct import Struct
from .types import Array, Bytes, Int16, Schema, String


class ListGroupsResponse_v0(Struct):
    API_KEY = 16
    API_VERSION = 0
    SCHEMA = Schema(
        ('error_code', Int16),
        ('groups', Array(
            ('group', String('utf-8')),
            ('protocol_type', String('utf-8'))))
    )


class ListGroupsRequest_v0(Struct):
    API_KEY = 16
    API_VERSION = 0
    RESPONSE_TYPE = ListGroupsResponse_v0
    SCHEMA = Schema()


ListGroupsRequest = [ListGroupsRequest_v0]
ListGroupsResponse = [ListGroupsResponse_v0]


class DescribeGroupsResponse_v0(Struct):
    API_KEY = 15
    API_VERSION = 0
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


class DescribeGroupsRequest_v0(Struct):
    API_KEY = 15
    API_VERSION = 0
    RESPONSE_TYPE = DescribeGroupsResponse_v0
    SCHEMA = Schema(
        ('groups', Array(String('utf-8')))
    )


DescribeGroupsRequest = [DescribeGroupsRequest_v0]
DescribeGroupsResponse = [DescribeGroupsResponse_v0]
