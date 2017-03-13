from __future__ import absolute_import

from .api import Request, Response
from .types import Array, Boolean, Bytes, Int16, Int32, Schema, String


class ApiVersionResponse_v0(Response):
    API_KEY = 18
    API_VERSION = 0
    SCHEMA = Schema(
        ('error_code', Int16),
        ('api_versions', Array(
            ('api_key', Int16),
            ('min_version', Int16),
            ('max_version', Int16)))
    )


class ApiVersionRequest_v0(Request):
    API_KEY = 18
    API_VERSION = 0
    RESPONSE_TYPE = ApiVersionResponse_v0
    SCHEMA = Schema()


ApiVersionRequest = [ApiVersionRequest_v0]
ApiVersionResponse = [ApiVersionResponse_v0]


class CreateTopicsResponse_v0(Response):
    API_KEY = 19
    API_VERSION = 0
    SCHEMA = Schema(
        ('topic_error_codes', Array(
            ('topic', String('utf-8')),
            ('error_code', Int16)))
    )


class CreateTopicsResponse_v1(Response):
    API_KEY = 19
    API_VERSION = 1
    SCHEMA = Schema(
        ('topic_error_codes', Array(
            ('topic', String('utf-8')),
            ('error_code', Int16),
            ('error_message', String('utf-8'))))
    )


class CreateTopicsRequest_v0(Request):
    API_KEY = 19
    API_VERSION = 0
    RESPONSE_TYPE = CreateTopicsResponse_v0
    SCHEMA = Schema(
        ('create_topic_requests', Array(
            ('topic', String('utf-8')),
            ('num_partitions', Int32),
            ('replication_factor', Int16),
            ('replica_assignment', Array(
                ('partition_id', Int32),
                ('replicas', Array(Int32)))),
            ('configs', Array(
                ('config_key', String('utf-8')),
                ('config_value', String('utf-8')))))),
        ('timeout', Int32)
    )


class CreateTopicsRequest_v1(Request):
    API_KEY = 19
    API_VERSION = 1
    RESPONSE_TYPE = CreateTopicsResponse_v1
    SCHEMA = Schema(
        ('create_topic_requests', Array(
            ('topic', String('utf-8')),
            ('num_partitions', Int32),
            ('replication_factor', Int16),
            ('replica_assignment', Array(
                ('partition_id', Int32),
                ('replicas', Array(Int32)))),
            ('configs', Array(
                ('config_key', String('utf-8')),
                ('config_value', String('utf-8')))))),
        ('timeout', Int32),
        ('validate_only', Boolean)
    )


CreateTopicsRequest = [CreateTopicsRequest_v0, CreateTopicsRequest_v1]
CreateTopicsResponse = [CreateTopicsResponse_v0, CreateTopicsRequest_v1]


class DeleteTopicsResponse_v0(Response):
    API_KEY = 20
    API_VERSION = 0
    SCHEMA = Schema(
        ('topic_error_codes', Array(
            ('topic', String('utf-8')),
            ('error_code', Int16)))
    )


class DeleteTopicsRequest_v0(Request):
    API_KEY = 20
    API_VERSION = 0
    RESPONSE_TYPE = DeleteTopicsResponse_v0
    SCHEMA = Schema(
        ('topics', Array(String('utf-8'))),
        ('timeout', Int32)
    )


DeleteTopicsRequest = [DeleteTopicsRequest_v0]
DeleteTopicsResponse = [DeleteTopicsResponse_v0]


class ListGroupsResponse_v0(Response):
    API_KEY = 16
    API_VERSION = 0
    SCHEMA = Schema(
        ('error_code', Int16),
        ('groups', Array(
            ('group', String('utf-8')),
            ('protocol_type', String('utf-8'))))
    )


class ListGroupsRequest_v0(Request):
    API_KEY = 16
    API_VERSION = 0
    RESPONSE_TYPE = ListGroupsResponse_v0
    SCHEMA = Schema()


ListGroupsRequest = [ListGroupsRequest_v0]
ListGroupsResponse = [ListGroupsResponse_v0]


class DescribeGroupsResponse_v0(Response):
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


class DescribeGroupsRequest_v0(Request):
    API_KEY = 15
    API_VERSION = 0
    RESPONSE_TYPE = DescribeGroupsResponse_v0
    SCHEMA = Schema(
        ('groups', Array(String('utf-8')))
    )


DescribeGroupsRequest = [DescribeGroupsRequest_v0]
DescribeGroupsResponse = [DescribeGroupsResponse_v0]


class SaslHandShakeResponse_v0(Response):
    API_KEY = 17
    API_VERSION = 0
    SCHEMA = Schema(
        ('error_code', Int16),
        ('enabled_mechanisms', Array(String('utf-8')))
    )


class SaslHandShakeRequest_v0(Request):
    API_KEY = 17
    API_VERSION = 0
    RESPONSE_TYPE = SaslHandShakeResponse_v0
    SCHEMA = Schema(
        ('mechanism', String('utf-8'))
    )

SaslHandShakeRequest = [SaslHandShakeRequest_v0]
SaslHandShakeResponse = [SaslHandShakeResponse_v0]
