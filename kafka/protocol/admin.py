from __future__ import absolute_import

from kafka.protocol.api import Request, Response
from kafka.protocol.types import Array, Boolean, Bytes, Int16, Int32, Schema, String


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


class ApiVersionResponse_v1(Response):
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


class ApiVersionRequest_v0(Request):
    API_KEY = 18
    API_VERSION = 0
    RESPONSE_TYPE = ApiVersionResponse_v0
    SCHEMA = Schema()


class ApiVersionRequest_v1(Request):
    API_KEY = 18
    API_VERSION = 1
    RESPONSE_TYPE = ApiVersionResponse_v1
    SCHEMA = ApiVersionRequest_v0.SCHEMA


ApiVersionRequest = [ApiVersionRequest_v0, ApiVersionRequest_v1]
ApiVersionResponse = [ApiVersionResponse_v0, ApiVersionResponse_v1]


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


class CreateTopicsResponse_v2(Response):
    API_KEY = 19
    API_VERSION = 2
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
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


class CreateTopicsRequest_v2(Request):
    API_KEY = 19
    API_VERSION = 2
    RESPONSE_TYPE = CreateTopicsResponse_v2
    SCHEMA = CreateTopicsRequest_v1.SCHEMA


CreateTopicsRequest = [
    CreateTopicsRequest_v0, CreateTopicsRequest_v1, CreateTopicsRequest_v2
]
CreateTopicsResponse = [
    CreateTopicsResponse_v0, CreateTopicsResponse_v1, CreateTopicsResponse_v2
]


class DeleteTopicsResponse_v0(Response):
    API_KEY = 20
    API_VERSION = 0
    SCHEMA = Schema(
        ('topic_error_codes', Array(
            ('topic', String('utf-8')),
            ('error_code', Int16)))
    )


class DeleteTopicsResponse_v1(Response):
    API_KEY = 20
    API_VERSION = 1
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
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


class DeleteTopicsRequest_v1(Request):
    API_KEY = 20
    API_VERSION = 1
    RESPONSE_TYPE = DeleteTopicsResponse_v1
    SCHEMA = DeleteTopicsRequest_v0.SCHEMA


DeleteTopicsRequest = [DeleteTopicsRequest_v0, DeleteTopicsRequest_v1]
DeleteTopicsResponse = [DeleteTopicsResponse_v0, DeleteTopicsResponse_v1]


class ListGroupsResponse_v0(Response):
    API_KEY = 16
    API_VERSION = 0
    SCHEMA = Schema(
        ('error_code', Int16),
        ('groups', Array(
            ('group', String('utf-8')),
            ('protocol_type', String('utf-8'))))
    )


class ListGroupsResponse_v1(Response):
    API_KEY = 16
    API_VERSION = 1
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
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


class ListGroupsRequest_v1(Request):
    API_KEY = 16
    API_VERSION = 1
    RESPONSE_TYPE = ListGroupsResponse_v1
    SCHEMA = ListGroupsRequest_v0.SCHEMA


ListGroupsRequest = [ListGroupsRequest_v0, ListGroupsRequest_v1]
ListGroupsResponse = [ListGroupsResponse_v0, ListGroupsResponse_v1]


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


class DescribeGroupsResponse_v1(Response):
    API_KEY = 15
    API_VERSION = 1
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
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


class DescribeGroupsRequest_v1(Request):
    API_KEY = 15
    API_VERSION = 1
    RESPONSE_TYPE = DescribeGroupsResponse_v1
    SCHEMA = DescribeGroupsRequest_v0.SCHEMA


DescribeGroupsRequest = [DescribeGroupsRequest_v0, DescribeGroupsRequest_v1]
DescribeGroupsResponse = [DescribeGroupsResponse_v0, DescribeGroupsResponse_v1]


class SaslHandShakeResponse_v0(Response):
    API_KEY = 17
    API_VERSION = 0
    SCHEMA = Schema(
        ('error_code', Int16),
        ('enabled_mechanisms', Array(String('utf-8')))
    )


class SaslHandShakeResponse_v1(Response):
    API_KEY = 17
    API_VERSION = 1
    SCHEMA = SaslHandShakeResponse_v0.SCHEMA


class SaslHandShakeRequest_v0(Request):
    API_KEY = 17
    API_VERSION = 0
    RESPONSE_TYPE = SaslHandShakeResponse_v0
    SCHEMA = Schema(
        ('mechanism', String('utf-8'))
    )


class SaslHandShakeRequest_v1(Request):
    API_KEY = 17
    API_VERSION = 1
    RESPONSE_TYPE = SaslHandShakeResponse_v1
    SCHEMA = SaslHandShakeRequest_v0.SCHEMA


SaslHandShakeRequest = [SaslHandShakeRequest_v0, SaslHandShakeRequest_v1]
SaslHandShakeResponse = [SaslHandShakeResponse_v0, SaslHandShakeResponse_v1]


class SaslAuthenticateResponse_v0(Request):
    API_KEY = 36
    API_VERSION = 0
    SCHEMA = Schema(
        ('error_code', Int16),
        ('error_message', String('utf-8')),
        ('sasl_auth_bytes', Bytes)
    )


class SaslAuthenticateRequest_v0(Request):
    API_KEY = 36
    API_VERSION = 0
    RESPONSE_TYPE = SaslAuthenticateResponse_v0
    SCHEMA = Schema(
        ('sasl_auth_bytes', Bytes)
    )


SaslAuthenticateRequest = [SaslAuthenticateRequest_v0]
SaslAuthenticateResponse = [SaslAuthenticateResponse_v0]


class CreatePartitionsResponse_v0(Response):
    API_KEY = 37
    API_VERSION = 0
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('topic_errors', Array(
            ('topic', String('utf-8')),
            ('error_code', Int16),
            ('error_message', String('utf-8'))))
    )


class CreatePartitionsRequest_v0(Request):
    API_KEY = 37
    API_VERSION = 0
    RESPONSE_TYPE = CreatePartitionsResponse_v0
    SCHEMA = Schema(
        ('topic_partitions', Array(
            ('topic', String('utf-8')),
            ('new_partitions', Schema(
                ('count', Int32),
                ('assignment', Array(Int32)))))),
        ('timeout', Int32),
        ('validate_only', Boolean)
    )


CreatePartitionsRequest = [CreatePartitionsRequest_v0]
CreatePartitionsResponse = [CreatePartitionsResponse_v0]
