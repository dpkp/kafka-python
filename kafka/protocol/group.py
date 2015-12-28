from .struct import Struct
from .types import Array, Bytes, Int16, Int32, Schema, String


class JoinGroupResponse(Struct):
    SCHEMA = Schema(
        ('error_code', Int16),
        ('generation_id', Int32),
        ('group_protocol', String('utf-8')),
        ('leader_id', String('utf-8')),
        ('member_id', String('utf-8')),
        ('members', Array(
            ('member_id', String('utf-8')),
            ('member_metadata', Bytes)))
    )


class JoinGroupRequest(Struct):
    API_KEY = 11
    API_VERSION = 0
    RESPONSE_TYPE = JoinGroupResponse
    SCHEMA = Schema(
        ('group', String('utf-8')),
        ('session_timeout', Int32),
        ('member_id', String('utf-8')),
        ('protocol_type', String('utf-8')),
        ('group_protocols', Array(
            ('protocol_name', String('utf-8')),
            ('protocol_metadata', Bytes)))
    )
    UNKNOWN_MEMBER_ID = ''


class ProtocolMetadata(Struct):
    SCHEMA = Schema(
        ('version', Int16),
        ('subscription', Array(String('utf-8'))), # topics list
        ('user_data', Bytes)
    )


class SyncGroupResponse(Struct):
    SCHEMA = Schema(
        ('error_code', Int16),
        ('member_assignment', Bytes)
    )


class SyncGroupRequest(Struct):
    API_KEY = 14
    API_VERSION = 0
    RESPONSE_TYPE = SyncGroupResponse
    SCHEMA = Schema(
        ('group', String('utf-8')),
        ('generation_id', Int32),
        ('member_id', String('utf-8')),
        ('group_assignment', Array(
            ('member_id', String('utf-8')),
            ('member_metadata', Bytes)))
    )


class MemberAssignment(Struct):
    SCHEMA = Schema(
        ('version', Int16),
        ('partition_assignment', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(Int32)))),
        ('user_data', Bytes)
    )


class HeartbeatResponse(Struct):
    SCHEMA = Schema(
        ('error_code', Int16)
    )


class HeartbeatRequest(Struct):
    API_KEY = 12
    API_VERSION = 0
    RESPONSE_TYPE = HeartbeatResponse
    SCHEMA = Schema(
        ('group', String('utf-8')),
        ('generation_id', Int32),
        ('member_id', String('utf-8'))
    )


class LeaveGroupResponse(Struct):
    SCHEMA = Schema(
        ('error_code', Int16)
    )


class LeaveGroupRequest(Struct):
    API_KEY = 13
    API_VERSION = 0
    RESPONSE_TYPE = LeaveGroupResponse
    SCHEMA = Schema(
        ('group', String('utf-8')),
        ('member_id', String('utf-8'))
    )
