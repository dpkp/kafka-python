import collections

from kafka.protocol.api import Request, Response
from kafka.protocol.struct import Struct
from kafka.protocol.types import Array, Bytes, Int16, Int32, Schema, String


DEFAULT_GENERATION_ID = -1
UNKNOWN_MEMBER_ID = ''

GroupMember = collections.namedtuple("GroupMember", ["member_id", "group_instance_id", "metadata_bytes"])
GroupMember.__new__.__defaults__ = (None,) * len(GroupMember._fields)


class JoinGroupResponse_v0(Response):
    API_KEY = 11
    API_VERSION = 0
    SCHEMA = Schema(
        ('error_code', Int16),
        ('generation_id', Int32),
        ('protocol_name', String('utf-8')),
        ('leader', String('utf-8')),
        ('member_id', String('utf-8')),
        ('members', Array(
            ('member_id', String('utf-8')),
            ('metadata', Bytes)))
    )
    ALIASES = {
        'group_protocol': 'protocol_name',
        'leader_id': 'leader',
    }


class JoinGroupResponse_v1(Response):
    API_KEY = 11
    API_VERSION = 1
    SCHEMA = JoinGroupResponse_v0.SCHEMA
    ALIASES = JoinGroupResponse_v0.ALIASES


class JoinGroupResponse_v2(Response):
    API_KEY = 11
    API_VERSION = 2
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('error_code', Int16),
        ('generation_id', Int32),
        ('protocol_name', String('utf-8')),
        ('leader', String('utf-8')),
        ('member_id', String('utf-8')),
        ('members', Array(
            ('member_id', String('utf-8')),
            ('metadata', Bytes)))
    )
    ALIASES = JoinGroupResponse_v1.ALIASES


class JoinGroupResponse_v3(Response):
    API_KEY = 11
    API_VERSION = 3
    SCHEMA = JoinGroupResponse_v2.SCHEMA
    ALIASES = JoinGroupResponse_v2.ALIASES


class JoinGroupResponse_v4(Response):
    API_KEY = 11
    API_VERSION = 4
    SCHEMA = JoinGroupResponse_v3.SCHEMA
    ALIASES = JoinGroupResponse_v3.ALIASES


class JoinGroupResponse_v5(Response):
    API_KEY = 11
    API_VERSION = 5
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('error_code', Int16),
        ('generation_id', Int32),
        ('protocol_name', String('utf-8')),
        ('leader', String('utf-8')),
        ('member_id', String('utf-8')),
        ('members', Array(
            ('member_id', String('utf-8')),
            ('group_instance_id', String('utf-8')),
            ('metadata', Bytes)))
    )
    ALIASES = JoinGroupResponse_v4.ALIASES


class JoinGroupRequest_v0(Request):
    API_KEY = 11
    API_VERSION = 0
    SCHEMA = Schema(
        ('group_id', String('utf-8')),
        ('session_timeout_ms', Int32),
        ('member_id', String('utf-8')),
        ('protocol_type', String('utf-8')),
        ('protocols', Array(
            ('name', String('utf-8')),
            ('metadata', Bytes)))
    )
    ALIASES = {
        'group': 'group_id',
        'session_timeout': 'session_timeout_ms',
        'group_protocols': 'protocols',
    }


class JoinGroupRequest_v1(Request):
    API_KEY = 11
    API_VERSION = 1
    SCHEMA = Schema(
        ('group_id', String('utf-8')),
        ('session_timeout_ms', Int32),
        ('rebalance_timeout_ms', Int32),
        ('member_id', String('utf-8')),
        ('protocol_type', String('utf-8')),
        ('protocols', Array(
            ('name', String('utf-8')),
            ('metadata', Bytes)))
    )
    ALIASES = {
        'group': 'group_id',
        'session_timeout': 'session_timeout_ms',
        'rebalance_timeout': 'rebalance_timeout_ms',
        'group_protocols': 'protocols',
    }


class JoinGroupRequest_v2(Request):
    API_KEY = 11
    API_VERSION = 2
    SCHEMA = JoinGroupRequest_v1.SCHEMA
    ALIASES = JoinGroupRequest_v1.ALIASES


class JoinGroupRequest_v3(Request):
    API_KEY = 11
    API_VERSION = 3
    SCHEMA = JoinGroupRequest_v2.SCHEMA
    ALIASES = JoinGroupRequest_v2.ALIASES


class JoinGroupRequest_v4(Request):
    API_KEY = 11
    API_VERSION = 4
    SCHEMA = JoinGroupRequest_v3.SCHEMA
    ALIASES = JoinGroupRequest_v3.ALIASES


class JoinGroupRequest_v5(Request):
    API_KEY = 11
    API_VERSION = 5
    SCHEMA = Schema(
        ('group_id', String('utf-8')),
        ('session_timeout_ms', Int32),
        ('rebalance_timeout_ms', Int32),
        ('member_id', String('utf-8')),
        ('group_instance_id', String('utf-8')),
        ('protocol_type', String('utf-8')),
        ('protocols', Array(
            ('name', String('utf-8')),
            ('metadata', Bytes)))
    )
    ALIASES = JoinGroupRequest_v4.ALIASES


JoinGroupRequest = [
    JoinGroupRequest_v0, JoinGroupRequest_v1, JoinGroupRequest_v2,
    JoinGroupRequest_v3, JoinGroupRequest_v4, JoinGroupRequest_v5,

]
JoinGroupResponse = [
    JoinGroupResponse_v0, JoinGroupResponse_v1, JoinGroupResponse_v2,
    JoinGroupResponse_v3, JoinGroupResponse_v4, JoinGroupResponse_v5,
]


# Currently unused -- see kafka.coordinator.protocol
class ProtocolMetadata(Struct):
    SCHEMA = Schema(
        ('version', Int16),
        ('subscription', Array(String('utf-8'))), # topics list
        ('user_data', Bytes)
    )


class SyncGroupResponse_v0(Response):
    API_KEY = 14
    API_VERSION = 0
    SCHEMA = Schema(
        ('error_code', Int16),
        ('assignment', Bytes)
    )
    ALIASES = {
        'member_assignment': 'assignment',
    }


class SyncGroupResponse_v1(Response):
    API_KEY = 14
    API_VERSION = 1
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('error_code', Int16),
        ('assignment', Bytes)
    )
    ALIASES = SyncGroupResponse_v0.ALIASES


class SyncGroupResponse_v2(Response):
    API_KEY = 14
    API_VERSION = 2
    SCHEMA = SyncGroupResponse_v1.SCHEMA
    ALIASES = SyncGroupResponse_v1.ALIASES


class SyncGroupResponse_v3(Response):
    API_KEY = 14
    API_VERSION = 3
    SCHEMA = SyncGroupResponse_v2.SCHEMA
    ALIASES = SyncGroupResponse_v2.ALIASES


class SyncGroupRequest_v0(Request):
    API_KEY = 14
    API_VERSION = 0
    SCHEMA = Schema(
        ('group_id', String('utf-8')),
        ('generation_id', Int32),
        ('member_id', String('utf-8')),
        ('assignments', Array(
            ('member_id', String('utf-8')),
            ('assignment', Bytes)))
    )
    ALIASES = {
        'group': 'group_id',
        'group_assignment': 'assignments',
    }


class SyncGroupRequest_v1(Request):
    API_KEY = 14
    API_VERSION = 1
    SCHEMA = SyncGroupRequest_v0.SCHEMA
    ALIASES = SyncGroupRequest_v0.ALIASES


class SyncGroupRequest_v2(Request):
    API_KEY = 14
    API_VERSION = 2
    SCHEMA = SyncGroupRequest_v1.SCHEMA
    ALIASES = SyncGroupRequest_v1.ALIASES


class SyncGroupRequest_v3(Request):
    API_KEY = 14
    API_VERSION = 3
    SCHEMA = Schema(
        ('group_id', String('utf-8')),
        ('generation_id', Int32),
        ('member_id', String('utf-8')),
        ('group_instance_id', String('utf-8')),
        ('assignments', Array(
            ('member_id', String('utf-8')),
            ('assignment', Bytes)))
    )
    ALIASES = SyncGroupRequest_v2.ALIASES


SyncGroupRequest = [
    SyncGroupRequest_v0, SyncGroupRequest_v1, SyncGroupRequest_v2,
    SyncGroupRequest_v3,
]
SyncGroupResponse = [
    SyncGroupResponse_v0, SyncGroupResponse_v1, SyncGroupResponse_v2,
    SyncGroupResponse_v3,
]


# Currently unused -- see kafka.coordinator.protocol
class MemberAssignment(Struct):
    SCHEMA = Schema(
        ('version', Int16),
        ('assignment', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(Int32)))),
        ('user_data', Bytes)
    )


class HeartbeatResponse_v0(Response):
    API_KEY = 12
    API_VERSION = 0
    SCHEMA = Schema(
        ('error_code', Int16)
    )


class HeartbeatResponse_v1(Response):
    API_KEY = 12
    API_VERSION = 1
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('error_code', Int16)
    )


class HeartbeatResponse_v2(Response):
    API_KEY = 12
    API_VERSION = 2
    SCHEMA = HeartbeatResponse_v1.SCHEMA


class HeartbeatResponse_v3(Response):
    API_KEY = 12
    API_VERSION = 3
    SCHEMA = HeartbeatResponse_v2.SCHEMA


class HeartbeatRequest_v0(Request):
    API_KEY = 12
    API_VERSION = 0
    SCHEMA = Schema(
        ('group_id', String('utf-8')),
        ('generation_id', Int32),
        ('member_id', String('utf-8'))
    )
    ALIASES = {
        'group': 'group_id',
    }


class HeartbeatRequest_v1(Request):
    API_KEY = 12
    API_VERSION = 1
    SCHEMA = HeartbeatRequest_v0.SCHEMA
    ALIASES = HeartbeatRequest_v0.ALIASES


class HeartbeatRequest_v2(Request):
    API_KEY = 12
    API_VERSION = 2
    SCHEMA = HeartbeatRequest_v1.SCHEMA
    ALIASES = HeartbeatRequest_v1.ALIASES


class HeartbeatRequest_v3(Request):
    API_KEY = 12
    API_VERSION = 3
    SCHEMA = Schema(
        ('group_id', String('utf-8')),
        ('generation_id', Int32),
        ('member_id', String('utf-8')),
        ('group_instance_id', String('utf-8'))
    )
    ALIASES = HeartbeatRequest_v2.ALIASES


HeartbeatRequest = [
    HeartbeatRequest_v0, HeartbeatRequest_v1, HeartbeatRequest_v2,
    HeartbeatRequest_v3,
]
HeartbeatResponse = [
    HeartbeatResponse_v0, HeartbeatResponse_v1, HeartbeatResponse_v2,
    HeartbeatResponse_v3,
]


class LeaveGroupResponse_v0(Response):
    API_KEY = 13
    API_VERSION = 0
    SCHEMA = Schema(
        ('error_code', Int16)
    )


class LeaveGroupResponse_v1(Response):
    API_KEY = 13
    API_VERSION = 1
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('error_code', Int16)
    )


class LeaveGroupResponse_v2(Response):
    API_KEY = 13
    API_VERSION = 2
    SCHEMA = LeaveGroupResponse_v1.SCHEMA


class LeaveGroupResponse_v3(Response):
    API_KEY = 13
    API_VERSION = 3
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('error_code', Int16),
        ('members', Array(
            ('member_id', String('utf-8')),
            ('group_instance_id', String('utf-8')),
            ('error_code', Int16)))
    )


class LeaveGroupRequest_v0(Request):
    API_KEY = 13
    API_VERSION = 0
    SCHEMA = Schema(
        ('group_id', String('utf-8')),
        ('member_id', String('utf-8'))
    )
    ALIASES = {
        'group': 'group_id',
    }


class LeaveGroupRequest_v1(Request):
    API_KEY = 13
    API_VERSION = 1
    SCHEMA = LeaveGroupRequest_v0.SCHEMA
    ALIASES = LeaveGroupRequest_v0.ALIASES


class LeaveGroupRequest_v2(Request):
    API_KEY = 13
    API_VERSION = 2
    SCHEMA = LeaveGroupRequest_v1.SCHEMA
    ALIASES = LeaveGroupRequest_v1.ALIASES


class LeaveGroupRequest_v3(Request):
    API_KEY = 13
    API_VERSION = 3
    SCHEMA = Schema(
        ('group_id', String('utf-8')),
        ('members', Array(
            ('member_id', String('utf-8')),
            ('group_instance_id', String('utf-8'))))
    )
    ALIASES = LeaveGroupRequest_v2.ALIASES


LeaveGroupRequest = [
    LeaveGroupRequest_v0, LeaveGroupRequest_v1, LeaveGroupRequest_v2,
    LeaveGroupRequest_v3,
]
LeaveGroupResponse = [
    LeaveGroupResponse_v0, LeaveGroupResponse_v1, LeaveGroupResponse_v2,
    LeaveGroupResponse_v3,
]
