from ..api_message import ApiMessage


DEFAULT_GENERATION_ID = -1
UNKNOWN_MEMBER_ID = ''

class JoinGroupRequest(ApiMessage): pass
class JoinGroupResponse(ApiMessage): pass

class SyncGroupRequest(ApiMessage): pass
class SyncGroupResponse(ApiMessage): pass

class LeaveGroupRequest(ApiMessage): pass
class LeaveGroupResponse(ApiMessage): pass

class HeartbeatRequest(ApiMessage): pass
class HeartbeatResponse(ApiMessage): pass

class OffsetFetchRequest(ApiMessage): pass
class OffsetFetchResponse(ApiMessage): pass

class OffsetCommitRequest(ApiMessage): pass
class OffsetCommitResponse(ApiMessage): pass

class OffsetDeleteRequest(ApiMessage): pass
class OffsetDeleteResponse(ApiMessage): pass


__all__ = [
    'DEFAULT_GENERATION_ID', 'UNKNOWN_MEMBER_ID',
    'JoinGroupRequest', 'JoinGroupResponse',
    'SyncGroupRequest', 'SyncGroupResponse',
    'LeaveGroupRequest', 'LeaveGroupResponse',
    'HeartbeatRequest', 'HeartbeatResponse',
    'OffsetFetchRequest', 'OffsetFetchResponse',
    'OffsetCommitRequest', 'OffsetCommitResponse',
    'OffsetDeleteRequest', 'OffsetDeleteResponse',
]
