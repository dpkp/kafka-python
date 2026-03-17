from ..api_message import ApiMessage


UNKNOWN_OFFSET = -1

class OffsetResetStrategy:
    LATEST = -1
    EARLIEST = -2
    NONE = 0

class ListOffsetsRequest(ApiMessage): pass
class ListOffsetsResponse(ApiMessage): pass

class OffsetForLeaderEpochRequest(ApiMessage): pass
class OffsetForLeaderEpochResponse(ApiMessage): pass


__all__ = [
    'UNKNOWN_OFFSET', 'OffsetResetStrategy',
    'ListOffsetsRequest', 'ListOffsetsResponse',
    'OffsetForLeaderEpochRequest', 'OffsetForLeaderEpochResponse',
]
