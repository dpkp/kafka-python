from enum import IntEnum

from ..api_message import ApiMessage
from kafka.util import EnumHelper


UNKNOWN_OFFSET = -1

class OffsetResetStrategy:
    LATEST = -1
    EARLIEST = -2
    NONE = 0


class IsolationLevel(EnumHelper, IntEnum):
    READ_UNCOMMITTED = 0
    READ_COMMITTED = 1


class OffsetSpec(EnumHelper, IntEnum):
    # Any >= 0:         # earliest offset whose timestamp is greater than or equal to the given timestamp and the timestamp of that record.
    LATEST = -1         # offset of the next message that will be appended to the log and a timestamp of -1
    EARLIEST = -2       # first offset on the partition, including remote-storage, and a timestamp of -1
    MAX_TIMESTAMP = -3  # offset and timestamp corresponding to the record with the highest timestamp on the partition. (KIP-734)
    EARLIEST_LOCAL = -4 # first offset on the local partition of the leader broker, excluding remote-storage, and a timestamp of -1 (KIP-405)
    LATEST_TIERED = -5  # the latest offset of the partition in remote storage (KIP-1005)


class ListOffsetsRequest(ApiMessage):
    @classmethod
    def min_version_for_timestamp(cls, ts):
        ts = OffsetSpec(ts)
        if ts == OffsetSpec.MAX_TIMESTAMP:
            return 7
        elif ts == OffsetSpec.EARLIEST_LOCAL:
            return 8
        elif ts == OffsetSpec.LATEST_TIERED:
            return 9
        else:
            return 0

    @classmethod
    def min_version_for_isolation_level(cls, il):
        if int(il) > 0:
            return 2
        else:
            return 0

class ListOffsetsResponse(ApiMessage): pass


class OffsetForLeaderEpochRequest(ApiMessage): pass
class OffsetForLeaderEpochResponse(ApiMessage): pass


__all__ = [
    'UNKNOWN_OFFSET', 'OffsetResetStrategy', 'IsolationLevel', 'OffsetSpec',
    'ListOffsetsRequest', 'ListOffsetsResponse',
    'OffsetForLeaderEpochRequest', 'OffsetForLeaderEpochResponse',
]
