from enum import IntEnum

from ..api_message import ApiMessage
from kafka.util import EnumHelper


class CoordinatorType(EnumHelper, IntEnum):
    GROUP = 0
    TRANSACTION = 1
    SHARE = 2


class FindCoordinatorRequest(ApiMessage): pass
class FindCoordinatorResponse(ApiMessage): pass


__all__ = [
    'CoordinatorType', 'FindCoordinatorRequest', 'FindCoordinatorResponse',
]
