from ..api_message import ApiMessage


class FetchRequest(ApiMessage): pass
class FetchResponse(ApiMessage): pass

class ListOffsetsRequest(ApiMessage): pass
class ListOffsetsResponse(ApiMessage): pass

class OffsetForLeaderEpochRequest(ApiMessage): pass
class OffsetForLeaderEpochResponse(ApiMessage): pass


__all__ = [
    'FetchRequest', 'FetchResponse',
    'ListOffsetsRequest', 'ListOffsetsResponse',
    'OffsetForLeaderEpochRequest', 'OffsetForLeaderEpochResponse',
]
