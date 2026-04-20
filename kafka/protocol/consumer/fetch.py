from ..api_message import ApiMessage


class FetchRequest(ApiMessage):
    @classmethod
    def min_version_for_isolation_level(cls, il):
        if int(il) > 0:
            return 4
        else:
            return 0

class FetchResponse(ApiMessage): pass

__all__ = [
    'FetchRequest', 'FetchResponse',
]
