from ..api_message import ApiMessage


class FetchRequest(ApiMessage): pass
class FetchResponse(ApiMessage): pass

__all__ = [
    'FetchRequest', 'FetchResponse',
]
