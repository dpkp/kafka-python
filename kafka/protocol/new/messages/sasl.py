from ..api_message import ApiMessage


class SaslHandshakeRequest(ApiMessage): pass
class SaslHandshakeResponse(ApiMessage): pass

class SaslAuthenticateRequest(ApiMessage): pass
class SaslAuthenticateResponse(ApiMessage): pass


__all__ = [
    'SaslHandshakeRequest', 'SaslHandshakeResponse',
    'SaslAuthenticateRequest', 'SaslAuthenticateResponse',
]
