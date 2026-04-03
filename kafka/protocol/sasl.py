import struct

from .api_message import ApiMessage


class SaslHandshakeRequest(ApiMessage): pass
class SaslHandshakeResponse(ApiMessage): pass

class SaslAuthenticateRequest(ApiMessage): pass
class SaslAuthenticateResponse(ApiMessage): pass


class SaslBytesRequest:
    """Request for raw SASL v0 exchange -- length-prefixed raw bytes."""
    API_VERSION = 0

    def __init__(self, data):
        self._data = data
        self.header = None

    def with_header(self, correlation_id=None, **kwargs):
        self.header = SaslBytesResponse(correlation_id)

    def encode(self, framed=True, header=True):
        return struct.pack('>I', len(self._data)) + self._data

    def expect_response(self):
        return True


class SaslBytesResponse:
    """Response for raw SASL v0 exchange -- returns bytes as-is."""
    def __init__(self, correlation_id):
        self.correlation_id = correlation_id
        self.error_code = 0

    def parse_header(self, read_buffer):
        return self

    def decode(self, read_buffer):
        self.auth_bytes = read_buffer.read()
        return self

    def get_response_class(self):
        return self


__all__ = [
    'SaslHandshakeRequest', 'SaslHandshakeResponse',
    'SaslAuthenticateRequest', 'SaslAuthenticateResponse',
    'SaslBytesRequest', 'SaslBytesResponse',
]
