import io

from ...api_message import ApiMessage
from ....types import Int16, Int32


class ApiVersionsRequest(ApiMessage): pass
class ApiVersionsResponse(ApiMessage):
    # ApiVersionsResponse header never uses flexible formats, even if body does
    @classmethod
    def parse_header(cls, data, flexible=False):
        return super().parse_header(data, flexible=False)

    def encode_header(self, flexible=False):
        return super().encode_header(flexible=False)

    # ApiVersionsResponse body always decodes as version 0 when error is present
    @classmethod
    def decode(cls, data, version=None, header=False, framed=False):
        if isinstance(data, bytes):
            data = io.BytesIO(data)
        if framed:
            size = Int32.decode(data)
        if header:
            hdr = cls.parse_header(data)
        else:
            hdr = None
        curr = data.tell()
        err = Int16.decode(data)
        data.seek(curr)
        if err != 0:
            version = 0
        ret = super().decode(data, version=version, header=False, framed=False)
        if hdr is not None:
            ret._header = hdr
        return ret


__all__ = [
    'ApiVersionsRequest', 'ApiVersionsResponse',
]
