from .struct import Struct
from .types import Int16, Int32, String, Schema


class RequestHeader(Struct):
    SCHEMA = Schema(
        ('api_key', Int16),
        ('api_version', Int16),
        ('correlation_id', Int32),
        ('client_id', String('utf-8'))
    )

    def __init__(self, request, correlation_id=0, client_id='kafka-python'):
        super(RequestHeader, self).__init__(
            request.API_KEY, request.API_VERSION, correlation_id, client_id
        )
