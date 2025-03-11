from __future__ import absolute_import

from kafka.protocol.api import Request, Response
from kafka.protocol.types import Array, Int16, Schema, String


class SaslHandshakeResponse_v0(Response):
    API_KEY = 17
    API_VERSION = 0
    SCHEMA = Schema(
        ('error_code', Int16),
        ('enabled_mechanisms', Array(String('utf-8')))
    )


class SaslHandshakeResponse_v1(Response):
    API_KEY = 17
    API_VERSION = 1
    SCHEMA = SaslHandshakeResponse_v0.SCHEMA


class SaslHandshakeRequest_v0(Request):
    API_KEY = 17
    API_VERSION = 0
    RESPONSE_TYPE = SaslHandshakeResponse_v0
    SCHEMA = Schema(
        ('mechanism', String('utf-8'))
    )


class SaslHandshakeRequest_v1(Request):
    API_KEY = 17
    API_VERSION = 1
    RESPONSE_TYPE = SaslHandshakeResponse_v1
    SCHEMA = SaslHandshakeRequest_v0.SCHEMA


SaslHandshakeRequest = [SaslHandshakeRequest_v0, SaslHandshakeRequest_v1]
SaslHandshakeResponse = [SaslHandshakeResponse_v0, SaslHandshakeResponse_v1]
