from __future__ import absolute_import

from kafka.protocol.api import Request, Response
from kafka.protocol.types import Bytes, Int16, Int64, Schema, String


class SaslAuthenticateResponse_v0(Response):
    API_KEY = 36
    API_VERSION = 0
    SCHEMA = Schema(
        ('error_code', Int16),
        ('error_message', String('utf-8')),
        ('auth_bytes', Bytes))


class SaslAuthenticateResponse_v1(Response):
    API_KEY = 36
    API_VERSION = 1
    SCHEMA = Schema(
        ('error_code', Int16),
        ('error_message', String('utf-8')),
        ('auth_bytes', Bytes),
        ('session_lifetime_ms', Int64))


class SaslAuthenticateRequest_v0(Request):
    API_KEY = 36
    API_VERSION = 0
    RESPONSE_TYPE = SaslAuthenticateResponse_v0
    SCHEMA = Schema(
        ('auth_bytes', Bytes))


class SaslAuthenticateRequest_v1(Request):
    API_KEY = 36
    API_VERSION = 1
    RESPONSE_TYPE = SaslAuthenticateResponse_v1
    SCHEMA = SaslAuthenticateRequest_v0.SCHEMA


SaslAuthenticateRequest = [SaslAuthenticateRequest_v0, SaslAuthenticateRequest_v1]
SaslAuthenticateResponse = [SaslAuthenticateResponse_v0, SaslAuthenticateResponse_v1]
