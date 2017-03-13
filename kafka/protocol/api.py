from __future__ import absolute_import

import abc

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


class Request(Struct):
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def API_KEY(self):
        """Integer identifier for api request"""
        pass

    @abc.abstractproperty
    def API_VERSION(self):
        """Integer of api request version"""
        pass

    @abc.abstractproperty
    def SCHEMA(self):
        """An instance of Schema() representing the request structure"""
        pass

    @abc.abstractproperty
    def RESPONSE_TYPE(self):
        """The Response class associated with the api request"""
        pass

    def expect_response(self):
        """Override this method if an api request does not always generate a response"""
        return True


class Response(Struct):
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def API_KEY(self):
        """Integer identifier for api request/response"""
        pass

    @abc.abstractproperty
    def API_VERSION(self):
        """Integer of api request/response version"""
        pass

    @abc.abstractproperty
    def SCHEMA(self):
        """An instance of Schema() representing the response structure"""
        pass
