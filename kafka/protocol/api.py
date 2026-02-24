import abc
from io import BytesIO

from kafka.protocol.struct import Struct
from kafka.protocol.types import Int16, Int32, String, Schema, Array, TaggedFields


class RequestHeader(Struct):
    SCHEMA = Schema(
        ('api_key', Int16),
        ('api_version', Int16),
        ('correlation_id', Int32),
        ('client_id', String('utf-8'))
    )


class RequestHeaderV2(Struct):
    # Flexible response / request headers end in field buffer
    SCHEMA = Schema(
        ('api_key', Int16),
        ('api_version', Int16),
        ('correlation_id', Int32),
        ('client_id', String('utf-8')),
        ('tags', TaggedFields),
    )


class ResponseHeader(Struct):
    SCHEMA = Schema(
        ('correlation_id', Int32),
    )


class ResponseHeaderV2(Struct):
    SCHEMA = Schema(
        ('correlation_id', Int32),
        ('tags', TaggedFields),
    )


class Request(Struct, metaclass=abc.ABCMeta):
    FLEXIBLE_VERSION = False

    @abc.abstractproperty
    def API_KEY(self):
        """Integer identifier for api request"""
        pass

    @abc.abstractproperty
    def API_VERSION(self):
        """Integer of api request version"""
        pass

    @abc.abstractproperty
    def RESPONSE_TYPE(self):
        """The Response class associated with the api request"""
        pass

    def expect_response(self):
        """Override this method if an api request does not always generate a response"""
        return True

    def to_object(self):
        return _to_object(self.SCHEMA, self)

    def build_header(self, correlation_id=0, client_id='kafka-python'):
        if self.FLEXIBLE_VERSION:
            return RequestHeaderV2(self.API_KEY, self.API_VERSION, correlation_id, client_id, {})
        return RequestHeader(self.API_KEY, self.API_VERSION, correlation_id, client_id)

    @classmethod
    def parse_header(cls, read_buffer):
        if cls.FLEXIBLE_VERSION:
            return RequestHeaderV2.decode(read_buffer)
        return RequestHeader.decode(read_buffer)

    def encode(self, header=False, framed=False, correlation_id=None, client_id=None, **kwargs):
        data = super(Request, self).encode()
        if not framed and not header:
            return data
        bits = [data]
        if header:
            bits.insert(0, self.build_header(correlation_id, client_id).encode())
        if framed:
            bits.insert(0, Int32.encode(sum(map(len, bits))))
        return b''.join(bits)

    @classmethod
    def decode(cls, data, header=False, framed=False):
        if not framed and not header:
            return super(Request, cls).decode(data)
        if isinstance(data, bytes):
            data = BytesIO(data)
        ret = []
        if framed:
            ret.append(Int32.decode(data))
        if header:
            ret.append(cls.parse_header(data))
        ret.append(super(Request, cls).decode(data))
        return tuple(ret)


class Response(Struct, metaclass=abc.ABCMeta):
    FLEXIBLE_VERSION = False

    @abc.abstractproperty
    def API_KEY(self):
        """Integer identifier for api request/response"""
        pass

    @abc.abstractproperty
    def API_VERSION(self):
        """Integer of api request/response version"""
        pass

    def to_object(self):
        return _to_object(self.SCHEMA, self)

    def build_header(self, correlation_id=0):
        if self.FLEXIBLE_VERSION:
            return ResponseHeaderV2(correlation_id=correlation_id, tags=None)
        return ResponseHeader(correlation_id=correlation_id)

    @classmethod
    def parse_header(cls, read_buffer):
        if cls.FLEXIBLE_VERSION:
            return ResponseHeaderV2.decode(read_buffer)
        return ResponseHeader.decode(read_buffer)

    def encode(self, header=False, framed=False, correlation_id=None, **kwargs):
        data = super(Response, self).encode()
        if not framed and not header:
            return data
        bits = [data]
        if header:
            bits.insert(0, self.build_header(correlation_id).encode())
        if framed:
            bits.insert(0, Int32.encode(sum(map(len, bits))))
        return b''.join(bits)

    @classmethod
    def decode(cls, data, header=False, framed=False):
        if not framed and not header:
            return super(Response, cls).decode(data)
        if isinstance(data, bytes):
            data = BytesIO(data)
        ret = []
        if framed:
            ret.append(Int32.decode(data))
        if header:
            ret.append(cls.parse_header(data))
        ret.append(super(Response, cls).decode(data))
        return tuple(ret)


def _to_object(schema, data):
    obj = {}
    for idx, (name, _type) in enumerate(zip(schema.names, schema.fields)):
        if isinstance(data, Struct):
            val = data.get_item(name)
        else:
            val = data[idx]

        if isinstance(_type, Schema):
            obj[name] = _to_object(_type, val)
        elif isinstance(_type, Array):
            if isinstance(_type.array_of, (Array, Schema)):
                obj[name] = [
                    _to_object(_type.array_of, x)
                    for x in val
                ]
            else:
                obj[name] = val
        else:
            obj[name] = val

    return obj
