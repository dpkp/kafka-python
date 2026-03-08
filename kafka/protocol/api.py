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


class RequestResponse(Struct, metaclass=abc.ABCMeta):
    FLEXIBLE_VERSION = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._header = None

    @abc.abstractproperty
    def API_KEY(self):
        """Integer identifier for api request"""
        pass

    @abc.abstractproperty
    def API_VERSION(self):
        """Integer of api request version"""
        pass

    def to_object(self):
        return _to_object(self.SCHEMA, self)

    @property
    def header(self):
        return self._header

    def encode(self, header=False, framed=False):
        data = super().encode()
        if not framed and not header:
            return data
        bits = [data]
        if header:
            bits.insert(0, self.header.encode())
        if framed:
            bits.insert(0, Int32.encode(sum(map(len, bits))))
        return b''.join(bits)

    @classmethod
    @abc.abstractmethod
    def header_class(cls):
        pass

    @classmethod
    def parse_header(cls, read_buffer):
        return cls.header_class().decode(read_buffer)

    @classmethod
    def decode(cls, data, header=False, framed=False):
        if not framed and not header:
            return super().decode(data)
        if isinstance(data, bytes):
            data = BytesIO(data)
        ret = []
        if framed:
            ret.append(Int32.decode(data))
        if header:
            ret.append(cls.parse_header(data))
        ret.append(super().decode(data))
        return tuple(ret)


class Request(RequestResponse):
    @abc.abstractproperty
    def RESPONSE_TYPE(self):
        """The Response class associated with the api request"""
        pass

    def expect_response(self):
        """Override this method if an api request does not always generate a response"""
        return True

    def with_header(self, correlation_id=0, client_id='kafka-python'):
        if self.FLEXIBLE_VERSION:
            self._header = self.header_class()(self.API_KEY, self.API_VERSION, correlation_id, client_id, {})
        else:
            self._header = self.header_class()(self.API_KEY, self.API_VERSION, correlation_id, client_id)

    @classmethod
    def header_class(cls):
        if cls.FLEXIBLE_VERSION:
            return RequestHeaderV2
        else:
            return RequestHeader

    def encode(self, header=False, framed=False, correlation_id=None, client_id=None, **kwargs):
        if header and self.header is None:
            self.with_header(correlation_id=correlation_id, client_id=client_id)
        return super().encode(header=header, framed=framed)


class Response(RequestResponse):
    def with_header(self, correlation_id=0):
        if self.FLEXIBLE_VERSION:
            self._header = self.header_class()(correlation_id, {})
        else:
            self._header = self.header_class()(correlation_id)

    @classmethod
    def header_class(cls):
        if cls.FLEXIBLE_VERSION:
            return ResponseHeaderV2
        else:
            return ResponseHeader

    def encode(self, header=False, framed=False, correlation_id=None, **kwargs):
        if header and self.header is None:
            self.with_header(correlation_id=correlation_id)
        return super().encode(header=header, framed=framed)


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
