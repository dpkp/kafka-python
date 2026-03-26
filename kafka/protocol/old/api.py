import abc
from io import BytesIO
import weakref

from .struct import Struct
from .types import Int16, Int32, String, Schema, Array, TaggedFields


class ResponseClassRegistry:
    _response_class_registry = {}

    @classmethod
    def register_response_class(cls, response_class):
        cls._response_class_registry[(response_class.API_KEY, response_class.API_VERSION)] = response_class

    @classmethod
    def get_response_class(cls, header):
        key = (header.api_key, header.api_version)
        if key in cls._response_class_registry:
            return cls._response_class_registry[key]


class RequestHeader(ResponseClassRegistry, Struct):
    SCHEMA = Schema(
        ('api_key', Int16),
        ('api_version', Int16),
        ('correlation_id', Int32),
        ('client_id', String('utf-8'))
    )

    def get_response_class(self):
        return ResponseClassRegistry.get_response_class(self)


class RequestHeaderV2(ResponseClassRegistry, Struct):
    # Flexible response / request headers end in field buffer
    SCHEMA = Schema(
        ('api_key', Int16),
        ('api_version', Int16),
        ('correlation_id', Int32),
        ('client_id', String('utf-8')),
        ('tags', TaggedFields),
    )

    def get_response_class(self):
        key = (self.api_key, self.api_version) # pylint: disable=E1101
        if key in ResponseClassRegistry._response_class_registry:
            return ResponseClassRegistry._response_class_registry[key]


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

    @classmethod
    @abc.abstractmethod
    def is_request(cls):
        pass

    @property
    def header(self):
        return self._header

    def with_header(self, correlation_id=0, client_id='kafka-python'):
        if self.is_request():
            kwargs = {
                'api_key': self.API_KEY,
                'api_version': self.API_VERSION,
                'correlation_id': correlation_id,
                'client_id': client_id,
            }
        else:
            kwargs = {
                'correlation_id': correlation_id,
            }
        self._header = self.header_class()(**kwargs)

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
        if framed:
            size = Int32.decode(data)
        if header:
            hdr = cls.parse_header(data)
        else:
            hdr = None
        ret = super().decode(data)
        if hdr is not None:
            ret._header = hdr
        return ret

    def __eq__(self, other):
        return self._header == other._header and super().__eq__(other)


class Request(RequestResponse):
    FLEXIBLE_VERSION = False

    @classmethod
    def is_request(cls):
        return True

    def expect_response(self):
        """Override this method if an api request does not always generate a response"""
        return True

    @classmethod
    def header_class(cls):
        if cls.FLEXIBLE_VERSION:
            return RequestHeaderV2
        else:
            return RequestHeader


class Response(RequestResponse):
    FLEXIBLE_VERSION = False

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        ResponseClassRegistry.register_response_class(weakref.proxy(cls))

    @classmethod
    def is_request(cls):
        return False

    @classmethod
    def header_class(cls):
        if cls.FLEXIBLE_VERSION:
            return ResponseHeaderV2
        else:
            return ResponseHeader


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
