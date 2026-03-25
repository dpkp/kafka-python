import io
import weakref

from .api_data import JsonSchemaData
from .api_header import RequestHeader, ResponseHeader, ResponseClassRegistry
from .data_container import DataContainer
from .schemas import BaseField, StructField, load_json
from .schemas.fields.codecs import Int32, EncodeBuffer
from struct import pack_into

from kafka.util import classproperty


class VersionSubscriptable(type):
    def __init__(cls, name, bases, attrs, **kw):
        super().__init__(name, bases, attrs, **kw)
        if kw.get('init', True):
            # The primary message class has _version = None
            # and a _VERSIONS dict that provides access to version-specific wrappers
            # We also include cls[None] -> primary class to "exit" a version class
            if getattr(cls, '_class_version', None) is None:
                cls._class_version = None
                cls._VERSIONS = {}

    def __getitem__(cls, version):
        # Use [] lookups to move from primary class to "versioned" classes
        # which are simple wrappers around the primary class but with a _version attr
        if cls._class_version is not None:
            primary_cls = cls.mro()[1]
            if version is None:
                return primary_cls
            return primary_cls[version]
        elif version is None:
            return cls
        if cls._valid_versions is not None:
            if version < 0:
                version += 1 + cls.max_version # support negative index, e.g., [-1]
            if not cls.min_version <= version <= cls.max_version:
                raise ValueError('Invalid version! min=%d, max=%d' % (cls.min_version, cls.max_version))
        if version in cls._VERSIONS:
            return cls._VERSIONS[version]
        klass_name = cls.__name__ + '_v' + str(version)
        cls._VERSIONS[version] = type(klass_name, tuple(cls.mro()), {'_class_version': version}, init=False)
        return cls._VERSIONS[version]

    def __len__(cls):
        # Maintain compatibility
        if cls._valid_versions is None:
            raise RuntimeError('Unable to calculate __len__ for class without valid_versions')
        elif cls._class_version is not None:
            raise TypeError('len() only supported on primary message class (not versioned)')
        return cls._valid_versions[1] + 1


class ApiMessageData(VersionSubscriptable, JsonSchemaData):
    def __init__(cls, name, bases, attrs, **kw):
        super().__init__(name, bases, attrs, **kw)
        if kw.get('init', True):
            # Ignore min valid version on request/response schemas
            # We'll get the brokers supported versions via ApiVersionsRequest
            if cls._struct._versions[0] > 0:
                cls._struct._versions = (0, cls._struct._versions[1])


class ApiMessage(DataContainer, metaclass=ApiMessageData, init=False):
    __slots__ = ('_header')

    def __init_subclass__(cls, **kw):
        super().__init_subclass__(**kw)
        if kw.get('init', True):
            # pylint: disable=E1101
            assert cls._json is not None
            assert cls._json['type'] in ('request', 'response')
            cls._flexible_versions = BaseField.parse_versions(cls._json['flexibleVersions'])
            cls._valid_versions = BaseField.parse_versions(cls._json['validVersions'])
            if not cls.is_request():
                ResponseClassRegistry.register_response_class(weakref.proxy(cls))

    def __new__(cls, *args, **kwargs):
        # Translate "versioned" classes back to primary w/ version= kwarg on construction
        if cls._class_version is not None:
            if kwargs.get('version', cls._class_version) != cls._class_version: # pylint: disable=E1101
                raise ValueError("Version has already been set by class")
            kwargs['version'] = cls._class_version
            instance = super().__new__(cls[None])
            instance.__init__(*args, **kwargs)
            return instance
        return super().__new__(cls)

    def __init__(self, *args, **kwargs):
        self._header = None
        super().__init__(*args, **kwargs)

    @classproperty
    def name(cls): # pylint: disable=E0213
        return cls._json['name'] # pylint: disable=E1101

    @classproperty
    def type(cls): # pylint: disable=E0213
        return cls._json['type'] # pylint: disable=E1101

    @classproperty
    def API_KEY(cls): # pylint: disable=E0213
        return cls._json['apiKey'] # pylint: disable=E1101

    @classproperty
    def json(cls): # pylint: disable=E0213
        return cls._json # pylint: disable=E1101

    @classproperty
    def valid_versions(cls): # pylint: disable=E0213
        return cls._valid_versions

    @classproperty
    def min_version(cls): # pylint: disable=E0213
        return 0

    @classproperty
    def max_version(cls): # pylint: disable=E0213
        if cls._valid_versions is not None:
            return cls._valid_versions[1] # pylint: disable=E1136
        return None

    @classmethod
    def flexible_version_q(cls, version):
        if cls._flexible_versions is not None:
            if cls._flexible_versions[0] <= version <= cls._flexible_versions[1]: # pylint: disable=E1136
                return True
        return False

    @classmethod
    def is_request(cls):
        return cls.type == 'request'

    # allow override by api-specific classes (e.g., ProduceRequest)
    def expect_response(self):
        return True

    @property
    def API_VERSION(self):
        return self._version if self._version is not None else self._class_version # pylint: disable=E1101

    @API_VERSION.setter
    def API_VERSION(self, version):
        if not 0 <= version <= self.max_version:
            raise ValueError('Invalid version %s (max version is %s).' % (version, self.max_version))
        self._version = version
        if self._header is not None:
            self._header.request_api_version = version

    @property
    def header(self):
        return self._header

    @classproperty
    def header_class(cls): # pylint: disable=E0213
        if cls.type == 'response':
            return ResponseHeader
        elif cls.type == 'request':
            return RequestHeader
        elif cls.type is None:
            return None
        else:
            raise ValueError('Expected request or response type: %s' % cls.type)

    def with_header(self, correlation_id=0, client_id='kafka-python'):
        if self.is_request():
            kwargs = {
                'request_api_key': self.API_KEY,
                'request_api_version': self.API_VERSION,
                'correlation_id': correlation_id,
                'client_id': client_id,
            }
        else:
            kwargs = {
                'correlation_id': correlation_id,
            }
        self._header = self.header_class(**kwargs)

    # allow override by api-specific classes (e.g., ApiVersionsResponse)
    def encode_header(self, flexible=False):
        return self._header.encode(flexible=flexible) # pylint: disable=E1120

    def encode_header_into(self, out, flexible=False):
        # Subclasses may override encode_header to change flexible flag
        # (e.g., ApiVersionsResponse forces flexible=False).
        # Fall back to bytes-based encode_header if not overridden at this level.
        header_bytes = self.encode_header(flexible=flexible)
        n = len(header_bytes)
        out.buf[out.pos:out.pos+n] = header_bytes
        out.pos += n

    @classmethod
    def parse_header(cls, data, version=None):
        version = cls._class_version if version is None else version
        if version is None:
            raise ValueError('Version required to decode data')
        elif not 0 <= version <= cls.max_version:
            raise ValueError('Invalid version %s (max version is %s).' % (version, cls.max_version))
        flexible = cls.flexible_version_q(version)
        return cls.header_class.decode(data, flexible=flexible) # pylint: disable=E1101

    def encode(self, version=None, header=False, framed=False):
        if version is not None:
            self.API_VERSION = version
        if self.API_VERSION is None:
            raise ValueError('Version required to encode data')
        if header and self._header is None:
            raise ValueError('No header found')

        flexible = self.flexible_version_q(self.API_VERSION)
        out = EncodeBuffer()
        if framed:
            out.pos += 4  # reserve space for frame size
        if header:
            self.encode_header_into(out, flexible=flexible)
        fast_encode = self._struct.compiled_encode_into(self.API_VERSION, compact=flexible, tagged=flexible)
        fast_encode(self, out)
        if framed:
            payload_size = out.pos - 4
            pack_into('>i', out.buf, 0, payload_size)
        return out.result()

    @classmethod
    def decode(cls, data, version=None, header=False, framed=False):
        version = cls._class_version if version is None else version
        if version is None:
            raise ValueError('Version required to decode data')
        elif not 0 <= version <= cls.max_version:
            raise ValueError('Invalid version %s (max version is %s).' % (version, cls.max_version))

        if isinstance(data, bytes):
            data = io.BytesIO(data)
        if framed:
            size = Int32.decode(data)
        if header:
            hdr = cls.parse_header(data, version=version)
        else:
            hdr = None

        # Resolve versioned class to primary class (e.g., Response_v0 -> Response)
        data_class = cls[None]

        flexible = cls.flexible_version_q(version)
        ret = cls._struct.decode(data, version=version, compact=flexible, tagged=flexible, data_class=data_class)
        if hdr is not None:
            ret._header = hdr
        return ret
