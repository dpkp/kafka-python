import io
import weakref

from .api_header import RequestHeader, ResponseHeader
from .api_struct import ApiStruct
from .api_struct_data import ApiStructData, ApiStructMeta
from .field import Field, parse_versions
from .schema import load_json
from ..types import Int32

from kafka.util import classproperty


class ApiMessageMeta(ApiStructMeta):
    def __new__(metacls, name, bases, attrs, **kw):
        # Pass init=False from base classes
        if kw.get('init', True):
            json = load_json(name)
            kw['struct'] = ApiStruct(json['name'], tuple(map(Field, json.get('fields', []))))
            attrs['_json'] = json
            attrs['_name'] = json['name']
            attrs['_type'] = json['type']
            attrs['_api_key'] = json['apiKey']
            attrs['_flexible_versions'] = parse_versions(json['flexibleVersions'])
            attrs['_valid_versions'] = parse_versions(json['validVersions'])
            attrs['_class_version'] = kw.get('version', attrs.get('_class_version'))
            attrs['__doc__'] = json.get('doc')
            attrs['__license__'] = json.get('license')
        return super().__new__(metacls, name, bases, attrs, **kw)

    def __init__(cls, name, bases, attrs, **kw):
        super().__init__(name, bases, attrs, **kw)
        if kw.get('init', True):
            assert cls._type in ('request', 'response')
            assert cls._valid_versions is not None
            # The primary message class has _version = None
            # and a _VERSIONS dict that provides access to version-specific wrappers
            # We also include cls[None] -> primary class to "exit" a version class
            if cls._class_version is None:
                cls._VERSIONS = {None: weakref.proxy(cls)}
            # Configure the ApiStruct to use our ApiMessage wrapper
            # and not construct a default ApiStructData
            cls._struct._data_class = weakref.proxy(cls)

    def __getitem__(cls, version):
        # Use [] lookups to move from primary class to "versioned" classes
        # which are simple wrappers around the primary class but with a _version attr
        if cls._class_version is not None:
            return cls._VERSIONS[None].__getitem__(version)
        klass_name = cls.__name__ + '_v' + str(version)
        if klass_name in cls._VERSIONS:
            return cls._VERSIONS[klass_name]
        cls._VERSIONS[klass_name] = type(klass_name, tuple(cls.mro()), {'_class_version': version}, init=False)
        return cls._VERSIONS[klass_name]


class ApiMessage(ApiStructData, metaclass=ApiMessageMeta, init=False):
    __slots__ = ('_header', '_version')
    _json = None
    _name = None
    _type = None
    _api_key = None
    _flexible_versions = None
    _valid_versions = None
    _struct = None
    _class_version = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._header = None
        self._version = None
        if 'version' in kwargs:
            self.API_VERSION = kwargs['version']

    @classproperty
    def name(cls): # pylint: disable=E0213
        return cls._name

    @classproperty
    def type(cls): # pylint: disable=E0213
        return cls._type

    @classproperty
    def API_KEY(cls): # pylint: disable=E0213
        return cls._api_key

    @classproperty
    def json(cls): # pylint: disable=E0213
        return cls._json

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

    def to_schema(self, version):
        flex_version = self.flexible_version_q(version)
        return self._struct.to_schema(version, compact=flex_version, tagged=flex_version)

    @classmethod
    def is_request(cls):
        return cls._type == 'request'

    # allow override by api-specific classes (e.g., ProduceRequest)
    def expect_response(self):
        return True

    @property
    def API_VERSION(self):
        return self._version if self._version is not None else self._class_version

    @API_VERSION.setter
    def API_VERSION(self, version):
        if self._class_version is not None and self._class_version != version:
            raise ValueError("Version has already been set by class")
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

    @classmethod
    def parse_header(cls, data, flexible=False):
        return cls.header_class.decode(data, flexible=flexible) # pylint: disable=E1101

    def encode(self, version=None, header=False, framed=False):
        if version is not None:
            self.API_VERSION = version
        if self.API_VERSION is None:
            raise ValueError('Version required to encode data')
        if header and self._header is None:
            raise ValueError('No header found')

        flexible = self.flexible_version_q(self.API_VERSION)
        encoded = self._struct.encode(self, version=self.API_VERSION, compact=flexible, tagged=flexible)
        if not header and not framed:
            return encoded
        bits = [encoded]
        if header:
            bits.insert(0, self.encode_header(flexible=flexible))
        if framed:
            bits.insert(0, Int32.encode(sum(map(len, bits))))
        return b''.join(bits)

    @classmethod
    def decode(cls, data, version=None, header=False, framed=False):
        version = cls._class_version if version is None else version
        if version is None:
            raise ValueError('Version required to decode data')
        elif not 0 <= version <= cls.max_version:
            raise ValueError('Invalid version %s (max version is %s).' % (version, cls.max_version))
        # Return current class except: current class is versioned and diff version is requested
        if cls._class_version is not None and cls._class_version != version:
            data_class = cls[version]
        else:
            data_class = cls

        flexible = cls.flexible_version_q(version)
        if isinstance(data, bytes):
            data = io.BytesIO(data)
        if framed:
            size = Int32.decode(data)
        if header:
            hdr = cls.parse_header(data, flexible=flexible)
        else:
            hdr = None
        ret = cls._struct.decode(data, version=version, compact=flexible, tagged=flexible, data_class=data_class)
        if hdr is not None:
            ret._header = hdr
        return ret
