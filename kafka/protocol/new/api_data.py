import io
import weakref

from kafka.util import classproperty

from .data_container import DataContainer, SlotsBuilder
from .schemas import BaseField, StructField, load_json
from .schemas.fields.codecs import Int16, Int32


class JsonSchemaData(SlotsBuilder):
    def __new__(metacls, name, bases, attrs, **kw):
        if kw.get('init', True):
            json = load_json(name)
            if 'json_patch' in attrs:
                json = attrs['json_patch'].__func__(metacls, json)
            attrs['_json'] = json
            attrs['_struct'] = StructField(json)
            if 'doc' in json:
                attrs['__doc__'] = attrs.get('__doc__', '') + "\nNotes from json schema:\n" + json.get('doc')
            attrs['__license__'] = json.get('license')
        return super().__new__(metacls, name, bases, attrs, **kw)

    def __init__(cls, name, bases, attrs, **kw):
        super().__init__(name, bases, attrs, **kw)
        if kw.get('init', True):
            cls._struct.set_data_class(weakref.proxy(cls))


class ApiData(DataContainer, metaclass=JsonSchemaData, init=False):
    def __init_subclass__(cls, **kw):
        super().__init_subclass__(**kw)
        if kw.get('init', True):
            # pylint: disable=E1101
            assert cls._json is not None
            assert cls._json['type'] == 'data'
            cls._flexible_versions = BaseField.parse_versions(cls._json['flexibleVersions'])
            cls._valid_versions = BaseField.parse_versions(cls._json['validVersions'])

    def __init__(self, *args, **kwargs):
        if len(args) > 0 and isinstance(args[0], int) and 'version' not in kwargs:
            kwargs['version'] = args[0]
            args = tuple(args[1:])
        super().__init__(*args, **kwargs)

    @classproperty
    def name(cls): # pylint: disable=E0213
        return cls._json['name'] # pylint: disable=E1101

    @classproperty
    def type(cls): # pylint: disable=E0213
        return cls._json['type'] # pylint: disable=E1101

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

    @classproperty
    def header_class(cls): # pylint: disable=E0213
        return Int16

    def encode_header(self, flexible=False):
        assert self._version is not None
        return self.header_class.encode(self._version)

    @classmethod
    def parse_header(cls, data):
        return cls.header_class.decode(data)

    def encode(self, version=None, header=True, framed=False):
        if version is not None:
            self._version = version
        elif self._version is None:
            raise ValueError('Version required to encode data')
        flexible = self.flexible_version_q(self._version)
        encoded = self._struct.encode(self, version=self._version, compact=flexible, tagged=flexible)
        if not header and not framed:
            return encoded
        bits = [encoded]
        if header:
            bits.insert(0, self.encode_header(flexible=flexible))
        if framed:
            bits.insert(0, Int32.encode(sum(map(len, bits))))
        return b''.join(bits)

    @classmethod
    def decode(cls, data, version=None, header=True, framed=False):
        if not header:
            if version is None:
                raise ValueError('Version required to decode data')
            elif not 0 <= version <= cls.max_version:
                raise ValueError('Invalid version %s (max version is %s).' % (version, cls.max_version))
        if isinstance(data, bytes):
            data = io.BytesIO(data)
        if framed:
            size = Int32.decode(data)
        if header:
            decoded_version = cls.parse_header(data)
            if version is not None:
                if version > decoded_version:
                    raise ValueError('Version mismatch: found v%d, expected v%d' % (decoded_version, version))
            version = min(decoded_version, cls.max_version)
        flexible = cls.flexible_version_q(version)
        return cls._struct.decode(data, version=version, compact=flexible, tagged=flexible)
