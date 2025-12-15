from .api_struct import ApiStruct
from .api_struct_data import ApiStructData, ApiStructMeta
from .field import Field, parse_versions
from .schema import load_json


class ApiHeaderMeta(ApiStructMeta):
    def __new__(metacls, name, bases, attrs, **kw):
        if kw.get('init', True):
            json = load_json(name)
            kw['struct'] = ApiStruct(json['name'], tuple(map(Field, json.get('fields', []))))
            attrs['_json'] = json
            attrs['_name'] = json['name']
            attrs['_type'] = json['type']
            attrs['_flexible_versions'] = parse_versions(json['flexibleVersions'])
            attrs['_valid_versions'] = parse_versions(json['validVersions'])
        return super().__new__(metacls, name, bases, attrs, **kw)

    def __init__(cls, name, bases, attrs, **kw):
        super().__init__(name, bases, attrs, **kw)
        if name != 'ApiHeader':
            assert cls._type == 'header'
            assert cls._valid_versions is not None


class ApiHeader(ApiStructData, metaclass=ApiHeaderMeta, init=False):
    __slots__ = ()
    _json = None
    _name = None
    _type = None
    _flexible_versions = None
    _valid_versions = None
    _struct = None

    def encode(self, flexible=False):
        # Request versions are 1-2, Response versions are 0-1
        version = self._flexible_versions[0] if flexible else self._valid_versions[0]
        return super().encode(version=version, compact=False, tagged=flexible)

    @classmethod
    def decode(cls, data, flexible=False):
        # Request versions are 1-2, Response versions are 0-1
        version = cls._flexible_versions[0] if flexible else cls._valid_versions[0]
        return cls._struct.decode(data, version=version, compact=False, tagged=flexible, data_class=cls)


class RequestHeader(ApiHeader): pass
class ResponseHeader(ApiHeader): pass
