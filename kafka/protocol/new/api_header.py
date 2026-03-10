from .api_struct import ApiStruct
from .api_struct_data import ApiStructData, ApiStructMeta
from .field import Field, parse_versions
from .schema import load_json


class ApiHeaderMeta(ApiStructMeta):
    def __new__(metacls, name, bases, attrs, **kw):
        if kw.get('init', True):
            json = load_json(name)
            attrs['_json'] = json
            attrs['_struct'] = ApiStruct(json['name'], tuple(map(Field, json.get('fields', []))))
        return super().__new__(metacls, name, bases, attrs, **kw)


class ApiHeader(ApiStructData, metaclass=ApiHeaderMeta, init=False):
    __slots__ = ()

    def __init_subclass__(cls, **kw):
        super().__init_subclass__(**kw)
        if kw.get('init', True):
            # pylint: disable=E1101
            assert cls._json['type'] == 'header'
            cls._flexible_versions = parse_versions(cls._json['flexibleVersions'])
            cls._valid_versions = parse_versions(cls._json['validVersions'])

    def encode(self, flexible=False):
        # Request versions are 1-2, Response versions are 0-1
        version = self._flexible_versions[0] if flexible else self._valid_versions[0] # pylint: disable=E1136
        return super().encode(version=version, compact=False, tagged=flexible)

    @classmethod
    def decode(cls, data, flexible=False):
        # Request versions are 1-2, Response versions are 0-1
        version = cls._flexible_versions[0] if flexible else cls._valid_versions[0] # pylint: disable=E1136
        return cls._struct.decode(data, version=version, compact=False, tagged=flexible, data_class=cls)


class ResponseClassRegistry:
    _response_class_registry = {}

    @classmethod
    def register_response_class(cls, response_class):
        cls._response_class_registry[response_class.API_KEY] = response_class

    @classmethod
    def get_response_class(cls, request_header):
        response_class = cls._response_class_registry.get(request_header.request_api_key)
        if response_class is not None:
            return response_class[request_header.request_api_version]


class RequestHeader(ApiHeader):
    def get_response_class(self):
        return ResponseClassRegistry.get_response_class(self)


class ResponseHeader(ApiHeader): pass
