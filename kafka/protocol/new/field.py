import re
import uuid

from .api_array import ApiArray
from .api_struct import ApiStruct
from ..types import (
    Boolean, Bytes, CompactBytes, CompactString,
    Float64, Int8, Int16, Int32, Int64, String, UUID
)


def underscore_name(name):
    return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()


def parse_versions(versions, default=None):
    if versions is None:
        return default
    elif versions.strip() == '':
        return default
    elif versions.strip() == 'none':
        return None
    elif versions[-1] == '+':
        return (int(versions[:-1]), 32767)
    elif versions.find('-') == -1:
        return (int(versions), int(versions))
    else:
        return tuple(map(int, versions.split('-')))


class Field:
    TYPES = {
        'int8': Int8,
        'int16': Int16,
        #'uint16': UnsignedInt16,
        'int32': Int32,
        #'uint32': UnsignedInt32,
        'int64': Int64,
        'float64': Float64,
        'bool': Boolean,
        'uuid': UUID,
        'string': String('utf-8'), # CompactString if flexible version
        'bytes': Bytes, # CompactBytes if flexible version
        'records': Bytes,
    }

    def __init__(self, json):
        self._json = json
        self._name = json['name']
        self._tagged_versions = parse_versions(json.get('taggedVersions'))
        self._versions = parse_versions(json.get('versions'), default=self._tagged_versions)
        self._fields = tuple(map(Field, json.get('fields', []))) or None
        self._type_str = json['type']
        self._type = self._parse_type(json['type'], self._fields, self._tagged_versions)
        self._is_array = isinstance(self._type, ApiArray)
        self._is_struct = isinstance(self._type, ApiStruct)
        self._is_struct_array = isinstance(self._type, ApiArray) and isinstance(self._type.array_of, ApiStruct)
        self._map_key = json.get('mapKey')
        self._nullable_versions = parse_versions(json.get('nullableVersions'))
        self._ignorable = json.get('ignorable')
        self._entity_type = json.get('entityType')
        self._about = json.get('about', '')
        # used in rare cases for string/bytes fields that are not compact when rest of message is
        self._flexible_versions = parse_versions(json.get('flexibleVersions'))
        self._tag = json.get('tag')
        self._zero_copy = json.get('zeroCopy') # ?

    @property
    def tag(self):
        return self._tag

    @property
    def name(self):
        return underscore_name(self._name)

    @property
    def type(self):
        return self._type

    def is_array(self):
        return self._is_array

    def is_struct(self):
        return self._is_struct

    def is_struct_array(self):
        return self._is_struct_array

    def has_data_class(self):
        return self.is_struct() or self.is_struct_array()

    def __call__(self, *args, **kwargs):
        return self.data_class(*args, **kwargs) # pylint: disable=E1102

    @property
    def data_class(self):
        if self.is_struct():
            return self._type.data_class
        elif self.is_struct_array():
            return self._type.array_of.data_class
        else:
            raise ValueError('Non-struct field does not have a data_class!')

    @property
    def fields(self):
        if self.is_struct():
            return self._type.fields
        elif self.is_struct_array():
            return self._type.array_of.fields
        else:
            raise ValueError('Non-struct field does not have fields!')

    @property
    def min_version(self):
        return self._versions[0]

    @property
    def max_version(self):
        return self._versions[1]

    def _calculate_default(self, default):
        if self._type is Boolean:
            if not default:
                return False
            if isinstance(default, str):
                if default.lower() == 'false':
                    return False
                elif default.lower() == 'true':
                    return True
                else:
                    raise ValueError('Invalid default for boolean field %s: %s' % (self._name, default))
            return bool(default)
        elif self._type in (Int8, Int16, Int32, Int64):
            if not default:
                return 0
            if isinstance(default, str):
                if default.lower().startswith('0x'):
                    return int(default, 16)
                else:
                    return int(default)
            return int(default)
        elif self._type is UUID:
            if not default:
                return UUID.ZERO_UUID
            else:
                return uuid.UUID(default)
        elif self._type is Float64:
            if not default:
                return 0.0
            else:
                return float(default)
        elif default == 'null':
            self._validate_null_default()
            return None
        elif isinstance(self._type, String):
            return default
        elif not default:
            if self._type is Bytes:
                return b''
            elif isinstance(self._type, ApiStruct):
                if self.tag is not None:
                    return None
                raise NotImplementedError(f"Default value not implemented for struct field '{self._name}' of type '{self._type_str}'")
            elif isinstance(self._type, ApiArray):
                return []
        else:
            raise ValueError('Invalid default for field %s. The only valid default is empty or null.' % self._name)

    def _validate_null_default(self):
        if self._nullable_versions is not None:
            if self._nullable_versions[0] <= self._versions[0] and self._nullable_versions[1] >= self._versions[1]:
                return True
        raise ValueError('null cannot be the default for field %s, because not all versions of this field are nullable.' % self._name)

    @property
    def default(self):
        if not hasattr(self, '_default'):
            setattr(self, '_default', self._calculate_default(self._json.get('default', '')))
        return self._default # pylint: disable=E1101

    def _parse_type(self, type_str, fields, tagged_versions):
        if type_str in self.TYPES:
            assert fields is None
            return self.TYPES[type_str]
        elif type_str.startswith('[]'):
            type_str = type_str[2:]
            if type_str in self.TYPES:
                assert fields is None
                return ApiArray(self.TYPES[type_str])
            elif type_str[0].isupper():
                assert fields is not None
                return ApiArray(ApiStruct(type_str, fields))
            else:
                raise ValueError('Unable to parse field type')
        elif type_str[0].isupper():
            assert fields is not None
            # Note, tagged structs always use compact array
            return ApiStruct(type_str, fields)
        else:
            raise ValueError('Unable to parse field type: %s' % type_str)

    def to_schema(self, version, compact=False, tagged=False):
        if self._tag is not None:
            return None
        elif not self._versions[0] <= version <= self._versions[1]:
            return None
        elif isinstance(self._type, ApiStruct):
            schema_type = self._type.to_schema(version, compact=compact, tagged=tagged)
        elif isinstance(self._type, ApiArray):
            schema_type = self._type.to_schema(version, compact=compact, tagged=tagged)
        elif compact and self._type is Bytes:
            schema_type = CompactBytes
        elif compact and isinstance(self._type, String):
            schema_type = CompactString(self._type.encoding)
        else:
            schema_type = self._type
        return (self.name, schema_type)

    def for_version_q(self, version):
        return self.min_version <= version <= self.max_version

    def tagged_field_q(self, version):
        if self._tag is None:
            return False
        elif not self._tagged_versions[0] <= version <= self._tagged_versions[1]:
            return False
        else:
            return True

    def encode(self, value, version=None, compact=False, tagged=False):
        try:
            if version is None:
                version = self.max_version
            if self._tag is not None:
                return b''
            elif not self.min_version <= version <= self.max_version:
                return b''
            elif isinstance(self._type, (ApiArray, ApiStruct)):
                return self._type.encode(value, version=version, compact=compact, tagged=tagged)
            elif compact and self._type is Bytes:
                return CompactBytes.encode(value)
            elif compact and isinstance(self._type, String):
                return CompactString(self._type.encoding).encode(value)
            else:
                return self._type.encode(value)
        except ValueError as error:
            raise ValueError('Unable to encode field %s: %s' % (self.name, error))

    def decode(self, data, version=None, compact=False, tagged=False):
        try:
            if version is None:
                version = self.max_version
            if not self.min_version <= version <= self.max_version:
                return self.default
            if isinstance(self._type, (ApiArray, ApiStruct)):
                return self._type.decode(data, version=version, compact=compact, tagged=tagged)
            elif compact and self._type is Bytes:
                return CompactBytes.decode(data)
            elif compact and isinstance(self._type, String):
                return CompactString(self._type.encoding).decode(data)
            else:
                return self._type.decode(data)
        except ValueError as error:
            raise ValueError('Unable to decode field %s: %s' % (self.name, error))

    def __repr__(self):
        return 'Field(%s)' % self._json

    def __eq__(self, other):
        if self.__class__ != other.__class__:
            return False
        if self.name != other.name:
            return False
        if self.type != other.type:
            return False
        if self.tag != other.tag:
            return False
        return True
