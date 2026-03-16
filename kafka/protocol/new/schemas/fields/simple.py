import uuid

from .base import BaseField
from .codecs import (
    BitField, Boolean, Bytes, CompactBytes, CompactString,
    Float64, Int8, Int16, Int32, Int64, String, UUID
)


class SimpleField(BaseField):
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
        'bitfield': BitField, # patched only; does not exist in raw schemas
    }

    @classmethod
    def parse_json(cls, json):
        if 'fields' not in json and json['type'] in cls.TYPES:
            return cls(json)

    def __init__(self, json):
        if 'fields' in json:
            raise ValueError('Fields not allowed in SimpleField!')
        super().__init__(json)
        if self._type_str not in self.TYPES:
            raise ValueError('Unrecognized type: %s' % self._type_str)
        self._type = self.TYPES[self._type_str]

    def to_schema(self, version, compact=False, tagged=False):
        if compact and self._type is Bytes:
            schema_type = CompactBytes
        elif compact and isinstance(self._type, String):
            schema_type = CompactString(self._type.encoding)
        else:
            schema_type = self._type
        if self.name is None:
            return schema_type
        else:
            return (self.name, schema_type)

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
        elif self._type is BitField:
            if not default:
                return None
            else:
                default = BitField.from_32_bit_field(int(default))
                if default == {31}:
                    return None
                return default
        elif default == 'null':
            return None
        elif isinstance(self._type, String):
            return default
        elif not default:
            if self._type is Bytes:
                return b''
        else:
            raise ValueError('Invalid default for field %s. The only valid default is empty or null.' % self._name)

    def encode(self, value, version=None, compact=False, tagged=False):
        assert version is not None, 'version is required to encode Field'
        if not self.for_version_q(version):
            return b''
        if compact and self._type is Bytes:
            return CompactBytes.encode(value)
        elif compact and isinstance(self._type, String):
            return CompactString(self._type.encoding).encode(value)
        else:
            return self._type.encode(value)

    def decode(self, data, version=None, compact=False, tagged=False):
        assert version is not None, 'version is required to decode Field'
        if not self.for_version_q(version):
            return None
        print("decoding", self.name)
        if compact and self._type is Bytes:
            return CompactBytes.decode(data)
        elif compact and isinstance(self._type, String):
            return CompactString(self._type.encoding).decode(data)
        else:
            return self._type.decode(data)
