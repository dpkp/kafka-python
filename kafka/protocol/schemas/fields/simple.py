import uuid

from .base import BaseField
from .codecs import (
    BitField, Boolean, Bytes,
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
        'string': String('utf-8'),
        'bytes': Bytes,
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

    def is_batchable(self):
        return getattr(self._type, 'batchable', False)

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
                return None
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
        return self._type.encode(value, compact=compact)

    def encode_into(self, value, out, version=None, compact=False, tagged=False):
        self._type.encode_into(out, value, compact=compact)

    def emit_encode_into(self, ctx, val_expr, indent, version=None, compact=False, tagged=False):
        self._type.emit_encode_into(ctx, val_expr, indent, compact=compact)

    def emit_decode_from(self, ctx, var_name, indent, version=None, compact=False, tagged=False):
        self._type.emit_decode_from(ctx, var_name, indent, compact=compact)

    def decode(self, data, version=None, compact=False, tagged=False):
        return self._type.decode(data, compact=compact)

    def to_json(self, val):
        if val is None:
            return None
        if self._type is UUID:
            return str(val)
        elif self._type is Bytes:
            if not isinstance(val, (bytes, bytearray, memoryview)):
                val = val.encode()
            if not isinstance(val, memoryview):
                val = val.tobytes()
            return val.decode(errors='backslashreplace')
        elif self._type is BitField:
            return list(val)
        else:
            return val

    def __repr__(self):
        return 'SimpleField(%s)' % self._json
