from .base import BaseField
from .simple import SimpleField
from .codecs import (
    Array, CompactArray,
    UnsignedVarInt32, Int32,
)


class ArrayField(BaseField):
    @classmethod
    def parse_inner_type(cls, json):
        if 'fields' in json:
            return
        type_str = cls.parse_array_type(json)
        if type_str is not None:
            inner_json = {**json, 'type': type_str}
            return SimpleField.parse_json(inner_json)

    @classmethod
    def parse_array_type(cls, json):
        if json['type'].startswith('[]'):
            type_str = json['type'][2:]
            assert not type_str.startswith('[]'), 'Unexpected double-array type: %s' % json['type']
            return type_str

    @classmethod
    def parse_json(cls, json):
        inner_type = cls.parse_inner_type(json)
        if inner_type is not None:
            return cls(json, array_of=inner_type)

    def __init__(self, json, array_of=None):
        if array_of is None:
            array_of = self.parse_inner_type(json)
            assert array_of is not None, 'json does not contain a (simple) Array!'
        super().__init__(json)
        self.array_of = array_of # SimpleField

    def is_array(self):
        return True

    def to_schema(self, version, compact=False, tagged=False):
        if not self.for_version_q(version) or self.tagged_field_q(version):
            return None
        arr_type = CompactArray if compact else Array
        inner_type = self.array_of.to_schema(version, compact=compact, tagged=tagged)
        return (self.name, arr_type(inner_type))

    def _calculate_default(self, default):
        if default == 'null':
            return None
        elif not default:
            return []
        else:
            raise ValueError('Invalid default for field %s. The only valid default is empty or null.' % self._name)

    def encode(self, items, version=None, compact=False, tagged=False):
        assert version is not None, 'version is required to encode Field'
        if not self.for_version_q(version):
            return b''
        if compact:
            size = UnsignedVarInt32.encode(len(items) + 1 if items is not None else 0)
        else:
            size = Int32.encode(len(items) if items is not None else -1)
        if items is None:
            return size
        fields = [self.array_of.encode(item, version=version, compact=compact, tagged=tagged)
                  for item in items]
        return b''.join([size] + fields)

    def decode(self, data, version=None, compact=False, tagged=False):
        assert version is not None, 'version is required to decode Field'
        if not self.for_version_q(version):
            return None
        if compact:
            size = UnsignedVarInt32.decode(data)
            size -= 1
        else:
            size = Int32.decode(data)
        if size == -1:
            return None
        return [self.array_of.decode(data, version=version, compact=compact, tagged=tagged)
                for _ in range(size)]
