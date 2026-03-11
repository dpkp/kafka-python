from .field import Field
from ..types import (
    Array, CompactArray,
    UnsignedVarInt32, Int32,
)


class ApiArray(Field):
    @classmethod
    def parse_json(cls, json):
        if json['type'].startswith('[]'):
            inner_type_str = json['type'][2:]
            if inner_type_str.startswith('[]'): # this would be strange...
                return None
            inner_json = {**json, 'type': inner_type_str}
            inner_type = super().parse_json(inner_json)
            if inner_type is not None:
                return cls(json, array_of=inner_type)

    def __init__(self, json, array_of=None):
        super().__init__(json)
        self.array_of = array_of # Field (ApiStruct or FieldBasicType)

    def is_array(self):
        return True

    def is_struct_array(self):
        return self.array_of.is_struct()

    @property
    def fields(self):
        if self.is_struct_array():
            return self.array_of.fields

    def has_data_class(self):
        return self.is_struct_array()

    @property
    def data_class(self):
        if self.has_data_class():
            return self.array_of.data_class
        else:
            raise ValueError('Non-struct field does not have a data_class!')

    def __call__(self, *args, **kw):
        return self.data_class(*args, **kw) # pylint: disable=E1102

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
