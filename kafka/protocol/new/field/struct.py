import weakref

from ..data_container import DataContainer
from .field import Field
from ..tagged_fields import TaggedFields
from ...types import Schema


class StructField(Field):
    @classmethod
    def parse_json(cls, json):
        if 'type' not in json or json['type'].startswith('[]'):
            return
        if 'fields' in json:
            return cls(json)

    # Cases
    # oldschool  - standard types, no tagged fields
    # newschool  - compact types, tagged fields
    # nested tag - compact types, no (nested) tagged fields
    def __init__(self, json):
        super().__init__(json)
        self._field_map = {field.name: field for field in self._fields}
        self._data_class = None

    @property
    def fields(self):
        return self._field_map

    def is_struct(self):
        return True

    def has_data_class(self):
        return True

    @property
    def data_class(self):
        if self._data_class is None:
            self._data_class = type(self._type_str, (DataContainer,), {'_struct': weakref.proxy(self)})
        return self._data_class

    def to_schema(self, version, compact=False, tagged=False):
        if not self.for_version_q(version):
            return None
        version_fields = [field.to_schema(version, compact=compact, tagged=tagged)
                          for field in self._fields]
        if tagged:
            version_fields.append(('tags', self.tagged_fields(version)))
        return Schema(*[field for field in version_fields if field is not None])

    def _calculate_default(self, default):
        if default == 'null':
            return None
        if self._tag is not None:
            return None
        elif not default:
            raise NotImplementedError(f"Default value not implemented for struct field '{self._name}'")
        else:
            raise ValueError('Invalid default for struct field %s. The only valid default is null.' % self._name)

    def tagged_fields(self, version):
        return TaggedFields([field for field in self._fields
                             if field.for_version_q(version)
                             and field.tagged_field_q(version)])

    def encode(self, item, version=None, compact=False, tagged=False):
        assert version is not None, 'version required to encode Field'
        if not self.for_version_q(version):
            return b''
        # TODO: assert version is not None or isinstance(self, ApiMessage), 'version is required to encode non-schema structs'
        fields = [field for field in self._fields if field.for_version_q(version) and not field.tagged_field_q(version)]
        if isinstance(item, tuple):
            getter = lambda item, i, field: item[i]
            tags = {} if len(item) == len(fields) else item[-1]
        elif isinstance(item, dict):
            getter = lambda item, i, field: item.get(field.name) # defaults?
            tags = item
        else:
            getter = lambda item, i, field: getattr(item, field.name)
            tags = item
        encoded = [field.encode(getter(item, i, field),
                                version=version, compact=compact, tagged=tagged)
                   for i, field in enumerate(fields)]
        if tagged:
            # TaggedFields are always compact and never include nested tagged fields
            encoded.append(self.tagged_fields(version).encode(tags, version=version,
                                                               compact=True, tagged=False))
        return b''.join(encoded)

    def decode(self, data, version=None, compact=False, tagged=False, data_class=None):
        assert version is not None, 'version required to encode Field'
        if not self.for_version_q(version):
            return None
        if data_class is None:
            data_class = self.data_class
        decoded = {
            field.name: field.decode(data, version=version, compact=compact, tagged=tagged)
            for field in self._fields
            if field.for_version_q(version) and not field.tagged_field_q(version)
        }
        if tagged:
            decoded.update(self.tagged_fields(version).decode(data, version=version, compact=True, tagged=False))
        return data_class(**decoded)

    def __len__(self):
        return len(self._fields)

    def __eq__(self, other):
        if not super().__eq__(other):
            return False
        if self._fields != other._fields:
            return False
        return True

    def __repr__(self):
        return '%s(%s, %s)' % (self.__class__.__name__, self._name, self._fields)
