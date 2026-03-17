from .array import ArrayField
from .struct import StructField


class StructArrayField(ArrayField):
    @classmethod
    def parse_inner_type(cls, json):
        # StructArrayField requires non-empty fields
        if 'fields' not in json:
            return
        assert len(json['fields']) > 0, 'Unexpected empty fields in json'
        type_str = cls.parse_array_type(json)
        if type_str is None:
            return
        inner_json = {**json, 'type': type_str}
        return StructField.parse_json(inner_json)

    @classmethod
    def parse_json(cls, json):
        inner_type = cls.parse_inner_type(json)
        if inner_type is not None:
            return cls(json, array_of=inner_type)

    def __init__(self, json, array_of=None):
        if array_of is None:
            array_of = self.parse_inner_type(json)
            assert array_of is not None, 'json does not contain a StructArray!'
        super().__init__(json, array_of=array_of)
        # map_key will be (idx, field) of the mapKey field if found
        self.map_key = next(filter(lambda x: x[1]._json.get('mapKey'), enumerate(self._fields)), None)

    @property
    def type_str(self):
        return self._type_str[2:]

    def is_struct_array(self):
        return True

    @property
    def fields(self):
        return self.array_of.fields

    def tagged_fields(self, version):
        return self.array_of.tagged_fields(version)

    def untagged_fields(self, version):
        return self.array_of.untagged_fields(version)

    def has_data_class(self):
        return self.array_of.has_data_class()

    def set_data_class(self, data_class):
        return self.array_of.set_data_class(data_class)

    @property
    def data_class(self):
        return self.array_of.data_class

    def __call__(self, *args, **kw):
        return self.data_class(*args, **kw) # pylint: disable=E1102

    def __repr__(self):
        return 'StructArrayField(%s)' % self._json
