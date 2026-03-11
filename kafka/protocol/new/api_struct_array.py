from .api_array import ApiArray
from .api_struct import ApiStruct


class ApiStructArray(ApiArray):
    @classmethod
    def parse_inner_type(cls, json):
        # ApiStructArray requires non-empty fields
        if 'fields' not in json:
            return
        assert len(json['fields']) > 0, 'Unexpected empty fields in json'
        type_str = cls.parse_array_type(json)
        if type_str is None:
            return
        inner_json = {**json, 'type': type_str}
        return ApiStruct.parse_json(inner_json)

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

    def is_struct_array(self):
        return True

    @property
    def fields(self):
        return self.array_of.fields

    def has_data_class(self):
        return True

    @property
    def data_class(self):
        return self.array_of.data_class

    def __call__(self, *args, **kw):
        return self.data_class(*args, **kw) # pylint: disable=E1102
