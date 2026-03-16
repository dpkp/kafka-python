import re


class BaseField:
    FIELD_TYPES = []

    def __init_subclass__(cls, **kw):
        cls.FIELD_TYPES.append(cls)

    @classmethod
    def parse_json_fields(cls, json):
        return tuple(map(BaseField.parse_json, json.get('fields', []))) or None # Note: DFS Field construction

    @classmethod
    def parse_json(cls, json):
        if 'type' not in json:
            raise ValueError('No type found in json')
        type_str = json['type']
        for field_type in cls.FIELD_TYPES:
            maybe_type = field_type.parse_json(json)
            if maybe_type is not None:
                return maybe_type
        else:
            raise ValueError('Unable to parse field type: %s' % type_str)

    @classmethod
    def underscore_name(cls, name):
        return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()

    @classmethod
    def parse_versions(cls, versions):
        if versions is None or versions.strip() == '':
            return None
        elif versions.strip() == 'none':
            return (-1, -1)
        elif versions[-1] == '+':
            return (int(versions[:-1]), 32767)
        elif versions.find('-') == -1:
            return (int(versions), int(versions))
        else:
            return tuple(map(int, versions.split('-')))

    def __init__(self, json):
        self._json = json
        self._name = json['name']
        # versions tells when to include the field in encode / decode
        # 'validVersions' is included for top-level request/response structs
        # otherwise this should always be 'versions'
        for versions_key in ('versions', 'validVersions'):
            if versions_key in json:
                self._versions = self.parse_versions(json[versions_key])
                if self._versions is not None:
                    break
        else:
            raise ValueError('Unable to find versions in json!')
        # If the field is tagged, it should have 'tag' and 'taggedVersions'
        self._tag = json.get('tag')
        self._tagged_versions = self.parse_versions(json.get('taggedVersions'))
        # flexibleVersions is used to override 'compact' string/bytes encoding for a specific field
        # currently only used in RequestHeader client_id
        self._flexible_versions = self.parse_versions(json.get('flexibleVersions'))
        self._nullable_versions = self.parse_versions(json.get('nullableVersions'))
        self._type_str = json['type']
        if 'fields' in json:
            self._fields = self.parse_json_fields(json)
        else:
            self._fields = None
        self._ignorable = json.get('ignorable')
        self._entity_type = json.get('entityType')
        self._about = json.get('about', '')
        self._zero_copy = json.get('zeroCopy') # ?

    @property
    def name(self):
        return self.underscore_name(self._name)

    @property
    def type_str(self):
        return self._type_str

    @property
    def tag(self):
        return self._tag

    # Override in subclasses
    def is_array(self):
        return False

    def is_struct(self):
        return False

    def is_struct_array(self):
        return False

    def has_data_class(self):
        return False

    def __call__(self, *args, **kwargs):
        raise NotImplementedError

    @property
    def fields(self):
        return self._fields

    @property
    def min_version(self):
        return self._versions[0]

    @property
    def max_version(self):
        return self._versions[1]

    def for_version_q(self, version):
        return self.min_version <= version <= self.max_version

    def tagged_field_q(self, version):
        if self._tag is None or self._tagged_versions is None:
            return False
        elif not self._tagged_versions[0] <= version <= self._tagged_versions[1]:
            return False
        else:
            return True

    def _calculate_default(self, default):
        return default

    @property
    def default(self):
        if not hasattr(self, '_default'):
            setattr(self, '_default', self._calculate_default(self._json.get('default', '')))
        return self._default # pylint: disable=E1101

    def encode(self, value, version=None, compact=False, tagged=False):
        raise NotImplementedError

    def decode(self, data, version=None, compact=False, tagged=False):
        raise NotImplementedError

    def __repr__(self):
        return 'BaseField(%s)' % self._json

    def __eq__(self, other):
        if self.__class__ != other.__class__:
            return False
        if self.name != other.name:
            return False
        if self.tag != other.tag:
            return False
        return True
