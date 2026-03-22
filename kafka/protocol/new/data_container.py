from kafka.util import classproperty


class SlotsBuilder(type):
    def __new__(metacls, name, bases, attrs, **kw):
        if attrs.get('_struct') is not None:
            attrs['__slots__'] = attrs.get('__slots__', ()) + tuple(attrs['_struct'].fields.keys())
        return super().__new__(metacls, name, bases, attrs)


class DataContainer(metaclass=SlotsBuilder):
    __slots__ = ('tags', 'unknown_tags', '_version')
    _struct = None

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # Generate field data_classes and set as class attrs (by field.type_str)
        if cls._struct is not None:
            for field in cls._struct.fields.values():
                if field.is_struct() or field.is_struct_array():
                    if not field.has_data_class():
                        field.set_data_class(type(field.type_str, (DataContainer,), {'_struct': field}))
                    setattr(cls, field.type_str, field.data_class)

    def __init__(self, *args, version=None, **field_vals):
        assert self._struct is not None
        if version is not None and not self._struct.min_version <= version <= self._struct.max_version:
            raise ValueError(f'Invalid version: {version} (min={self._struct.min_version}, max={self._struct.max_version})')
        self._version = version
        # Support positional arg init for convenience
        if len(args) > 0:
            if self._version is not None:
                field_args = [field for field in self._struct._fields if field.for_version_q(self._version)]
            else:
                field_args = self._struct._fields
            if len(args) > len(field_args):
                raise RuntimeError('Unable to init DataContainer with %d positional args: unexpected %d' % (len(args), len(field_args)))
            field_vals.update({field_args[i].name: arg for i, arg in enumerate(args)})
            args = ()
        self.tags = None
        self.unknown_tags = None
        for field in self._struct._fields:
            if field.name in field_vals and field.tag is not None:
                if self.tags is None:
                    self.tags = set()
                self.tags.add(field.name)
            setattr(self, field.name, field_vals.pop(field.name, field.default))

        for name in list(field_vals.keys()):
            if name.startswith('_'):
                if self.unknown_tags is None:
                    self.unknown_tags = {}
                self.unknown_tags[name] = field_vals.pop(name)

        if field_vals:
            raise ValueError('Unrecognized fields for type %s: %s' % (self._struct.name, field_vals))

    @property
    def version(self):
        return self._version

    def encode(self, *args, **kwargs):
        """Add version= to kwargs, otherwise pass-through to _struct"""
        return self._struct.encode(self, *args, **kwargs)

    @classmethod
    def decode(cls, data, **kwargs):
        """Add version= to kwargs, otherwise pass-through to _struct"""
        return cls._struct.decode(data, **kwargs)

    @classproperty
    def fields(cls): # pylint: disable=E0213
        return cls._struct.fields

    def __repr__(self):
        if self._version is not None:
            v_filter = lambda field: field.for_version_q(self._version)
            key_vals = ['version=%s' % self._version]
        else:
            v_filter = lambda field: True
            key_vals = []
        for field in filter(v_filter, self._struct._fields):
            key_vals.append('%s=%s' % (field.name, repr(getattr(self, field.name))))
        return self.__class__.__name__ + '(' + ', '.join(key_vals) + ')'

    def __eq__(self, other):
        # For backwards compatibility Data struct is equal to tuple with same field values
        if isinstance(other, tuple):
            # TODO: handle fields changes by version?
            if len(other) < len(self._struct._fields):
                return False
            for i, field in enumerate(self._struct._fields):
                if getattr(self, field.name) != other[i]:
                    return False
            if len(other) == len(self._struct._fields):
                return True
            elif len(other) == len(self._struct._fields) + 1 and isinstance(other[-1], dict) and other[-1] == {}:
                # TODO: Handle non-empty tag dicts...
                return True
            return False
        if self.__class__ != other.__class__:
            return False
        if self._struct != other._struct:
            return False
        for field in self._struct._fields:
            if getattr(self, field.name) != getattr(other, field.name):
                return False
        return True

    def __iter__(self):
        if self._version is None:
            raise RuntimeError('DataContainer Iteration not supported without _version')
        return iter([getattr(self, field.name) for field in self._struct.untagged_fields(self._version)])

    def _to_dict_vals(self, meta=False, json=True):
        if meta:
            yield ('_type', self.__class__.__name__)
            yield ('_version', self._version)
            if meta != 'all':
                meta=False
        for field in self._struct._fields:
            if self._version is not None and not field.for_version_q(self._version):
                continue
            if field.is_struct():
                yield (field.name, dict(getattr(self, field.name)._to_dict_vals(meta=meta, json=json)))
            elif field.is_struct_array():
                yield (field.name, [dict(val._to_dict_vals(meta=meta, json=json)) for val in getattr(self, field.name)])
            else:
                val = getattr(self, field.name)
                if json:
                    if isinstance(val, bytes):
                        val = val.decode()
                    elif isinstance(val, set):
                        val = list(val)
                yield (field.name, val)

    def to_dict(self, meta=False, json=True):
        """Use meta=True to include top-level version; meta='all' to include all internal versions
        json=False to return raw encoding; json=True (default) to convert values to be json-serializable
        """
        return dict(self._to_dict_vals(meta=meta, json=json))

    def __getitem__(self, key):
        if self._version is None:
            raise RuntimeError('DataContainer subscript not supported without _version')
        elif isinstance(key, int):
            field = self._struct.untagged_fields(self._version)[key]
            return getattr(self, field.name)
        elif isinstance(key, slice):
            fields = self._struct.untagged_fields(self._version)
            start, stop, step = key.indices(len(fields))
            return [getattr(self, fields[i].name) for i in range(start, stop, step)]
        else:
            raise TypeError('DataContainer subscript supports int or slices only: %s' % type(key).__name__)
