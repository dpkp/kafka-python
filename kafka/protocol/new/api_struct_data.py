from kafka.util import classproperty


class ApiStructMeta(type):
    def __new__(metacls, name, bases, attrs, **kw):
        if kw.get('init', True):
            struct = kw.get('struct', attrs.get('_struct'))
            assert struct is not None
            attrs['_struct'] = struct
            attrs['__slots__'] = attrs.get('__slots__', ()) + tuple(attrs['_struct'].fields.keys())
            # Link sub-struct data_classes as class attrs
            for field in attrs['_struct'].fields.values():
                if field.has_data_class():
                    attrs[field.data_class.__name__] = field.data_class
        return super().__new__(metacls, name, bases, attrs)


class ApiStructData(metaclass=ApiStructMeta, init=False):
    __slots__ = ('tags', 'unknown_tags')
    _struct = None

    def __init__(self, **field_vals):
        assert self._struct is not None
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

    def encode(self, *args, **kwargs):
        """Add version= to kwargs, otherwise pass-through to ApiStruct"""
        return self._struct.encode(self, *args, **kwargs)

    @classmethod
    def decode(cls, data, **kwargs):
        """Add version= to kwargs, otherwise pass-through to ApiStruct"""
        return cls._struct.decode(data, **kwargs)

    @classproperty
    def fields(cls):
        return cls._struct.fields

    def __repr__(self):
        key_vals = ['%s=%s' % (field.name, repr(getattr(self, field.name)))
                    for field in self._struct._fields]
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
