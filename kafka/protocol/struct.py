import abc
from io import BytesIO

from kafka.protocol.abstract import AbstractType
from kafka.protocol.types import Schema, TaggedFields

from kafka.util import WeakMethod


class Struct(metaclass=abc.ABCMeta):

    @abc.abstractproperty
    def SCHEMA(self):
        """An instance of Schema() representing the structure"""
        pass

    ALIASES = {} # for compatibility with new protocol defs from json
    def __getattr__(self, name):
        if name in self.ALIASES:
            return getattr(self, self.ALIASES[name])
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    def __setattr__(self, name, value):
        if name in self.ALIASES:
            name = self.ALIASES[name]
        return super().__setattr__(name, value)

    def __init__(self, *args, **kwargs):
        if self.SCHEMA.has_tagged_fields():
            # Dont require TaggedFields value in *args
            if len(args) == len(self.SCHEMA) - 1:
                args = (*args, {})
            elif len(args) == len(self.SCHEMA) and args[-1] is None:
                args = (*args[:-1], {})
        if len(args) == len(self.SCHEMA):
            for i, name in enumerate(self.SCHEMA.names):
                setattr(self, name, args[i])
        elif len(args) > 0:
            raise ValueError('Args must be empty or mirror schema')
        else:
            if self.SCHEMA.has_tagged_fields():
                if kwargs.get('tags') is None:
                    kwargs['tags'] = {}
            for name in self.ALIASES:
                if name in kwargs:
                    kwargs[self.ALIASES[name]] = kwargs.pop(name)
            for name in self.SCHEMA.names:
                setattr(self, name, kwargs.pop(name, None))
            if kwargs:
                raise ValueError('Keyword(s) not in schema %s: %s'
                                 % (list(self.SCHEMA.names),
                                    ', '.join(kwargs.keys())))

    def encode(self):
        return self.SCHEMA.encode(
            [getattr(self, name) for name in self.SCHEMA.names]
        )

    @classmethod
    def decode(cls, data):
        if isinstance(data, bytes):
            data = BytesIO(data)
        return cls(*cls.SCHEMA.decode(data))

    def get_item(self, name):
        if name not in self.SCHEMA.names:
            raise KeyError("%s is not in the schema" % name)
        return getattr(self, name)

    def __repr__(self):
        key_vals = []
        for name, field in zip(self.SCHEMA.names, self.SCHEMA.fields):
            key_vals.append('%s=%s' % (name, field.repr(getattr(self, name))))
        return self.__class__.__name__ + '(' + ', '.join(key_vals) + ')'

    def __hash__(self):
        return hash(self.encode())

    def __eq__(self, other):
        if not isinstance(other, Struct):
            return False
        if self.SCHEMA != other.SCHEMA:
            return False
        for attr in self.SCHEMA.names:
            if getattr(self, attr) != getattr(other, attr):
                return False
        return True
