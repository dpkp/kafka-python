from collections import namedtuple
from io import BytesIO

from .abstract import AbstractType
from .types import Schema


class Struct(AbstractType):
    SCHEMA = Schema()

    def __init__(self, *args, **kwargs):
        if len(args) == len(self.SCHEMA.fields):
            for i, name in enumerate(self.SCHEMA.names):
                self.__dict__[name] = args[i]
        elif len(args) > 0:
            raise ValueError('Args must be empty or mirror schema')
        else:
            self.__dict__.update(kwargs)

        # overloading encode() to support both class and instance
        self.encode = self._encode_self

    @classmethod
    def encode(cls, item):
        bits = []
        for i, field in enumerate(cls.SCHEMA.fields):
            bits.append(field.encode(item[i]))
        return b''.join(bits)

    def _encode_self(self):
        return self.SCHEMA.encode(
            [self.__dict__[name] for name in self.SCHEMA.names]
        )

    @classmethod
    def decode(cls, data):
        if isinstance(data, bytes):
            data = BytesIO(data)
        return cls(*[field.decode(data) for field in cls.SCHEMA.fields])

    def __repr__(self):
        key_vals =['%s=%r' % (name, self.__dict__[name])
                   for name in self.SCHEMA.names] 
        return self.__class__.__name__ + '(' + ', '.join(key_vals) + ')'

"""
class MetaStruct(type):
    def __new__(cls, clsname, bases, dct):
        nt = namedtuple(clsname, [name for (name, _) in dct['SCHEMA']])
        bases = tuple([Struct, nt] + list(bases))
        return super(MetaStruct, cls).__new__(cls, clsname, bases, dct)
"""
