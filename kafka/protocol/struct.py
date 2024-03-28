from io import BytesIO
from typing import List, Union

from kafka.protocol.abstract import AbstractType
from kafka.protocol.types import Schema


from kafka.util import WeakMethod


class Struct(AbstractType):
    SCHEMA = Schema()

    def __init__(self, *args, **kwargs) -> None:
        if len(args) == len(self.SCHEMA.fields):
            for i, name in enumerate(self.SCHEMA.names):
                self.__dict__[name] = args[i]
        elif len(args) > 0:
            raise ValueError('Args must be empty or mirror schema')
        else:
            for name in self.SCHEMA.names:
                self.__dict__[name] = kwargs.pop(name, None)
            if kwargs:
                raise ValueError('Keyword(s) not in schema %s: %s'
                                 % (list(self.SCHEMA.names),
                                    ', '.join(kwargs.keys())))

        # overloading encode() to support both class and instance
        # Without WeakMethod() this creates circular ref, which
        # causes instances to "leak" to garbage
        self.encode = WeakMethod(self._encode_self)


    @classmethod
    def encode(cls, item):  # pylint: disable=E0202
        bits = []
        for i, field in enumerate(cls.SCHEMA.fields):
            bits.append(field.encode(item[i]))
        return b''.join(bits)

    def _encode_self(self) -> bytes:
        return self.SCHEMA.encode(
            [self.__dict__[name] for name in self.SCHEMA.names]
        )

    @classmethod
    def decode(cls, data: Union[BytesIO, bytes]) -> "Struct":
        if isinstance(data, bytes):
            data = BytesIO(data)
        return cls(*[field.decode(data) for field in cls.SCHEMA.fields])

    def get_item(self, name: str) -> Union[int, List[List[Union[int, str, bool, List[List[Union[int, List[int]]]]]]], str, List[List[Union[int, str]]]]:
        if name not in self.SCHEMA.names:
            raise KeyError("%s is not in the schema" % name)
        return self.__dict__[name]

    def __repr__(self) -> str:
        key_vals = []
        for name, field in zip(self.SCHEMA.names, self.SCHEMA.fields):
            key_vals.append(f'{name}={field.repr(self.__dict__[name])}')
        return self.__class__.__name__ + '(' + ', '.join(key_vals) + ')'

    def __hash__(self):
        return hash(self.encode())

    def __eq__(self, other: "Struct") -> bool:
        if self.SCHEMA != other.SCHEMA:
            return False
        for attr in self.SCHEMA.names:
            if self.__dict__[attr] != other.__dict__[attr]:
                return False
        return True
