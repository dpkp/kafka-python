from .struct import Struct
from .types import (
    Int8, Int16, Int32, Int64, Bytes, String, Array, Schema, AbstractType
)
from ..util import crc32


class Message(Struct):
    SCHEMA = Schema(
        ('crc', Int32),
        ('magic', Int8),
        ('attributes', Int8),
        ('key', Bytes),
        ('value', Bytes)
    )

    def __init__(self, value, key=None, magic=0, attributes=0, crc=0):
        self.crc = crc
        self.magic = magic
        self.attributes = attributes
        self.key = key
        self.value = value
        self.encode = self._encode_self

    def _encode_self(self, recalc_crc=True):
        message = Message.SCHEMA.encode(
          (self.crc, self.magic, self.attributes, self.key, self.value)
        )
        if not recalc_crc:
            return message
        self.crc = crc32(message[4:])
        return self.SCHEMA.fields[0].encode(self.crc) + message[4:]


class MessageSet(AbstractType):
    ITEM = Schema(
        ('offset', Int64),
        ('message_size', Int32),
        ('message', Message.SCHEMA)
    )

    @classmethod
    def encode(cls, items, size=True, recalc_message_size=True):
        encoded_values = []
        for (offset, message_size, message) in items:
            if isinstance(message, Message):
                encoded_message = message.encode()
            else:
                encoded_message = cls.ITEM.fields[2].encode(message)
            if recalc_message_size:
                message_size = len(encoded_message)
            encoded_values.append(cls.ITEM.fields[0].encode(offset))
            encoded_values.append(cls.ITEM.fields[1].encode(message_size))
            encoded_values.append(encoded_message)
        encoded = b''.join(encoded_values)
        if not size:
            return encoded
        return Int32.encode(len(encoded)) + encoded

    @classmethod
    def decode(cls, data):
        size = Int32.decode(data)
        bytes_read = 0
        items = []
        while bytes_read < size:
            items.append(cls.ITEM.decode(data))
            msg_size = items[-1][1]
            bytes_read += (8 + 4 + msg_size)
        return items
