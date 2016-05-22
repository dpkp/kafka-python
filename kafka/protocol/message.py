import io

from ..codec import (has_gzip, has_snappy, has_lz4,
                     gzip_decode, snappy_decode, lz4_decode)
from . import pickle
from .struct import Struct
from .types import (
    Int8, Int32, Int64, Bytes, Schema, AbstractType
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
    CODEC_MASK = 0x03
    CODEC_GZIP = 0x01
    CODEC_SNAPPY = 0x02
    CODEC_LZ4 = 0x03
    HEADER_SIZE = 14 # crc(4), magic(1), attributes(1), key+value size(4*2)

    def __init__(self, value, key=None, magic=0, attributes=0, crc=0):
        assert value is None or isinstance(value, bytes), 'value must be bytes'
        assert key is None or isinstance(key, bytes), 'key must be bytes'
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

    @classmethod
    def decode(cls, data):
        if isinstance(data, bytes):
            data = io.BytesIO(data)
        fields = [field.decode(data) for field in cls.SCHEMA.fields]
        return cls(fields[4], key=fields[3],
                   magic=fields[1], attributes=fields[2], crc=fields[0])

    def validate_crc(self):
        raw_msg = self._encode_self(recalc_crc=False)
        crc = crc32(raw_msg[4:])
        if crc == self.crc:
            return True
        return False

    def is_compressed(self):
        return self.attributes & self.CODEC_MASK != 0

    def decompress(self):
        codec = self.attributes & self.CODEC_MASK
        assert codec in (self.CODEC_GZIP, self.CODEC_SNAPPY, self.CODEC_LZ4)
        if codec == self.CODEC_GZIP:
            assert has_gzip(), 'Gzip decompression unsupported'
            raw_bytes = gzip_decode(self.value)
        elif codec == self.CODEC_SNAPPY:
            assert has_snappy(), 'Snappy decompression unsupported'
            raw_bytes = snappy_decode(self.value)
        elif codec == self.CODEC_LZ4:
            assert has_lz4(), 'LZ4 decompression unsupported'
            raw_bytes = lz4_decode(self.value)
        else:
          raise Exception('This should be impossible')

        return MessageSet.decode(raw_bytes, bytes_to_read=len(raw_bytes))

    def __hash__(self):
        return hash(self._encode_self(recalc_crc=False))


class PartialMessage(bytes):
    def __repr__(self):
        return 'PartialMessage(%s)' % self


class MessageSet(AbstractType):
    ITEM = Schema(
        ('offset', Int64),
        ('message', Bytes)
    )
    HEADER_SIZE = 12 # offset + message_size

    @classmethod
    def encode(cls, items, size=True, recalc_message_size=True):
        # RecordAccumulator encodes messagesets internally
        if isinstance(items, io.BytesIO):
            size = Int32.decode(items)
            # rewind and return all the bytes
            items.seek(-4, 1)
            return items.read(size + 4)

        encoded_values = []
        for (offset, message) in items:
            encoded_values.append(Int64.encode(offset))
            encoded_values.append(Bytes.encode(message))
        encoded = b''.join(encoded_values)
        if not size:
            return encoded
        return Bytes.encode(encoded)

    @classmethod
    def decode(cls, data, bytes_to_read=None):
        """Compressed messages should pass in bytes_to_read (via message size)
        otherwise, we decode from data as Int32
        """
        if isinstance(data, bytes):
            data = io.BytesIO(data)
        if bytes_to_read is None:
            bytes_to_read = Int32.decode(data)
        items = []

        # if FetchRequest max_bytes is smaller than the available message set
        # the server returns partial data for the final message
        while bytes_to_read:
            try:
                offset = Int64.decode(data)
                msg_bytes = Bytes.decode(data)
                bytes_to_read -= 8 + 4 + len(msg_bytes)
                items.append((offset, len(msg_bytes), Message.decode(msg_bytes)))
            except ValueError:
                # PartialMessage to signal that max_bytes may be too small
                items.append((None, None, PartialMessage()))
                break
        return items

    @classmethod
    def repr(cls, messages):
        if isinstance(messages, io.BytesIO):
            offset = messages.tell()
            decoded = cls.decode(messages)
            messages.seek(offset)
            messages = decoded
        return str([cls.ITEM.repr(m) for m in messages])
