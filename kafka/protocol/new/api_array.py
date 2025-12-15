from .api_struct import ApiStruct
from ..types import (
    Bytes, CompactBytes,
    String, CompactString,
    Array, CompactArray,
    UnsignedVarInt32, Int32,
)


class ApiArray:
    def __init__(self, array_of):
        self.array_of = array_of # ApiStruct or AbstractType (not Field)

    def inner_type(self, compact=False):
        if self.array_of in (Bytes, CompactBytes):
            return CompactBytes if compact else Bytes
        elif isinstance(self.array_of, (String, CompactString)):
            encoding = self.array_of.encoding
            return CompactString(encoding) if compact else String(encoding)
        else:
            return self.array_of

    def to_schema(self, version, compact=False, tagged=False):
        arr_type = CompactArray if compact else Array
        if isinstance(self.array_of, ApiStruct):
            inner_type = self.array_of.to_schema(version, compact=compact, tagged=tagged)
        else:
            inner_type = self.inner_type()
        return arr_type(inner_type)

    def encode(self, items, version=None, compact=False, tagged=False):
        if compact:
            size = UnsignedVarInt32.encode(len(items) + 1 if items is not None else 0)
        else:
            size = Int32.encode(len(items) if items is not None else -1)
        if isinstance(self.array_of, ApiStruct):
            fields = [self.array_of.encode(item, version=version, compact=compact, tagged=tagged)
                      for item in items]
        else:
            inner_type = self.inner_type(compact=compact)
            fields = [inner_type.encode(item) for item in items]
        return b''.join([size] + fields)

    def decode(self, data, version=None, compact=False, tagged=False):
        if compact:
            size = UnsignedVarInt32.decode(data)
            size -= 1
        else:
            size = Int32.decode(data)
        if size == -1:
            return None
        elif isinstance(self.array_of, ApiStruct):
            return [self.array_of.decode(data, version=version, compact=compact, tagged=tagged)
                    for _ in range(size)]
        else:
            inner_type = self.inner_type(compact=compact)
            return [inner_type.decode(data) for _ in range(size)]
