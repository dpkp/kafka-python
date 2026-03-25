from . import UnsignedVarInt32


def _uvarint_size(value):
    """Return the number of bytes needed to encode value as an unsigned varint."""
    if value < 0x80:
        return 1
    elif value < 0x4000:
        return 2
    elif value < 0x200000:
        return 3
    elif value < 0x10000000:
        return 4
    else:
        return 5


class TaggedFields:
    def __init__(self, fields):
        self._fields = list(fields) # listify to clean filter() inputs
        self._tags = {field.tag: field for field in self._fields}
        self._names = {field.name: field for field in self._fields}

    def encode(self, item, version=None):
        if isinstance(item, dict):
            tags = [(self._names[name].tag, val)
                    for name, val in item.items()
                    if name in self._names
                    and val != self._names[name].default]
        else:
            tags = [(self._names[name].tag, getattr(item, name))
                    for name in self._names
                    if hasattr(item, name)
                    and getattr(item, name) != self._names[name].default]
        ret = [UnsignedVarInt32.encode(len(tags))]
        for tag, val in tags:
            ret.append(UnsignedVarInt32.encode(tag))
            # struct tags have an empty nested tagged fields (tagged=None)
            encoded_val = self._tags[tag].encode(val, version=version, compact=True, tagged=None)
            ret.append(UnsignedVarInt32.encode(len(encoded_val)))
            ret.append(encoded_val)
        return b''.join(ret)

    def encode_into(self, item, out, version=None):
        if isinstance(item, dict):
            tags = [(self._names[name].tag, name)
                    for name, val in item.items()
                    if name in self._names
                    and val != self._names[name].default]
        else:
            tags = [(self._names[name].tag, name)
                    for name in self._names
                    if hasattr(item, name)
                    and getattr(item, name) != self._names[name].default]
        UnsignedVarInt32.encode_into(out, len(tags))
        for tag, name in tags:
            field = self._tags[tag]
            val = item.get(name) if isinstance(item, dict) else getattr(item, name)
            UnsignedVarInt32.encode_into(out, tag)
            # Encode value into buffer after reserving space for size prefix.
            # Strategy: assume size fits in 1 byte (< 128), encode value,
            # then fix up if the size varint is larger.
            size_pos = out.pos
            out.pos += 1  # reserve 1 byte for size
            val_start = out.pos
            # struct tags have an empty nested tagged fields (tagged=None)
            field.encode_into(val, out, version=version, compact=True, tagged=None)
            val_size = out.pos - val_start
            actual_size_len = _uvarint_size(val_size)
            if actual_size_len == 1:
                # Common case: size fits in 1 byte, just write it
                out.buf[size_pos] = val_size
            else:
                # Rare case: size needs more bytes. Shift the value data forward.
                extra = actual_size_len - 1
                out.ensure(extra)
                # Move value bytes forward to make room
                out.buf[val_start + extra:out.pos + extra] = out.buf[val_start:out.pos]
                out.pos += extra
                # Write the multi-byte size at size_pos
                saved_pos = out.pos
                out.pos = size_pos
                UnsignedVarInt32.encode_into(out, val_size)
                out.pos = saved_pos

    def decode(self, data, version=None):
        num_fields = UnsignedVarInt32.decode(data)
        ret = {}
        for i in range(num_fields):
            tag = UnsignedVarInt32.decode(data)
            size = UnsignedVarInt32.decode(data)
            if tag in self._tags:
                field = self._tags[tag]
                # struct tags have an empty nested tagged fields (tagged=None)
                ret[field.name] = field.decode(data, version=version, compact=True, tagged=None)
            else:
                ret['_%d' % tag] = data.read(size)
        return ret

    @classmethod
    def decode_empty(cls, data):
        assert UnsignedVarInt32.decode(data) == 0

    @classmethod
    def encode_empty(cls):
        return UnsignedVarInt32.encode(0)

    def __repr__(self):
        return 'TaggedFields(%s)' % list(self._names.keys())
