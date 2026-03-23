from . import UnsignedVarInt32


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
