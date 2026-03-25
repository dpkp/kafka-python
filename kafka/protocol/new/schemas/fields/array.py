from .base import BaseField
from .simple import SimpleField
from .codecs import (
    UnsignedVarInt32, Int32,
)


class ArrayField(BaseField):
    @classmethod
    def parse_inner_type(cls, json):
        if 'fields' in json:
            return
        type_str = cls.parse_array_type(json)
        if type_str is not None:
            inner_json = {**json, 'type': type_str}
            return SimpleField.parse_json(inner_json)

    @classmethod
    def parse_array_type(cls, json):
        if json['type'].startswith('[]'):
            type_str = json['type'][2:]
            assert not type_str.startswith('[]'), 'Unexpected double-array type: %s' % json['type']
            return type_str

    @classmethod
    def parse_json(cls, json):
        inner_type = cls.parse_inner_type(json)
        if inner_type is not None:
            return cls(json, array_of=inner_type)

    def __init__(self, json, array_of=None):
        if array_of is None:
            array_of = self.parse_inner_type(json)
            assert array_of is not None, 'json does not contain a (simple) Array!'
        super().__init__(json)
        self.array_of = array_of # SimpleField

    def is_array(self):
        return True

    def _calculate_default(self, default):
        if default == 'null':
            return None
        elif not default:
            return []
        else:
            raise ValueError('Invalid default for field %s. The only valid default is empty or null.' % self._name)

    def encode(self, items, version=None, compact=False, tagged=False):
        if compact:
            size = UnsignedVarInt32.encode(len(items) + 1 if items is not None else 0)
        else:
            size = Int32.encode(len(items) if items is not None else -1)
        if items is None:
            return size
        fields = [self.array_of.encode(item, version=version, compact=compact, tagged=tagged)
                  for item in items]
        return b''.join([size] + fields)

    def encode_into(self, items, out, version=None, compact=False, tagged=False):
        if compact:
            UnsignedVarInt32.encode_into(out, len(items) + 1 if items is not None else 0)
        else:
            Int32.encode_into(out, len(items) if items is not None else -1)
        if items is None:
            return
        encode_into = self.array_of.encode_into
        for item in items:
            encode_into(item, out, version=version, compact=compact, tagged=tagged)

    def emit_encode_into(self, ctx, val_expr, indent, version=None, compact=False, tagged=False):
        if compact:
            an = ctx.next_var('an')
            ctx.emit(indent, '%s = len(%s) + 1 if %s is not None else 0' % (an, val_expr, val_expr))
            UnsignedVarInt32.emit_encode_into(ctx, an, indent)
        else:
            ctx.emit(indent, 'if %s is None:' % val_expr)
            ctx.emit(indent, "    pack_into('>i', buf, pos, -1)")
            ctx.emit(indent, '    pos += 4')
            ctx.emit(indent, 'else:')
            ctx.emit(indent, "    pack_into('>i', buf, pos, len(%s))" % val_expr)
            ctx.emit(indent, '    pos += 4')
        guard = indent + '    ' if not compact else indent
        item_var = ctx.next_var('ai')
        ctx.emit(guard, 'for %s in %s:' % (item_var, val_expr))
        self.array_of.emit_encode_into(ctx, item_var, guard + '    ',
                                        version=version, compact=compact, tagged=tagged)

    def decode(self, data, version=None, compact=False, tagged=False):
        if compact:
            size = UnsignedVarInt32.decode(data)
            size -= 1
        else:
            size = Int32.decode(data)
        if size == -1:
            return None
        return [self.array_of.decode(data, version=version, compact=compact, tagged=tagged)
                for _ in range(size)]

    def __repr__(self):
        return 'ArrayField(%s)' % self._json
