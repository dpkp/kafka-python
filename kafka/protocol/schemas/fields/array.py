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
            UnsignedVarInt32.emit_encode_into(ctx, an, indent)  # reserves the length varint
            ctx.emit(indent, 'if %s is not None:' % val_expr)
        else:
            ctx.emit_reserve(indent, 4)
            ctx.emit(indent, 'if %s is None:' % val_expr)
            ctx.emit(indent, "    pack_into('>i', buf, pos, -1)")
            ctx.emit(indent, '    pos += 4')
            ctx.emit(indent, 'else:')
            ctx.emit(indent, "    pack_into('>i', buf, pos, len(%s))" % val_expr)
            ctx.emit(indent, '    pos += 4')
        guard = indent + '    '
        item_var = ctx.next_var('ai')
        elem = self.array_of
        if isinstance(elem, SimpleField) and elem.is_batchable():
            # Fast path: array of fixed-size primitives (e.g. []int32). Reserve
            # the whole run with a single ensure, then write each element inline
            # without a per-element capacity check. The reserve sits inside the
            # `is not None` branch (compact) / `else` branch (non-compact), so
            # len() is always safe here.
            be_fmt = elem._type._be_fmt
            size = elem._type.size
            ctx.emit_reserve(guard, '%d * len(%s)' % (size, val_expr))
            ctx.emit(guard, 'for %s in %s:' % (item_var, val_expr))
            ctx.emit(guard, "    pack_into('%s', buf, pos, %s)" % (be_fmt, item_var))
            ctx.emit(guard, '    pos += %d' % size)
        else:
            ctx.emit(guard, 'for %s in %s:' % (item_var, val_expr))
            elem.emit_encode_into(ctx, item_var, guard + '    ',
                                  version=version, compact=compact, tagged=tagged)

    def emit_decode_from(self, ctx, var_name, indent, version=None, compact=False, tagged=False):
        n = ctx.next_var('n')
        if compact:
            UnsignedVarInt32.emit_decode_from(ctx, n, indent)
            ctx.emit(indent, '%s -= 1' % n)
        else:
            ctx.emit(indent, '%s = unpack_from(">i", data, pos)[0]' % n)
            ctx.emit(indent, 'pos += 4')
        ctx.emit(indent, 'if %s == -1:' % n)
        ctx.emit(indent, '    %s = None' % var_name)
        ctx.emit(indent, 'else:')
        inner_indent = indent + '    '
        ctx.emit(inner_indent, '%s = []' % var_name)
        idx = ctx.next_var('idx')
        item = ctx.next_var('item')
        ctx.emit(inner_indent, 'for %s in range(%s):' % (idx, n))
        self.array_of.emit_decode_from(ctx, item, inner_indent + '    ',
                                        version=version, compact=compact, tagged=tagged)
        ctx.emit(inner_indent, '    %s.append(%s)' % (var_name, item))

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

    def to_json(self, val):
        if val is None:
            return None
        return [self.array_of.to_json(i) for i in val]

    def __repr__(self):
        return 'ArrayField(%s)' % self._json
