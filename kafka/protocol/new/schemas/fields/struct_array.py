from .array import ArrayField
from .struct import StructField


class StructArrayField(ArrayField):
    @classmethod
    def parse_inner_type(cls, json):
        # StructArrayField requires non-empty fields
        if 'fields' not in json:
            return
        assert len(json['fields']) > 0, 'Unexpected empty fields in json'
        type_str = cls.parse_array_type(json)
        if type_str is None:
            return
        inner_json = {**json, 'type': type_str}
        return StructField.parse_json(inner_json)

    @classmethod
    def parse_json(cls, json):
        inner_type = cls.parse_inner_type(json)
        if inner_type is not None:
            return cls(json, array_of=inner_type)

    def __init__(self, json, array_of=None):
        if array_of is None:
            array_of = self.parse_inner_type(json)
            assert array_of is not None, 'json does not contain a StructArray!'
        super().__init__(json, array_of=array_of)
        # map_key will be (idx, field) of the mapKey field if found
        self.map_key = next(filter(lambda x: x[1]._json.get('mapKey'), enumerate(self._fields)), None)

    @property
    def type_str(self):
        return self._type_str[2:]

    def is_struct_array(self):
        return True

    @property
    def fields(self):
        return self.array_of.fields

    def tagged_fields(self, version):
        return self.array_of.tagged_fields(version)

    def untagged_fields(self, version):
        return self.array_of.untagged_fields(version)

    def has_data_class(self):
        return self.array_of.has_data_class()

    def set_data_class(self, data_class):
        return self.array_of.set_data_class(data_class)

    @property
    def data_class(self):
        return self.array_of.data_class

    def __call__(self, *args, **kw):
        return self.data_class(*args, **kw) # pylint: disable=E1102

    def emit_encode_into(self, ctx, val_expr, indent, version=None, compact=False, tagged=False):
        from .codecs import UnsignedVarInt32
        inner_struct = self.array_of
        fields = inner_struct.untagged_fields(version)
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
        item_var = ctx.next_var('si')
        if len(fields) == 1:
            # Single-field struct: items may be scalars (str, int, etc.)
            ctx.emit(guard, 'for %s in %s:' % (item_var, val_expr))
            scalar_indent = guard + '    '
            ctx.emit(scalar_indent, 'if isinstance(%s, tuple): %s = %s[0]' % (item_var, item_var, item_var))
            ctx.emit(scalar_indent, 'elif hasattr(%s, "%s"): %s = %s.%s' % (
                item_var, fields[0].name, item_var, item_var, fields[0].name))
            fields[0].emit_encode_into(ctx, item_var, scalar_indent,
                                        version=version, compact=compact, tagged=tagged)
            if tagged:
                tf_var = ctx.next_var('tf')
                ctx.globs[tf_var] = inner_struct.tagged_fields(version)
                ctx.emit(scalar_indent, 'out.pos = pos')
                ctx.emit(scalar_indent, '# tagged fields for single-field struct')
                ctx.emit(scalar_indent, '%s.encode_into(%s, out, version=%d)' % (tf_var, item_var, version))
                ctx.emit(scalar_indent, 'pos = out.pos')
            elif tagged is None:
                ctx.emit(scalar_indent, 'buf[pos] = 0')
                ctx.emit(scalar_indent, 'pos += 1')
        else:
            # Multi-field struct: emit both tuple and attribute access paths
            ctx.emit(guard, 'if %s and isinstance(%s[0], tuple):' % (val_expr, val_expr))
            ctx.emit(guard, '    for %s in %s:' % (item_var, val_expr))
            inner_struct.emit_encode_into(ctx, item_var, guard + '        ',
                                           version=version, compact=compact, tagged=tagged,
                                           tuple_access=True)
            ctx.emit(guard, 'else:')
            ctx.emit(guard, '    for %s in %s:' % (item_var, val_expr))
            inner_struct.emit_encode_into(ctx, item_var, guard + '        ',
                                           version=version, compact=compact, tagged=tagged,
                                           tuple_access=False)

    def __repr__(self):
        return 'StructArrayField(%s)' % self._json
