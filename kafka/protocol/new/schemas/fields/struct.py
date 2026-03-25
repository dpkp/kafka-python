from .base import BaseField
from .codecs.tagged_fields import TaggedFields
from .codecs.types import UnsignedVarInt32
from .codegen import CodegenContext


class StructField(BaseField):
    @classmethod
    def parse_json(cls, json):
        if 'type' not in json or json['type'].startswith('[]'):
            return
        if 'fields' in json:
            return cls(json)

    # Cases
    # oldschool  - standard types, no tagged fields
    # newschool  - compact types, tagged fields
    # nested tag - compact types, no (nested) tagged fields
    def __init__(self, json):
        super().__init__(json)
        self._field_map = {field.name: field for field in self._fields}
        self._data_class = None
        self._untagged_fields_cache = {}
        self._tagged_fields_cache = {}
        self._compiled_encoders = {}

    @property
    def fields(self):
        return self._field_map

    def is_struct(self):
        return True

    def has_data_class(self):
        return self._data_class is not None

    def set_data_class(self, data_class):
        assert self._data_class is None
        self._data_class = data_class

    @property
    def data_class(self):
        return self._data_class

    def __call__(self, *args, **kw):
        return self.data_class(*args, **kw) # pylint: disable=E1102

    def _calculate_default(self, default):
        if default == 'null':
            return None
        if self._tag is not None:
            return None
        elif not default:
            raise NotImplementedError(f"Default value not implemented for struct field '{self._name}'")
        else:
            raise ValueError('Invalid default for struct field %s. The only valid default is null.' % self._name)

    def tagged_fields(self, version):
        if version not in self._tagged_fields_cache:
            self._tagged_fields_cache[version] = TaggedFields(
                [field for field in self._fields
                 if field.for_version_q(version)
                 and field.tagged_field_q(version)])
        return self._tagged_fields_cache[version]

    def untagged_fields(self, version):
        if version not in self._untagged_fields_cache:
            self._untagged_fields_cache[version] = [
                field for field in self._fields
                if field.for_version_q(version)
                and not field.tagged_field_q(version)]
        return self._untagged_fields_cache[version]

    def encode(self, item, version=None, compact=False, tagged=False):
        fields = self.untagged_fields(version)
        if isinstance(item, tuple):
            getter = lambda item, i, field: item[i]
            tags = {} if len(item) == len(fields) else item[-1]
        elif isinstance(item, dict):
            getter = lambda item, i, field: item.get(field.name) # defaults?
            tags = item
        elif isinstance(item, (str, int, float)):
            assert len(fields) == 1, "Encoding single value item (str/int/float) requires single field struct"
            getter = lambda item, i, field: item
            tags = {}
        else:
            getter = lambda item, i, field: getattr(item, field.name)
            tags = item
        encoded = [field.encode(getter(item, i, field),
                                version=version, compact=compact, tagged=tagged)
                   for i, field in enumerate(fields)]
        if tagged:
            # TaggedFields are always compact and never include nested tagged fields
            encoded.append(self.tagged_fields(version).encode(tags, version=version))
        elif tagged is None:
            encoded.append(TaggedFields.encode_empty())
        return b''.join(encoded)

    def emit_encode_into(self, ctx, item_expr, indent, version=None, compact=False,
                          tagged=False, tuple_access=False):
        fields = self.untagged_fields(version)
        for i, field in enumerate(fields):
            if tuple_access:
                val_expr = '%s[%d]' % (item_expr, i)
            else:
                val_expr = '%s.%s' % (item_expr, field.name)
            field.emit_encode_into(ctx, val_expr, indent, version=version,
                                   compact=compact, tagged=tagged)
        if tagged:
            tf_var = ctx.next_var('tf')
            ctx.globs[tf_var] = self.tagged_fields(version)
            ctx.emit(indent, 'out.pos = pos')
            ctx.emit(indent, '%s.encode_into(%s, out, version=%d)' % (tf_var, item_expr, version))
            ctx.emit(indent, 'pos = out.pos')
        elif tagged is None:
            ctx.emit(indent, 'buf[pos] = 0')
            ctx.emit(indent, 'pos += 1')

    def encode_into(self, item, out, version=None, compact=False, tagged=False):
        fields = self.untagged_fields(version)
        if isinstance(item, tuple):
            for i, field in enumerate(fields):
                field.encode_into(item[i], out, version=version, compact=compact, tagged=tagged)
        elif isinstance(item, dict):
            for field in fields:
                field.encode_into(item.get(field.name), out, version=version, compact=compact, tagged=tagged)
        elif isinstance(item, (str, int, float)):
            fields[0].encode_into(item, out, version=version, compact=compact, tagged=tagged)
        else:
            for field in fields:
                field.encode_into(getattr(item, field.name), out, version=version, compact=compact, tagged=tagged)
        if tagged:
            self.tagged_fields(version).encode_into(item, out, version=version)
        elif tagged is None:
            UnsignedVarInt32.encode_into(out, 0)

    def compiled_encode_into(self, version, compact=False, tagged=False):
        """Return a compiled flat encode function for this struct+version.

        Lazily compiled on first call and cached. The returned function has
        signature: f(item, out) where out is an EncodeBuffer.
        """
        key = (version, compact, tagged)
        if key not in self._compiled_encoders:
            ctx = CodegenContext()
            indent = '    '
            ctx.lines.append('def _encode(item, out):')
            ctx.emit(indent, 'buf = out.buf')
            ctx.emit(indent, 'pos = out.pos')
            self.emit_encode_into(ctx, 'item', indent, version=version,
                                  compact=compact, tagged=tagged)
            ctx.emit(indent, 'out.pos = pos')
            code = ctx.source()
            exec(compile(code, '<codegen:%s_v%d>' % (self.name, version), 'exec'), ctx.globs)
            self._compiled_encoders[key] = ctx.globs['_encode']
        return self._compiled_encoders[key]

    def decode(self, data, version=None, compact=False, tagged=False, data_class=None):
        if data_class is None:
            data_class = self.data_class
        decoded = {
            field.name: field.decode(data, version=version, compact=compact, tagged=tagged)
            for field in self.untagged_fields(version)
        }
        if tagged:
            decoded.update(self.tagged_fields(version).decode(data, version=version))
        elif tagged is None:
            TaggedFields.decode_empty(data)

        if data_class is not None:
            return data_class(version=version, **decoded)
        return decoded

    def __len__(self):
        return len(self._fields)

    def __eq__(self, other):
        if not super().__eq__(other):
            return False
        if self._fields != other._fields:
            return False
        return True

    def __repr__(self):
        return 'StructField(%s)' % self._json
