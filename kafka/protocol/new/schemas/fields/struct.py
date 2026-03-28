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

    def encode_into__optimized_context(self, version, compact=False, tagged=False):
        ctx = CodegenContext()
        indent = '    '
        ctx.lines.append('def _encode(item, out):')
        ctx.emit(indent, 'buf = out.buf')
        ctx.emit(indent, 'pos = out.pos')
        self.emit_encode_into(ctx, 'item', indent, version=version, compact=compact, tagged=tagged)
        ctx.emit(indent, 'out.pos = pos')
        return ctx

    def compiled_encode_into(self, version, compact=False, tagged=False):
        """Return a compiled flat encode function for this struct+version.

        Lazily compiled on first call and cached. The returned function has
        signature: f(item, out) where out is an EncodeBuffer.
        """
        key = (version, compact, tagged)
        if key not in self._compiled_encoders:
            ctx = self.encode_into__optimized_context(version, compact=compact, tagged=tagged)
            exec(compile(ctx.source(), '<codegen:%s_v%d>' % (self.name, version), 'exec'), ctx.globs)
            self._compiled_encoders[key] = ctx.globs['_encode']
        return self._compiled_encoders[key]

    def emit_decode_from(self, ctx, var_name, indent, version=None, compact=False, tagged=False):
        """Emit decode code that creates a DataContainer via __new__ + direct slot assignment.

        Batches adjacent batchable fields into single unpack_from calls.
        """
        fields = self.untagged_fields(version)
        data_class = self.data_class

        if data_class is not None:
            dc_var = ctx.next_var('dc')
            ctx.globs[dc_var] = data_class
            ctx.emit(indent, '%s = object.__new__(%s)' % (var_name, dc_var))
            ctx.emit(indent, '%s._version = %d' % (var_name, version))
            ctx.emit(indent, '%s.tags = None' % var_name)
            ctx.emit(indent, '%s.unknown_tags = None' % var_name)

        # Walk fields, batching adjacent batchable fields
        i = 0
        while i < len(fields):
            field = fields[i]

            # Try to batch consecutive batchable fields.
            if field.is_batchable():
                # Collect the run
                batch_fields = []
                batch_fmt = '>'
                batch_size = 0
                while i < len(fields):
                    f = fields[i]
                    if f.is_batchable():
                        batch_fields.append(f)
                        batch_fmt += f._type.fmt
                        batch_size += f._type.size
                        i += 1
                    else:
                        break

                if len(batch_fields) == 1:
                    # Single field — no batching benefit
                    f = batch_fields[0]
                    v = ctx.next_var('val')
                    f.emit_decode_from(ctx, v, indent, version=version,
                                       compact=compact, tagged=tagged)
                    if data_class is not None:
                        ctx.emit(indent, '%s.%s = %s' % (var_name, f.name, v))
                else:
                    # Batched unpack
                    var_names = [ctx.next_var('val') for _ in batch_fields]
                    ctx.emit(indent, '%s = unpack_from("%s", data, pos)' % (
                        ', '.join(var_names), batch_fmt))
                    ctx.emit(indent, 'pos += %d' % batch_size)
                    if data_class is not None:
                        for f, v in zip(batch_fields, var_names):
                            ctx.emit(indent, '%s.%s = %s' % (var_name, f.name, v))
            else:
                # Non-batchable field (String, Bytes, Array, Struct, etc.)
                v = ctx.next_var('val')
                field.emit_decode_from(ctx, v, indent, version=version,
                                       compact=compact, tagged=tagged)
                if data_class is not None:
                    ctx.emit(indent, '%s.%s = %s' % (var_name, field.name, v))
                i += 1

        if tagged:
            # Fast path: most messages have zero tagged fields (single 0x00 byte).
            # Only fall back to full TaggedFields.decode when tags are present.
            ctx.emit(indent, 'if data[pos] == 0:')
            ctx.emit(indent, '    pos += 1')
            ctx.emit(indent, 'else:')
            self._emit_tagged_decode(ctx, var_name, indent + '    ', version)
        elif tagged is None:
            # Empty tagged fields: single 0x00 byte
            ctx.emit(indent, 'pos += 1  # empty tagged fields')

    def _emit_tagged_decode(self, ctx, var_name, indent, version):
        """Emit tagged fields decode — falls back to method-based decode."""
        tf_var = ctx.next_var('tf')
        ctx.globs[tf_var] = self.tagged_fields(version)
        ctx.globs['_BytesIO'] = __import__('io').BytesIO
        bio_var = ctx.next_var('bio')
        ctx.emit(indent, '%s = _BytesIO(bytes(data[pos:]))' % bio_var)
        ctx.emit(indent, '_tfd = %s.decode(%s, version=%d)' % (tf_var, bio_var, version))
        ctx.emit(indent, 'pos += %s.tell()' % bio_var)
        ctx.emit(indent, 'if _tfd:')
        ctx.emit(indent, '    for _tk, _tv in _tfd.items():')
        ctx.emit(indent, '        if _tk.startswith("_"):')
        ctx.emit(indent, '            if %s.unknown_tags is None: %s.unknown_tags = {}' % (var_name, var_name))
        ctx.emit(indent, '            %s.unknown_tags[_tk] = _tv' % var_name)
        ctx.emit(indent, '        else:')
        ctx.emit(indent, '            if %s.tags is None: %s.tags = set()' % (var_name, var_name))
        ctx.emit(indent, '            %s.tags.add(_tk)' % var_name)
        ctx.emit(indent, '            setattr(%s, _tk, _tv)' % var_name)

    def compiled_decode_from(self, version, compact=False, tagged=False, data_class=None):
        """Return a compiled flat decode function for this struct+version.

        Lazily compiled on first call and cached. The returned function has
        signature: f(data, pos) -> (obj, pos) where data is a memoryview/bytes.
        data_class overrides self.data_class for the top-level object (needed
        to resolve weakref proxies on ApiMessage classes).
        """
        key = ('decode', version, compact, tagged, data_class)
        if key not in self._compiled_encoders:
            # Temporarily override data_class for codegen if provided
            saved = self._data_class
            if data_class is not None:
                self._data_class = data_class
            try:
                ctx = CodegenContext()
                indent = '    '
                ctx.lines.append('def _decode(data, pos):')
                self.emit_decode_from(ctx, 'obj', indent, version=version,
                                      compact=compact, tagged=tagged)
                ctx.emit(indent, 'return obj, pos')
                code = ctx.source()
                exec(compile(code, '<codegen_decode:%s_v%d>' % (self.name, version), 'exec'), ctx.globs)
                self._compiled_encoders[key] = ctx.globs['_decode']
            finally:
                self._data_class = saved
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
