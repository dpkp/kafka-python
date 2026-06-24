"""Generate flat encode/decode functions for a StructField + version.

Given a StructField and a protocol version, generates Python functions
that encode/decode directly with zero dispatch overhead - no intermediate
SimpleField/ArrayField/StructField method calls.

Usage:
    from kafka.protocol.schemas.fields.codegen import CodegenContext
    # Encode: see StructField.compiled_encode_into()
    # Decode: see StructField.compiled_decode_from()
"""

from struct import pack_into, unpack_from


class CodegenContext:
    """Shared state for code generation."""

    def __init__(self):
        self.lines = []
        self.globs = {'pack_into': pack_into, 'unpack_from': unpack_from}
        self._var_counter = 0

    def next_var(self, prefix='v'):
        self._var_counter += 1
        return f'_{prefix}{self._var_counter}'

    def emit(self, indent, line):
        self.lines.append(f'{indent}{line}')

    def emit_reserve(self, indent, nbytes):
        """Emit an inline capacity check before a write of up to ``nbytes`` bytes.

        Generated encode functions keep three locals in sync:

          * ``buf``  - the destination bytearray (``out.buf``),
          * ``pos``  - the current write offset,
          * ``_cap`` - ``len(buf)``, the cached capacity.

        These are not set up per fragment: they are declared once by the
        ``def _encode(item, out):`` preamble in
        ``StructField.encode_into__optimized_context`` (the sole generator of
        the compiled encode function), and every emitted fragment - including
        this one - is spliced into that function body. Emit fragments are
        therefore only valid inside that body; they cannot stand alone.

        ``nbytes`` is the MAXIMUM number of bytes the following write can
        consume (an ``int`` for fixed/varint fields, or a string expression
        such as ``'len(_bv1)'`` for a variable payload). The fast path is a
        single comparison; only on overflow do we sync ``out.pos``, grow via
        :meth:`EncodeBuffer.ensure`, and re-read the (possibly reallocated)
        buffer back into ``buf``/``_cap``.

        Because :meth:`EncodeBuffer.ensure` may rebind ``out.buf``, code that
        instead delegates to a runtime ``encode_into`` (e.g. tagged fields)
        must re-bind ``buf``/``_cap`` itself afterwards - ``emit_reserve`` only
        covers writes emitted inline.
        """
        self.emit(indent, 'if pos + %s > _cap:' % nbytes)
        self.emit(indent, '    out.pos = pos; out.ensure(%s); buf = out.buf; _cap = len(buf)' % nbytes)

    def source(self):
        return '\n'.join(self.lines)

    def print(self):
        print('GLOBALS:')
        for var in self.globs:
            print(f'     {var}={self.globs[var]}')
        print('\nSOURCE:')
        for i, line in enumerate(self.lines):
            print(f'{i+1:<4} {line}')
