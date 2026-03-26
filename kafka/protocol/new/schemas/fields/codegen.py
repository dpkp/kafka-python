"""Generate flat encode/decode functions for a StructField + version.

Given a StructField and a protocol version, generates Python functions
that encode/decode directly with zero dispatch overhead — no intermediate
SimpleField/ArrayField/StructField method calls.

Usage:
    from kafka.protocol.new.schemas.fields.codegen import CodegenContext
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

    def source(self):
        return '\n'.join(self.lines)
