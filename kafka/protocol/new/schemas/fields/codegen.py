"""Generate flat encode_into functions for a StructField + version.

Given a StructField and a protocol version, generates a Python function
that encodes directly into an EncodeBuffer with zero dispatch overhead —
no intermediate SimpleField/ArrayField/StructField method calls.

Usage:
    from kafka.protocol.new.schemas.fields.codegen import compile_encode_into
    fast_encode = compile_encode_into(some_struct_field, version=0, compact=False, tagged=False)
    fast_encode(item, out)
"""

from struct import pack_into


class CodegenContext:
    """Shared state for code generation."""

    def __init__(self):
        self.lines = []
        self.globs = {'pack_into': pack_into}
        self._var_counter = 0

    def next_var(self, prefix='v'):
        self._var_counter += 1
        return f'_{prefix}{self._var_counter}'

    def emit(self, indent, line):
        self.lines.append(f'{indent}{line}')

    def source(self):
        return '\n'.join(self.lines)
