"""Generate .pyi type stub files for dynamically created protocol classes.

Usage:
    python -m kafka.protocol.generate_stubs           # generate stubs
    python -m kafka.protocol.generate_stubs --check    # exit 1 if stubs are out of date
    python -m kafka.protocol.generate_stubs --dry-run  # print to stdout
"""
import importlib
import inspect
import os
import sys

from .api_data import ApiData
from .api_message import ApiMessage
from .data_container import DataContainer
from .schemas.fields.simple import SimpleField
from .schemas.fields.array import ArrayField
from .schemas.fields.struct import StructField
from .schemas.fields.struct_array import StructArrayField
from kafka.util import classproperty


# JSON schema type string -> Python type annotation
SIMPLE_TYPE_MAP = {
    'int8': 'int',
    'int16': 'int',
    'int32': 'int',
    'int64': 'int',
    'float64': 'float',
    'bool': 'bool',
    'string': 'str',
    'bytes': 'bytes',
    'records': 'bytes',
    'uuid': 'uuid.UUID',
    'bitfield': 'set[int]',
}

# Modules containing protocol classes, relative to kafka.protocol
MESSAGE_MODULES = [
    'kafka.protocol.admin.acl',
    'kafka.protocol.admin.client_quotas',
    'kafka.protocol.admin.cluster',
    'kafka.protocol.admin.configs',
    'kafka.protocol.admin.groups',
    'kafka.protocol.admin.topics',
    'kafka.protocol.admin.users',
    'kafka.protocol.consumer.fetch',
    'kafka.protocol.consumer.group',
    'kafka.protocol.consumer.metadata',
    'kafka.protocol.consumer.offsets',
    'kafka.protocol.metadata.api_versions',
    'kafka.protocol.metadata.find_coordinator',
    'kafka.protocol.metadata.metadata',
    'kafka.protocol.producer.produce',
    'kafka.protocol.producer.transaction',
    'kafka.protocol.sasl',
]


def _all_fields_recursive(cls):
    """Yield all fields from cls and its nested structs."""
    if cls._struct is None:
        return
    for field in cls._struct._fields:
        yield field
        if field.is_struct() or field.is_struct_array():
            dc = getattr(cls, field.type_str, None)
            if dc is not None:
                yield from _all_fields_recursive(dc)


def resolve_type(field):
    """Convert a field object to a Python type annotation string."""
    if isinstance(field, StructArrayField):
        base = 'list[%s]' % field.type_str
    elif isinstance(field, ArrayField):
        # Simple array (e.g., []int32, []string)
        inner = SIMPLE_TYPE_MAP.get(field.array_of._type_str, 'Any')
        base = 'list[%s]' % inner
    elif isinstance(field, StructField):
        base = field.type_str
    elif isinstance(field, SimpleField):
        base = SIMPLE_TYPE_MAP.get(field._type_str, 'Any')
        # bytes fields accept ApiData objects (encoded automatically via .encode())
        if field._type_str in ('bytes', 'records'):
            base = 'bytes | ApiData'
    else:
        base = 'Any'

    if field._nullable_versions is not None:
        return '%s | None' % base
    return base


def _collect_nested_classes(cls):
    """Yield (field, data_class) pairs for nested struct types on cls."""
    if cls._struct is None:
        return
    seen = set()
    for field in cls._struct._fields:
        if field.is_struct() or field.is_struct_array():
            dc = getattr(cls, field.type_str, None)
            if dc is not None and dc not in seen:
                seen.add(dc)
                yield field, dc


def emit_class(cls, indent=0, base_name=None):
    """Generate stub lines for a single protocol class."""
    pad = ' ' * indent
    lines = []

    # Determine base class name for the stub
    if base_name is None:
        if issubclass(cls, ApiMessage):
            base_name = 'ApiMessage'
        elif issubclass(cls, ApiData):
            base_name = 'ApiData'
        else:
            base_name = 'DataContainer'

    lines.append('%sclass %s(%s):' % (pad, cls.__name__, base_name))

    inner_pad = pad + '    '

    # Nested inner classes first
    for field, dc in _collect_nested_classes(cls):
        lines.extend(emit_class(dc, indent=indent + 4, base_name='DataContainer'))
        lines.append('')

    # Field annotations
    if cls._struct is not None:
        for field in cls._struct._fields:
            type_ann = resolve_type(field)
            lines.append('%s%s: %s' % (inner_pad, field.name, type_ann))

    # __init__ signature
    init_params = ['self', '*args: Any']
    if cls._struct is not None:
        for field in cls._struct._fields:
            type_ann = resolve_type(field)
            init_params.append('%s: %s = ...' % (field.name, type_ann))
    init_params.append('version: int | None = None')
    init_params.append('**kwargs: Any')

    if len(init_params) <= 4:
        lines.append('%sdef __init__(%s) -> None: ...' % (inner_pad, ', '.join(init_params)))
    else:
        lines.append('%sdef __init__(' % inner_pad)
        for i, param in enumerate(init_params):
            comma = ',' if i < len(init_params) - 1 else ','
            lines.append('%s    %s%s' % (inner_pad, param, comma))
        lines.append('%s) -> None: ...' % inner_pad)

    # Common DataContainer methods/properties
    lines.append('%s@property' % inner_pad)
    lines.append('%sdef version(self) -> int | None: ...' % inner_pad)
    lines.append('%sdef to_dict(self, meta: bool = False, json: bool = True) -> dict: ...' % inner_pad)

    if base_name == 'DataContainer':
        pass  # encode/decode inherited from DataContainer
    elif base_name == 'ApiData':
        _emit_api_data_methods(lines, inner_pad, cls)
    elif base_name == 'ApiMessage':
        _emit_api_message_methods(lines, inner_pad, cls)

    # Detect custom members defined on the class itself (not inherited)
    _emit_custom_members(lines, inner_pad, cls, base_name)

    return lines


def _emit_api_data_methods(lines, pad, cls):
    """Emit method stubs for ApiData subclasses."""
    lines.append('%sname: str' % pad)
    lines.append('%stype: str' % pad)
    lines.append('%svalid_versions: tuple[int, int]' % pad)
    lines.append('%smin_version: int' % pad)
    lines.append('%smax_version: int' % pad)


def _emit_api_message_methods(lines, pad, cls):
    """Emit method stubs for ApiMessage subclasses."""
    lines.append('%sname: str' % pad)
    lines.append('%stype: str' % pad)
    lines.append('%sAPI_KEY: int' % pad)
    lines.append('%sAPI_VERSION: int' % pad)
    lines.append('%svalid_versions: tuple[int, int]' % pad)
    lines.append('%smin_version: int' % pad)
    lines.append('%smax_version: int' % pad)
    lines.append('%s@property' % pad)
    lines.append('%sdef header(self) -> Any: ...' % pad)
    lines.append('%s@classmethod' % pad)
    lines.append('%sdef is_request(cls) -> bool: ...' % pad)
    lines.append('%sdef expect_response(self) -> bool: ...' % pad)
    lines.append('%sdef with_header(self, correlation_id: int = 0, client_id: str = "kafka-python") -> None: ...' % pad)


def _format_annotation(ann):
    """Format a type annotation as a clean string, using short class names."""
    if isinstance(ann, str):
        return ann
    if isinstance(ann, type):
        return ann.__name__
    # For generic aliases like list[TopicPartition], use repr and clean up module paths
    s = str(ann)
    # Remove module prefixes (e.g., kafka.structs.TopicPartition -> TopicPartition)
    import re
    s = re.sub(r'[a-zA-Z_][a-zA-Z0-9_.]*\.([A-Z][a-zA-Z0-9_]*)', r'\1', s)
    return s


def _get_return_annotation(obj):
    """Extract the return type annotation string from a callable, property, or classproperty."""
    func = None
    if isinstance(obj, classproperty):
        func = obj.f
    elif isinstance(obj, property):
        func = obj.fget
    elif isinstance(obj, classmethod):
        func = obj.__func__
    elif isinstance(obj, staticmethod):
        func = obj.__func__
    elif callable(obj):
        func = obj

    if func is not None:
        hints = getattr(func, '__annotations__', {})
        ret = hints.get('return')
        if ret is not None:
            return _format_annotation(ret)
    return None


def _get_param_annotations(obj):
    """Extract parameter annotations from a callable, returning (params_str, has_annotations)."""
    func = None
    if isinstance(obj, classmethod):
        func = obj.__func__
    elif isinstance(obj, staticmethod):
        func = obj.__func__
    elif callable(obj):
        func = obj

    if func is None:
        return None

    try:
        sig = inspect.signature(func)
    except (ValueError, TypeError):
        return None

    parts = []
    for pname, param in sig.parameters.items():
        if pname == 'self' or pname == 'cls':
            parts.append(pname)
            continue
        ann = param.annotation
        if ann is inspect.Parameter.empty:
            ann_str = 'Any'
        else:
            ann_str = _format_annotation(ann)
        if param.default is not inspect.Parameter.empty:
            parts.append('%s: %s = ...' % (pname, ann_str))
        elif param.kind == inspect.Parameter.VAR_POSITIONAL:
            parts.append('*%s: %s' % (pname, ann_str))
        elif param.kind == inspect.Parameter.VAR_KEYWORD:
            parts.append('**%s: %s' % (pname, ann_str))
        else:
            parts.append('%s: %s' % (pname, ann_str))
    return ', '.join(parts)


def _emit_custom_members(lines, pad, cls, base_name):
    """Emit stubs for custom members defined directly on the class.

    Reads type annotations from the source when available.
    """
    skip = {
        '_json', '_struct', '_class_version', '_VERSIONS', '_flexible_versions',
        '_valid_versions', '__module__', '__qualname__', '__doc__', '__dict__',
        '__weakref__', '__license__', 'json_patch', '__init_subclass__',
    }
    if base_name == 'ApiMessage':
        base = ApiMessage
    elif base_name == 'ApiData':
        base = ApiData
    else:
        base = DataContainer

    for name, obj in cls.__dict__.items():
        if name.startswith('_') or name in skip:
            continue
        # Skip encode/decode overrides to avoid LSP violations in stubs
        if name in ('encode', 'decode', 'encode_into') and hasattr(base, name):
            continue
        # Skip slot descriptors and nested classes (already handled)
        if isinstance(obj, type):
            continue
        if type(obj).__name__ == 'member_descriptor':
            continue

        ret = _get_return_annotation(obj) or 'Any'

        if isinstance(obj, classproperty):
            lines.append('%s@property' % pad)
            lines.append('%sdef %s(self) -> %s: ...' % (pad, name, ret))
        elif isinstance(obj, property):
            lines.append('%s@property' % pad)
            lines.append('%sdef %s(self) -> %s: ...' % (pad, name, ret))
        elif isinstance(obj, classmethod):
            params = _get_param_annotations(obj) or 'cls, *args: Any, **kwargs: Any'
            lines.append('%s@classmethod' % pad)
            lines.append('%sdef %s(%s) -> %s: ...' % (pad, name, params, ret))
        elif isinstance(obj, staticmethod):
            params = _get_param_annotations(obj) or '*args: Any, **kwargs: Any'
            lines.append('%s@staticmethod' % pad)
            lines.append('%sdef %s(%s) -> %s: ...' % (pad, name, params, ret))
        elif callable(obj) and not isinstance(obj, type):
            params = _get_param_annotations(obj) or 'self, *args: Any, **kwargs: Any'
            lines.append('%sdef %s(%s) -> %s: ...' % (pad, name, params, ret))


def discover_modules():
    """Import all message modules and collect their exports.

    Returns dict mapping module file path to list of (name, obj) pairs.
    """
    result = {}
    for mod_name in MESSAGE_MODULES:
        mod = importlib.import_module(mod_name)
        mod_file = inspect.getfile(mod)
        all_names = getattr(mod, '__all__', [])
        exports = []
        for name in all_names:
            obj = getattr(mod, name)
            exports.append((name, obj))
        result[mod_file] = (mod, exports)
    return result


def generate_module(mod, exports):
    """Generate the complete .pyi file content for a module."""
    lines = []
    lines.append('# Generated by generate_stubs.py (Python %d.%d)' % sys.version_info[:2])
    lines.append('import uuid')
    lines.append('from typing import Any, Self')
    lines.append('')

    # Determine needed imports based on base classes
    from enum import IntEnum as _IntEnum
    needs_api_message = False
    needs_api_data = False
    needs_data_container = False
    needs_int_enum = False
    for name, obj in exports:
        if isinstance(obj, type):
            if issubclass(obj, ApiMessage):
                needs_api_message = True
            elif issubclass(obj, ApiData):
                needs_api_data = True
            if issubclass(obj, _IntEnum):
                needs_int_enum = True

    # Check fields for nested classes (DataContainer) and bytes fields (ApiData)
    for name, obj in exports:
        if isinstance(obj, type) and hasattr(obj, '_struct') and obj._struct is not None:
            for field in _all_fields_recursive(obj):
                if field.is_struct() or field.is_struct_array():
                    needs_data_container = True
                if isinstance(field, SimpleField) and field._type_str in ('bytes', 'records'):
                    needs_api_data = True

    if needs_int_enum:
        lines.append('from enum import IntEnum')
    if needs_api_message:
        lines.append('from kafka.protocol.api_message import ApiMessage')
    if needs_api_data:
        lines.append('from kafka.protocol.api_data import ApiData')
    if needs_data_container:
        lines.append('from kafka.protocol.data_container import DataContainer')
    if needs_api_message or needs_api_data or needs_data_container or needs_int_enum:
        lines.append('')

    # Export __all__ so __init__.py can import it
    all_names = [name for name, obj in exports]
    lines.append('__all__ = [%s]' % ', '.join("'%s'" % n for n in all_names))
    lines.append('')

    for name, obj in exports:
        if isinstance(obj, type) and issubclass(obj, DataContainer) and obj._struct is not None:
            lines.extend(emit_class(obj))
            lines.append('')
        elif isinstance(obj, type):
            # Non-protocol class (IntEnum, OffsetResetStrategy, etc.)
            # Reproduce class with its attributes so the stub doesn't shadow the real definition
            from enum import IntEnum as _IntEnum
            bases = [b.__name__ for b in obj.__bases__ if b is not object]
            base_str = '(%s)' % ', '.join(bases) if bases else ''
            lines.append('class %s%s:' % (name, base_str))
            members = {k: v for k, v in obj.__dict__.items()
                       if not k.startswith('_') and not callable(v)}
            if members:
                for k, v in members.items():
                    # IntEnum members have the enum class as type; use int instead
                    if issubclass(obj, _IntEnum):
                        lines.append('    %s: int' % k)
                    else:
                        lines.append('    %s: %s' % (k, type(v).__name__))
            else:
                lines.append('    ...')
            lines.append('')
        else:
            # Constants
            type_name = type(obj).__name__
            lines.append('%s: %s' % (name, type_name))
            lines.append('')

    return '\n'.join(lines)


def generate_all(dry_run=False, check=False):
    """Generate all stub files. Returns True if all stubs are up to date."""
    modules = discover_modules()
    all_up_to_date = True

    for mod_file, (mod, exports) in sorted(modules.items()):
        content = generate_module(mod, exports)
        pyi_path = mod_file.replace('.py', '.pyi')

        if dry_run:
            print('# %s' % pyi_path)
            print(content)
            print()
        elif check:
            if os.path.exists(pyi_path):
                with open(pyi_path, 'r') as f:
                    existing = f.read()
                if existing != content:
                    print('OUTDATED: %s' % pyi_path)
                    all_up_to_date = False
            else:
                print('MISSING: %s' % pyi_path)
                all_up_to_date = False
        else:
            with open(pyi_path, 'w') as f:
                f.write(content)
            print('Generated: %s' % pyi_path)

    return all_up_to_date


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Generate .pyi stubs for protocol classes')
    parser.add_argument('--check', action='store_true',
                        help='Check if stubs are up to date (exit 1 if not)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Print to stdout instead of writing files')
    args = parser.parse_args()

    if args.check:
        up_to_date = generate_all(check=True)
        sys.exit(0 if up_to_date else 1)
    else:
        generate_all(dry_run=args.dry_run)
