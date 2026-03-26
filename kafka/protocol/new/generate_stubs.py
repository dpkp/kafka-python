"""Generate .pyi type stub files for dynamically created protocol classes.

Usage:
    python -m kafka.protocol.new.generate_stubs           # generate stubs
    python -m kafka.protocol.new.generate_stubs --check    # exit 1 if stubs are out of date
    python -m kafka.protocol.new.generate_stubs --dry-run  # print to stdout
"""
import importlib
import inspect
import os
import sys

from kafka.protocol.new.api_data import ApiData
from kafka.protocol.new.api_message import ApiMessage
from kafka.protocol.new.data_container import DataContainer
from kafka.protocol.new.schemas.fields.simple import SimpleField
from kafka.protocol.new.schemas.fields.array import ArrayField
from kafka.protocol.new.schemas.fields.struct import StructField
from kafka.protocol.new.schemas.fields.struct_array import StructArrayField


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

# Modules containing protocol classes, relative to kafka.protocol.new
MESSAGE_MODULES = [
    'kafka.protocol.new.consumer.fetch',
    'kafka.protocol.new.consumer.group',
    'kafka.protocol.new.consumer.metadata',
    'kafka.protocol.new.consumer.offsets',
    'kafka.protocol.new.metadata.api_versions',
    'kafka.protocol.new.metadata.find_coordinator',
    'kafka.protocol.new.metadata.metadata',
    'kafka.protocol.new.producer.produce',
    'kafka.protocol.new.producer.transaction',
    'kafka.protocol.new.admin.acl',
    'kafka.protocol.new.admin.client_quotas',
    'kafka.protocol.new.admin.cluster',
    'kafka.protocol.new.admin.groups',
    'kafka.protocol.new.admin.topics',
    'kafka.protocol.new.sasl',
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


def _emit_custom_members(lines, pad, cls, base_name):
    """Emit stubs for custom members defined directly on the class."""
    # Skip internal/metaclass attributes
    skip = {
        '_json', '_struct', '_class_version', '_VERSIONS', '_flexible_versions',
        '_valid_versions', '__module__', '__qualname__', '__doc__', '__dict__',
        '__weakref__', '__license__', 'json_patch', '__init_subclass__',
    }
    # Get members unique to this class (not inherited from base)
    if base_name == 'ApiMessage':
        base = ApiMessage
    elif base_name == 'ApiData':
        base = ApiData
    else:
        base = DataContainer

    for name, obj in cls.__dict__.items():
        if name.startswith('_') or name in skip:
            continue
        # Skip if it's on the base class too
        if hasattr(base, name) and getattr(base, name) is obj:
            continue
        # classproperty or property
        if hasattr(obj, 'fget') or (hasattr(obj, '__func__') and callable(obj.__func__)):
            # Try to infer return type from source or just use Any
            lines.append('%s@property' % pad)
            lines.append('%sdef %s(self) -> Any: ...' % (pad, name))
        elif isinstance(obj, classmethod):
            lines.append('%s@classmethod' % pad)
            lines.append('%sdef %s(cls, *args: Any, **kwargs: Any) -> Any: ...' % (pad, name))
        elif isinstance(obj, property):
            lines.append('%s@property' % pad)
            lines.append('%sdef %s(self) -> Any: ...' % (pad, name))


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
    lines.append('import uuid')
    lines.append('from typing import Any, Self')
    lines.append('')

    # Determine needed imports based on base classes
    needs_api_message = False
    needs_api_data = False
    needs_data_container = False
    for name, obj in exports:
        if isinstance(obj, type):
            if issubclass(obj, ApiMessage):
                needs_api_message = True
            elif issubclass(obj, ApiData):
                needs_api_data = True

    # Check fields for nested classes (DataContainer) and bytes fields (ApiData)
    for name, obj in exports:
        if isinstance(obj, type) and hasattr(obj, '_struct') and obj._struct is not None:
            for field in _all_fields_recursive(obj):
                if field.is_struct() or field.is_struct_array():
                    needs_data_container = True
                if isinstance(field, SimpleField) and field._type_str in ('bytes', 'records'):
                    needs_api_data = True

    if needs_api_message:
        lines.append('from kafka.protocol.new.api_message import ApiMessage')
    if needs_api_data:
        lines.append('from kafka.protocol.new.api_data import ApiData')
    if needs_data_container:
        lines.append('from kafka.protocol.new.data_container import DataContainer')
    if needs_api_message or needs_api_data or needs_data_container:
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
            # These are already statically typed in source — just re-export
            lines.append('# Defined in source — see .py file')
            lines.append('class %s: ...' % name)
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
