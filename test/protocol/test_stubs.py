"""Tests for the .pyi stub generator."""
import os
import importlib
import inspect

import pytest

from kafka.protocol.api_data import ApiData
from kafka.protocol.api_message import ApiMessage
from kafka.protocol.data_container import DataContainer
from kafka.protocol.generate_stubs import (
    MESSAGE_MODULES,
    discover_modules,
    generate_module,
    resolve_type,
    emit_class,
)


class TestResolveType:
    def test_simple_int(self):
        from kafka.protocol.consumer.group import HeartbeatRequest
        field = HeartbeatRequest._struct._field_map['generation_id']
        assert resolve_type(field) == 'int'

    def test_simple_string(self):
        from kafka.protocol.consumer.group import HeartbeatRequest
        field = HeartbeatRequest._struct._field_map['group_id']
        assert resolve_type(field) == 'str'

    def test_nullable_string(self):
        from kafka.protocol.consumer.group import HeartbeatRequest
        field = HeartbeatRequest._struct._field_map['group_instance_id']
        assert resolve_type(field) == 'str | None'

    def test_struct_array(self):
        from kafka.protocol.consumer.fetch import FetchRequest
        field = FetchRequest._struct._field_map['topics']
        assert resolve_type(field) == 'list[FetchTopic]'

    def test_simple_array(self):
        from kafka.protocol.consumer.metadata import ConsumerProtocolSubscription
        field = ConsumerProtocolSubscription._struct._field_map['topics']
        assert resolve_type(field) == 'list[str]'

    def test_bytes_field(self):
        from kafka.protocol.consumer.metadata import ConsumerProtocolSubscription
        field = ConsumerProtocolSubscription._struct._field_map['user_data']
        assert resolve_type(field) == 'bytes | ApiData | None'


class TestEmitClass:
    def test_heartbeat_request_fields(self):
        from kafka.protocol.consumer.group import HeartbeatRequest
        lines = emit_class(HeartbeatRequest)
        text = '\n'.join(lines)
        assert 'class HeartbeatRequest(ApiMessage):' in text
        assert 'group_id: str' in text
        assert 'generation_id: int' in text
        assert 'group_instance_id: str | None' in text
        assert 'member_id: str' in text

    def test_fetch_request_nested_classes(self):
        from kafka.protocol.consumer.fetch import FetchRequest
        lines = emit_class(FetchRequest)
        text = '\n'.join(lines)
        assert 'class FetchTopic(DataContainer):' in text
        assert 'class FetchPartition(DataContainer):' in text
        assert 'topics: list[FetchTopic]' in text

    def test_api_data_methods(self):
        from kafka.protocol.consumer.metadata import ConsumerProtocolSubscription
        lines = emit_class(ConsumerProtocolSubscription)
        text = '\n'.join(lines)
        assert 'class ConsumerProtocolSubscription(ApiData):' in text
        assert 'name: str' in text
        assert 'valid_versions: tuple[int, int]' in text

    def test_api_message_methods(self):
        from kafka.protocol.consumer.group import HeartbeatRequest
        lines = emit_class(HeartbeatRequest)
        text = '\n'.join(lines)
        assert 'API_KEY: int' in text
        assert 'def with_header(' in text
        assert 'def is_request(' in text

    def test_init_signature(self):
        from kafka.protocol.consumer.group import HeartbeatRequest
        lines = emit_class(HeartbeatRequest)
        text = '\n'.join(lines)
        assert 'def __init__(' in text
        assert 'group_id: str = ...,' in text
        assert 'version: int | None = None,' in text
        assert '**kwargs: Any,' in text


class TestGenerateModule:
    def test_generates_imports(self):
        from kafka.protocol.consumer import group
        mod = group
        exports = [(name, getattr(mod, name)) for name in mod.__all__]
        content = generate_module(mod, exports)
        assert 'from typing import Any, Self' in content
        assert 'from kafka.protocol.api_message import ApiMessage' in content

    def test_constants_typed(self):
        from kafka.protocol.consumer import group
        mod = group
        exports = [(name, getattr(mod, name)) for name in mod.__all__]
        content = generate_module(mod, exports)
        assert 'DEFAULT_GENERATION_ID: int' in content
        assert 'UNKNOWN_MEMBER_ID: str' in content


class TestCompleteness:
    def test_all_modules_discoverable(self):
        modules = discover_modules()
        assert len(modules) == len(MESSAGE_MODULES)

    def test_all_protocol_classes_have_stubs(self):
        """Every ApiMessage/ApiData class in __all__ should appear in generated stubs."""
        modules = discover_modules()
        for mod_file, (mod, exports) in modules.items():
            content = generate_module(mod, exports)
            for name, obj in exports:
                if isinstance(obj, type) and issubclass(obj, DataContainer) and obj._struct is not None:
                    assert ('class %s(' % name) in content, \
                        '%s missing from stub for %s' % (name, mod_file)

    def test_stubs_up_to_date(self):
        """Generated stubs should match what is on disk."""
        import sys
        modules = discover_modules()
        for mod_file, (mod, exports) in modules.items():
            pyi_path = mod_file.replace('.py', '.pyi')
            if not os.path.exists(pyi_path):
                pytest.skip('Stubs not generated yet — run: python -m kafka.protocol.generate_stubs')
            with open(pyi_path, 'r') as f:
                existing = f.read()
            # Stubs are version-dependent; skip freshness check if generated on a different Python
            header = '# Generated by generate_stubs.py (Python %d.%d)' % sys.version_info[:2]
            if not existing.startswith(header):
                pytest.skip('Stubs generated on different Python version')
            content = generate_module(mod, exports)
            assert existing == content, 'Stub out of date: %s' % pyi_path
