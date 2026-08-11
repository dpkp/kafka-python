# pylint: skip-file

import pytest

from kafka.serializer import DefaultSerializer, JsonSerializer


@pytest.mark.parametrize('encoding', ['utf-8', 'utf-16'])
def test_default_serializer_roundtrip(encoding):
    ser = DefaultSerializer(encoding)
    data = 'h\u00e9llo w\u00f6rld'
    encoded = ser.serialize('topic', [], data)
    assert isinstance(encoded, bytes)
    assert ser.deserialize('topic', [], encoded) == data


@pytest.mark.parametrize('data', [b'raw', bytearray(b'raw'), memoryview(b'raw'), None])
def test_default_serializer_passthrough_bytes_like(data):
    assert DefaultSerializer().serialize('topic', [], data) == data


def test_default_serializer_deserialize_none():
    assert DefaultSerializer().deserialize('topic', [], None) is None


def test_default_serializer_rejects_non_bytes_str():
    with pytest.raises(AttributeError):
        DefaultSerializer().serialize('topic', [], 42)


def test_json_serializer_roundtrip():
    ser = JsonSerializer()
    data = {'key': 'value', 'n': 42, 'lst': [1, 2]}
    encoded = ser.serialize('topic', [], data)
    assert isinstance(encoded, bytes)
    assert ser.deserialize('topic', [], encoded) == data


def test_json_serializer_none():
    ser = JsonSerializer()
    assert ser.serialize('topic', [], None) is None
    assert ser.deserialize('topic', [], None) is None


def test_json_serializer_deserializes_plain_json():
    ser = JsonSerializer()
    assert ser.deserialize('topic', [], b'{"a": 1}') == {'a': 1}
