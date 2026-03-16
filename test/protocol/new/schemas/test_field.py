import pytest

from kafka.protocol.new.schemas.fields.array import ArrayField
from kafka.protocol.new.schemas.fields.struct import StructField
from kafka.protocol.new.schemas.fields.base import BaseField
from kafka.protocol.new.schemas.fields.simple import SimpleField
from kafka.protocol.types import Int16, Int32, Boolean, String, UUID


def test_parse_versions():
    assert BaseField.parse_versions("0+") == (0, 32767)
    assert BaseField.parse_versions("1-3") == (1, 3)
    assert BaseField.parse_versions("5") == (5, 5)
    assert BaseField.parse_versions("none") == (-1, -1)
    assert BaseField.parse_versions(None) is None
    assert BaseField.parse_versions("") is None


def test_field_int_defaults():
    # Test literal int default
    f1 = BaseField.parse_json({"name": "f1", "versions": "0+", "type": "int32", "default": -1})
    assert f1.default == -1

    # Test string int default
    f2 = BaseField.parse_json({"name": "f2", "versions": "0+", "type": "int32", "default": "-1"})
    assert f2.default == -1

    # Test hex string default
    f3 = BaseField.parse_json({"name": "f3", "versions": "0+", "type": "int32", "default": "0x7fffffff"})
    assert f3.default == 2147483647

    # Test empty string default (should be 0 for int)
    f4 = BaseField.parse_json({"name": "f4", "versions": "0+", "type": "int32", "default": ""})
    assert f4.default == 0


def test_field_bool_defaults():
    # Test literal bool default
    f1 = BaseField.parse_json({"name": "f1", "versions": "0+", "type": "bool", "default": True})
    assert f1.default is True

    # Test string bool default
    f2 = BaseField.parse_json({"name": "f2", "versions": "0+", "type": "bool", "default": "true"})
    assert f2.default is True
    f3 = BaseField.parse_json({"name": "f3", "versions": "0+", "type": "bool", "default": "False"})
    assert f3.default is False

    # Test empty string default (should be False for bool)
    f4 = BaseField.parse_json({"name": "f4", "versions": "0+", "type": "bool", "default": ""})
    assert f4.default is False


def test_field_string_defaults():
    f1 = BaseField.parse_json({"name": "f1", "versions": "0+", "type": "string", "default": "foo"})
    assert f1.default == "foo"

    f2 = BaseField.parse_json({"name": "f2", "versions": "0+", "type": "string", "default": ""})
    assert f2.default == ""

    # Nullable string
    f3 = BaseField.parse_json({
        "name": "f3",
        "type": "string",
        "default": "null",
        "nullableVersions": "0+",
        "versions": "0+"
    })
    assert f3.default is None


@pytest.mark.parametrize(('field_json', 'inner_type'), [
    ({"name": "f", "versions": "0+", "type": "int16"}, Int16),
    ({"name": "f", "versions": "0+", "type": "int32"}, Int32),
    ({"name": "f", "versions": "0+", "type": "string"}, String),
    ({"name": "f", "versions": "0+", "type": "bool"}, Boolean),
    ({"name": "f", "versions": "0+", "type": "uuid"}, UUID),
])
def test_field_basic_types(field_json, inner_type):
    field = BaseField.parse_json(field_json)
    assert isinstance(field, SimpleField)
    assert field._type is inner_type or isinstance(field._type, inner_type)


def test_struct_field():
    field = BaseField.parse_json({"name": "f", "versions": "0+", "type": "Foo", "fields": [{"name": "b", "versions": "0+", "type": "int16"}]})
    assert field.is_struct()
    assert isinstance(field, StructField)
    assert isinstance(field.fields['b'], SimpleField)
    assert field.fields['b']._type is Int16


def test_array_field():
    field = BaseField.parse_json({"name": "f", "versions": "0+", "type": "[]int16"})
    assert field.is_array()
    assert not field.is_struct_array()
    assert isinstance(field, ArrayField)
    assert isinstance(field.array_of, SimpleField)
    assert field.array_of._type is Int16


def test_struct_array_field():
    field = BaseField.parse_json({"name": "f", "versions": "0+", "type": "[]Foo", "fields": [{"name": "b", "versions": "0+", "type": "int16"}]})
    assert field.is_array()
    assert field.is_struct_array()
    assert isinstance(field, ArrayField)
    assert isinstance(field.array_of, StructField)
    assert field.array_of.fields['b']._type is Int16


def test_field_version_check():
    f = BaseField.parse_json({"name": "f", "type": "int32", "versions": "1-3"})
    assert not f.for_version_q(0)
    assert f.for_version_q(1)
    assert f.for_version_q(2)
    assert f.for_version_q(3)
    assert not f.for_version_q(4)
