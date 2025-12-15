import pytest

from kafka.protocol.new.field import Field, parse_versions
from kafka.protocol.types import Int16, Int32, Boolean, String, UUID


def test_parse_versions():
    assert parse_versions("0+") == (0, 32767)
    assert parse_versions("1-3") == (1, 3)
    assert parse_versions("5") == (5, 5)
    assert parse_versions(None) is None
    assert parse_versions("none") is None
    assert parse_versions("") is None


def test_field_int_defaults():
    # Test literal int default
    f1 = Field({"name": "f1", "type": "int32", "default": -1})
    assert f1.default == -1

    # Test string int default
    f2 = Field({"name": "f2", "type": "int32", "default": "-1"})
    assert f2.default == -1

    # Test hex string default
    f3 = Field({"name": "f3", "type": "int32", "default": "0x7fffffff"})
    assert f3.default == 2147483647

    # Test empty string default (should be 0 for int)
    f4 = Field({"name": "f4", "type": "int32", "default": ""})
    assert f4.default == 0


def test_field_bool_defaults():
    # Test literal bool default
    f1 = Field({"name": "f1", "type": "bool", "default": True})
    assert f1.default is True

    # Test string bool default
    f2 = Field({"name": "f2", "type": "bool", "default": "true"})
    assert f2.default is True
    f3 = Field({"name": "f3", "type": "bool", "default": "False"})
    assert f3.default is False

    # Test empty string default (should be False for bool)
    f4 = Field({"name": "f4", "type": "bool", "default": ""})
    assert f4.default is False


def test_field_string_defaults():
    f1 = Field({"name": "f1", "type": "string", "default": "foo"})
    assert f1.default == "foo"

    f2 = Field({"name": "f2", "type": "string", "default": ""})
    assert f2.default == ""

    # Nullable string
    f3 = Field({
        "name": "f3",
        "type": "string",
        "default": "null",
        "nullableVersions": "0+",
        "versions": "0+"
    })
    assert f3.default is None


def test_field_invalid_null_default():
    # null default on non-nullable field should raise ValueError
    with pytest.raises(ValueError, match="not all versions of this field are nullable"):
        f = Field({"name": "f1", "type": "string", "default": "null", "versions": "0+"})
        _ = f.default


def test_field_types():
    assert Field({"name": "f", "type": "int16"}).type is Int16
    assert Field({"name": "f", "type": "int32"}).type is Int32
    assert isinstance(Field({"name": "f", "type": "string"}).type, String)
    assert Field({"name": "f", "type": "bool"}).type is Boolean
    assert Field({"name": "f", "type": "uuid"}).type is UUID


def test_field_version_check():
    f = Field({"name": "f", "type": "int32", "versions": "1-3"})
    assert not f.for_version_q(0)
    assert f.for_version_q(1)
    assert f.for_version_q(2)
    assert f.for_version_q(3)
    assert not f.for_version_q(4)
