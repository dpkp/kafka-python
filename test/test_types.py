import pytest

from kafka.protocol.types import Bytes, String


@pytest.mark.parametrize("type_obj", [String(), Bytes()])
def test_none_value_encode(type_obj):
    with pytest.raises(ValueError):
        type_obj.encode(None)
