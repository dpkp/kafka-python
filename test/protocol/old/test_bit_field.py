import io

import pytest

from kafka.protocol.old.types import BitField


@pytest.mark.parametrize(('test_set',), [
    (set([0, 1, 5, 10]),),
    (set(range(15)),),
    (None,),
])
def test_bit_field(test_set):
    assert BitField.decode(io.BytesIO(BitField.encode(test_set))) == test_set


def test_bit_field_null():
    assert BitField.from_32_bit_field(-2147483648) == {31}
    assert BitField.decode(io.BytesIO(BitField.encode({31}))) is None
