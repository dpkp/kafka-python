import io

import pytest

from kafka.protocol.types import BitField


@pytest.mark.parametrize(('test_set',), [
    (set([0, 1, 5, 10, 31]),),
    (set(range(32)),),
])
def test_bit_field(test_set):
    assert BitField.decode(io.BytesIO(BitField.encode(test_set))) == test_set
