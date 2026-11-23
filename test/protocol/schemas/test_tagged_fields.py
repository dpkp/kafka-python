import io
import os
import subprocess
import sys
import textwrap

import pytest

from kafka.protocol.schemas.fields import SimpleField, StructField
from kafka.protocol.schemas.fields.codecs import TaggedFields, UnsignedVarInt32


def test_tagged_fields():
    tags = TaggedFields([
        SimpleField({'name': 'foo', 'tag': 0, 'type': 'int16', 'versions': "0+"}),
        SimpleField({'name': 'bar', 'tag': 1, 'type': 'string', 'versions': "0+"}),
    ])
    val = {'foo': 2, 'bar': 'foobar'}
    encoded = tags.encode(val, version=0)
    # num_tags(2), tag(0), size(2), b'\x00\x02', tag(1), size(7), len(6+1), 'foobar'
    expected = (UnsignedVarInt32.encode(2) +
                UnsignedVarInt32.encode(0) + UnsignedVarInt32.encode(2) + b'\x00\x02' +
                UnsignedVarInt32.encode(1) + UnsignedVarInt32.encode(7) + UnsignedVarInt32.encode(7) + b'foobar')
    assert encoded == expected
    decoded = tags.decode(io.BytesIO(encoded), version=0)
    assert decoded == val


def test_tagged_fields_struct():
    tags = TaggedFields([
        StructField({'name': 'foo', 'tag': 0, 'type': 'Bar', 'versions': "0+", "fields": [
            {'name': 'bar', 'tag': 1, 'type': 'string', 'versions': "0+"},
        ]}),
    ])
    val = {'foo': {'bar': 'foobar'}}
    encoded = tags.encode(val, version=0)
    # num_tags(1), tag(0), size(8), len(6+1), 'foobar', empty tags(\x00)
    expected = (UnsignedVarInt32.encode(1) + UnsignedVarInt32.encode(0) + UnsignedVarInt32.encode(8) +
                UnsignedVarInt32.encode(7) + b'foobar' +
                UnsignedVarInt32.encode(0))
    assert encoded == expected
    decoded = tags.decode(io.BytesIO(encoded), version=0)
    assert decoded == val


def test_tagged_fields_decode_empty():
    data = io.BytesIO(UnsignedVarInt32.encode(0) + b'\x01')
    TaggedFields.decode_empty(data)
    assert data.tell() == 1


def test_tagged_fields_decode_empty_not_empty():
    with pytest.raises(ValueError):
        TaggedFields.decode_empty(io.BytesIO(UnsignedVarInt32.encode(1)))


def test_decode_nested_structs_with_python_optimize():
    # decode_empty must not rely on assert, which is stripped by python -O
    script = textwrap.dedent("""
        import io
        from kafka.protocol.metadata.api_versions import ApiVersionsResponse
        R = ApiVersionsResponse[4]
        data = R(
            error_code=0,
            api_keys=[R.ApiVersion(api_key=18, min_version=0, max_version=4)],
            throttle_time_ms=0,
            supported_features=[R.SupportedFeatureKey(name='metadata.version', min_version=1, max_version=21)],
            finalized_features_epoch=1,
            finalized_features=[R.FinalizedFeatureKey(name='metadata.version', max_version_level=21, min_version_level=21)],
        ).encode()
        print(R.decode(io.BytesIO(data)).finalized_features[0].max_version_level)
    """)
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    result = subprocess.run([sys.executable, '-O', '-c', script], cwd=repo_root,
                            capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == '21'
