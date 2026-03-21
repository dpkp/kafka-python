from io import BytesIO
import json
import os

import pytest

from kafka.protocol.api import RequestHeaderV2
from kafka.protocol.api_versions import (
    ApiVersionsRequest, ApiVersionsResponse,
)
from kafka.protocol.new.metadata import (
    ApiVersionsRequest as NewApiVersionsRequest,
    ApiVersionsResponse as NewApiVersionsResponse
)
from kafka.protocol.types import Int16


# --- Golden Samples (Generated from existing working system) ---

# ApiVersionsRequest_v3 (with a client_id for the header)
# client_software_name = "kafka-python"
# client_software_version = "2.9.0"
# tags = {}
# Full request: RequestHeaderV2 + Request body
# RequestHeaderV2: api_key=18, api_version=3, correlation_id=1, client_id='test-client', tags={}
#   api_key (Int16) = 18 (0x0012)
#   api_version (Int16) = 3 (0x0003)
#   correlation_id (Int32) = 1 (0x00000001)
#   client_id (String) = len(11) + b'test-client' -> 0x000B + b'test-client'
#   tags (TaggedFields) = 0x00 (empty)
# Request Body:
#   client_software_name (CompactString) = len(12) + b'kafka-python' -> 0x0D + b'kafka-python'
#   client_software_version (CompactString) = len(5) + b'2.9.0' -> 0x06 + b'2.9.0'
#   tags (TaggedFields) = 0x00 (empty)
GOLDEN_API_VERSIONS_REQUEST_V3_BYTES = \
    b'\x00\x12' \
    b'\x00\x03' \
    b'\x00\x00\x00\x01' \
    b'\x00\x0btest-client' \
    b'\x00' \
    b'\x0dkafka-python' \
    b'\x062.9.0' \
    b'\x00'

# ApiVersionsResponse_v3 (with no header for simplicity in this golden sample)
# error_code = 0
# api_keys = [
#   {'api_key': 0, 'min_version': 0, 'max_version': 9, 'tags': {}}, # Produce
#   {'api_key': 1, 'min_version': 0, 'max_version': 10, 'tags': {}}, # Fetch
# ]
# throttle_time_ms = 0
# tags = {}
# Response Body:
#   error_code (Int16) = 0 (0x0000)
#   api_keys (CompactArray):
#     len(2) -> 0x03 (VarInt)
#     Item 1:
#       api_key (Int16) = 0 (0x0000)
#       min_version (Int16) = 0 (0x0000)
#       max_version (Int16) = 9 (0x0009)
#       tags (TaggedFields) = 0x00 (empty)
#     Item 2:
#       api_key (Int16) = 1 (0x0001)
#       min_version (Int16) = 0 (0x0000)
#       max_version (Int16) = 10 (0x000A)
#       tags (TaggedFields) = 0x00 (empty)
#   throttle_time_ms (Int32) = 0 (0x00000000)
#   tags (TaggedFields) = 0x00 (empty)
GOLDEN_API_VERSIONS_RESPONSE_V3_BODY_BYTES = \
    b'\x00\x00' \
    b'\x03' \
        b'\x00\x00\x00\x00\x00\x09\x00' \
        b'\x00\x01\x00\x00\x00\x0a\x00' \
    b'\x00\x00\x00\x00' \
    b'\x00'


def test_old_system_encode_decode_request():
    # Old system encoding
    request = ApiVersionsRequest[3](
        client_software_name="kafka-python",
        client_software_version="2.9.0",
    )
    request.with_header(correlation_id=1, client_id='test-client')
    full_encoded_request = request.encode(header=True)

    assert full_encoded_request == GOLDEN_API_VERSIONS_REQUEST_V3_BYTES

    # Old system decoding
    data = BytesIO(full_encoded_request)
    decoded_header = ApiVersionsRequest[3].parse_header(data)
    decoded_request = ApiVersionsRequest[3].decode(data)

    assert decoded_header.api_key == request.API_KEY
    assert decoded_header.api_version == request.API_VERSION
    assert decoded_header.correlation_id == 1
    assert decoded_header.client_id == 'test-client'
    assert decoded_header.tags == {}

    assert decoded_request.client_software_name == "kafka-python"
    assert decoded_request.client_software_version == "2.9.0"
    assert decoded_request.tags == {} # empty dict by default from Struct


def test_new_system_encode_decode_request():
    # New system encoding
    request = NewApiVersionsRequest[3](
        client_software_name="kafka-python",
        client_software_version="2.9.0",
    )
    request.with_header(correlation_id=1, client_id='test-client')
    full_encoded_request = request.encode(header=True)

    assert full_encoded_request == GOLDEN_API_VERSIONS_REQUEST_V3_BYTES

    # New system decoding
    data = BytesIO(full_encoded_request)
    decoded_header = NewApiVersionsRequest[3].parse_header(data)

    assert decoded_header.request_api_key == request.API_KEY
    assert decoded_header.request_api_version == request.API_VERSION
    assert decoded_header.correlation_id == 1
    assert decoded_header.client_id == 'test-client'
    assert decoded_header.tags == None

    decoded_request = NewApiVersionsRequest[3].decode(data)
    assert decoded_request.client_software_name == "kafka-python"
    assert decoded_request.client_software_version == "2.9.0"
    assert decoded_request.tags == None # new protocol is set('tag') or None if no tags


def test_old_system_encode_decode_response():
    # Old system encoding
    response = ApiVersionsResponse[3](
        error_code=0,
        api_keys=[
            #{'api_key': 0, 'min_version': 0, 'max_version': 9},
            (0, 0, 9),
            #{'api_key': 1, 'min_version': 0, 'max_version': 10}
            (1, 0, 10)
        ],
        throttle_time_ms=0,
    )
    encoded_body = response.encode()
    assert encoded_body == GOLDEN_API_VERSIONS_RESPONSE_V3_BODY_BYTES

    # Old system decoding
    decoded_response = ApiVersionsResponse[3].decode(BytesIO(encoded_body))

    assert decoded_response.error_code == 0
    assert len(decoded_response.api_keys) == 2
    assert decoded_response.api_keys[0][0] == 0 # api_key
    assert decoded_response.api_keys[0][1] == 0 # min_version
    assert decoded_response.api_keys[0][2] == 9 # max_version
    #assert decoded_response.api_keys[0].tags == {}
    assert decoded_response.api_keys[1][0] == 1 # api_key
    assert decoded_response.api_keys[1][1] == 0 # min_version
    assert decoded_response.api_keys[1][2] == 10 # max_version
    #assert decoded_response.api_keys[1].tags == {}
    assert decoded_response.throttle_time_ms == 0
    assert decoded_response.tags == {}


def test_new_system_nested_field_access():
    assert 'min_version' in NewApiVersionsResponse.fields['api_keys'].fields
    min_ver_field = NewApiVersionsResponse.fields['api_keys'].fields['min_version']
    assert min_ver_field.name == 'min_version'
    assert min_ver_field._type is Int16


def test_new_system_encode_decode_response():
    # New system encoding
    response = NewApiVersionsResponse[3](
        error_code=0,
        api_keys=[
            # NewApiVersionsResponse.fields['api_keys'](
            #    api_key=0, min_version=0, max_version=9
            #),
            (0, 0, 9),
            # NewApiVersionsResponse.fields['api_keys'](
            #     api_key=1, min_version=0, max_version=10
            # )
            (1, 0, 10)
        ],
        throttle_time_ms=0
    )

    new_system_encoded_body = response.encode()
    assert new_system_encoded_body == GOLDEN_API_VERSIONS_RESPONSE_V3_BODY_BYTES

    # New system decoding
    new_system_decoded_data = NewApiVersionsResponse[3].decode(
        BytesIO(GOLDEN_API_VERSIONS_RESPONSE_V3_BODY_BYTES)
    )
    assert new_system_decoded_data.error_code == 0
    assert new_system_decoded_data.throttle_time_ms == 0
    assert new_system_decoded_data.tags is None
    assert len(new_system_decoded_data.api_keys) == 2
    # Access nested struct_array items via attributes
    assert new_system_decoded_data.api_keys[0].api_key == 0
    assert new_system_decoded_data.api_keys[0].min_version == 0
    assert new_system_decoded_data.api_keys[0].max_version == 9
    assert new_system_decoded_data.api_keys[0].tags is None
    assert new_system_decoded_data.api_keys[1].api_key == 1
    assert new_system_decoded_data.api_keys[1].min_version == 0
    assert new_system_decoded_data.api_keys[1].max_version == 10
    assert new_system_decoded_data.api_keys[1].tags is None
    # Also access via [i] lookup for compatibility w/ old system
    assert new_system_decoded_data.api_keys[0][0] == 0
    assert new_system_decoded_data.api_keys[0][1] == 0
    assert new_system_decoded_data.api_keys[0][2] == 9
    assert new_system_decoded_data.api_keys[1][0] == 1
    assert new_system_decoded_data.api_keys[1][1] == 0
    assert new_system_decoded_data.api_keys[1][2] == 10


def test_tagged_fields_retention():
    # 1. Verify no tags present (None)
    # Using GOLDEN_API_VERSIONS_RESPONSE_V3_BODY_BYTES which has empty tags
    decoded_data = NewApiVersionsResponse[3].decode(
        BytesIO(GOLDEN_API_VERSIONS_RESPONSE_V3_BODY_BYTES)
    )
    assert decoded_data.tags is None
    assert decoded_data.unknown_tags is None

    # 2. Verify explicit tags present
    # ApiVersionsResponse_v3 has 'supported_features' (tagged, tag=0)
    # Manually construct a response with supported_features set
    # Using schema definition: SupportedFeatures (tag=0) -> []SupportedFeatureKey
    # SupportedFeatureKey: Name (compact string), MinVersion (int16), MaxVersion (int16)

    # Body:
    # error_code (Int16): 0 -> \x00\x00
    # api_keys (CompactArray): 0 length -> \x01
    # throttle_time_ms (Int32): 0 -> \x00\x00\x00\x00
    # tags (TaggedFields):
    #   num_fields (varint): 1
    #   tag (varint): 0 (SupportedFeatures)
    #   size (varint): length of data
    #   data: CompactArray of SupportedFeatureKey

    # SupportedFeatureKey data:
    #   CompactArray len 1 -> \x02
    #   Item 1:
    #     Name: "feat" -> \x05feat
    #     MinVersion: 1 -> \x00\x01
    #     MaxVersion: 2 -> \x00\x02
    #     Tags: empty -> \x00
    # Total item len: 1+4 + 2 + 2 + 1 = 10 bytes
    # Array data len: 1 (len byte) + 10 = 11 bytes

    # Tag size: 11

    tagged_bytes = (
        b'\x00\x00' # error_code
        b'\x01'     # api_keys (empty)
        b'\x00\x00\x00\x00' # throttle_time_ms
        b'\x01' # num_tags = 1
        b'\x00' # tag = 0 (SupportedFeatures)
        b'\x0b' # size = 11
        b'\x02' # array len = 1
        b'\x05feat' # Name
        b'\x00\x01' # MinVersion
        b'\x00\x02' # MaxVersion
        b'\x00' # Item tags
    )

    decoded_tagged = NewApiVersionsResponse[3].decode(BytesIO(tagged_bytes))
    assert decoded_tagged.tags == {'supported_features'}
    assert decoded_tagged.supported_features is not None
    assert len(decoded_tagged.supported_features) == 1
    assert decoded_tagged.supported_features[0].name == 'feat'
    assert decoded_tagged.supported_features[0].min_version == 1
    assert decoded_tagged.supported_features[0].max_version == 2
    assert decoded_tagged.unknown_tags is None

    # 3. Verify unknown tags
    # Same base, but tag = 99 (unknown)
    # data: \x01\x02\x03 (3 bytes)
    unknown_tagged_bytes = (
        b'\x00\x00' # error_code
        b'\x01'     # api_keys (empty)
        b'\x00\x00\x00\x00' # throttle_time_ms
        b'\x01' # num_tags = 1
        b'\x63' # tag = 99 (varint \x63)
        b'\x03' # size = 3
        b'\x01\x02\x03' # data
    )

    decoded_unknown = NewApiVersionsResponse[3].decode(BytesIO(unknown_tagged_bytes))
    assert decoded_unknown.tags is None
    assert decoded_unknown.unknown_tags is not None
    assert '_99' in decoded_unknown.unknown_tags
    assert decoded_unknown.unknown_tags['_99'] == b'\x01\x02\x03'


def test_new_class_len():
    # Used by kafka/client_async.py api_version()
    assert len(NewApiVersionsRequest) == NewApiVersionsRequest.max_version + 1
