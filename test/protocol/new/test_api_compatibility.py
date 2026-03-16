import pytest
import os
import json
from io import BytesIO

from kafka.protocol.api import RequestHeaderV2
from kafka.protocol.api_versions import (
    ApiVersionsRequest_v3, ApiVersionsResponse_v3,
    ApiVersionsRequest_v4, ApiVersionsResponse_v4
)
from kafka.protocol.new.messages.metadata import (
    ApiVersionsRequest as NewApiVersionsRequest,
    ApiVersionsResponse as NewApiVersionsResponse
)

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
# api_versions = [
#   {'api_key': 0, 'min_version': 0, 'max_version': 9, 'tags': {}}, # Produce
#   {'api_key': 1, 'min_version': 0, 'max_version': 10, 'tags': {}}, # Fetch
# ]
# throttle_time_ms = 0
# tags = {}
# Response Body:
#   error_code (Int16) = 0 (0x0000)
#   api_versions (CompactArray):
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


class TestApiCompatibility:

    def test_old_system_encode_decode_request_v3(self):
        # Old system encoding
        request = ApiVersionsRequest_v3(
            client_software_name="kafka-python",
            client_software_version="2.9.0",
            tags={} # This needs to be passed explicitly for older Structs
        )
        # Manually build header for old system to match golden bytes
        header = RequestHeaderV2(
            api_key=request.API_KEY,
            api_version=request.API_VERSION,
            correlation_id=1,
            client_id='test-client',
            tags={}
        )
        encoded_header = header.encode()
        encoded_body = request.encode()
        full_encoded_request = encoded_header + encoded_body

        assert full_encoded_request == GOLDEN_API_VERSIONS_REQUEST_V3_BYTES

        # Old system decoding
        decoded_header = RequestHeaderV2.decode(BytesIO(full_encoded_request))
        decoded_request = ApiVersionsRequest_v3.decode(
            BytesIO(full_encoded_request[len(encoded_header):])
        )

        assert decoded_header.api_key == request.API_KEY
        assert decoded_header.api_version == request.API_VERSION
        assert decoded_header.correlation_id == 1
        assert decoded_header.client_id == 'test-client'
        assert decoded_header.tags == {}

        assert decoded_request.client_software_name == "kafka-python"
        assert decoded_request.client_software_version == "2.9.0"
        assert decoded_request.tags == {} # should be empty dict by default from Struct

    def test_new_system_encode_decode_request_v3(self):
        # New system encoding
        request_data = NewApiVersionsRequest(
            client_software_name="kafka-python",
            client_software_version="2.9.0"
        )
        # New system does not manage the header directly in encode/decode calls
        # We need to construct the full message with a header for comparison
        # against the golden sample which includes a header.
        # Header is managed by the client_async / conn layer which uses
        # the _tagged_fields / tags distinction from api.py
        # For now, let's just test the body encoding/decoding.
        # This simplifies the comparison.

        # The new system's encode method for ApiMessage directly encodes the message body
        # without the request header.
        new_system_encoded_body = NewApiVersionsRequest.encode(request_data, version=3)

        # To compare against GOLDEN_API_VERSIONS_REQUEST_V3_BYTES, which includes a header,
        # we need to extract the body part from the golden bytes.
        # Length of header: Int16 + Int16 + Int32 + CompactString('test-client') + TaggedFields({})
        # 2 + 2 + 4 + (1+11) + 1 = 21 bytes
        # This is brittle. Better to generate golden body bytes directly.

        # Let's generate golden body bytes directly from the old system.
        old_request_body_instance = ApiVersionsRequest_v3(
            client_software_name="kafka-python",
            client_software_version="2.9.0",
            tags={}
        )
        old_system_golden_body_bytes = old_request_body_instance.encode()
        assert new_system_encoded_body == old_system_golden_body_bytes

        # New system decoding
        new_system_decoded_data = NewApiVersionsRequest.decode(
            BytesIO(old_system_golden_body_bytes), version=3
        )
        assert new_system_decoded_data.client_software_name == "kafka-python"
        assert new_system_decoded_data.client_software_version == "2.9.0"
        assert new_system_decoded_data.tags is None

    def test_old_system_encode_decode_response_v3(self):
        # Old system encoding
        response = ApiVersionsResponse_v3(
            error_code=0,
            api_versions=[
                #{'api_key': 0, 'min_version': 0, 'max_version': 9, 'tags': {}},
                (0, 0, 9, {}),
                #{'api_key': 1, 'min_version': 0, 'max_version': 10, 'tags': {}}
                (1, 0, 10, {})
            ],
            throttle_time_ms=0,
            tags={}
        )
        encoded_body = response.encode()
        assert encoded_body == GOLDEN_API_VERSIONS_RESPONSE_V3_BODY_BYTES

        # Old system decoding
        decoded_response = ApiVersionsResponse_v3.decode(BytesIO(encoded_body))

        assert decoded_response.error_code == 0
        assert len(decoded_response.api_versions) == 2
        assert decoded_response.api_versions[0][0] == 0 # api_key
        assert decoded_response.api_versions[0][1] == 0 # min_version
        assert decoded_response.api_versions[0][2] == 9 # max_version
        #assert decoded_response.api_versions[0].tags == {}
        assert decoded_response.api_versions[1][0] == 1 # api_key
        assert decoded_response.api_versions[1][1] == 0 # min_version
        assert decoded_response.api_versions[1][2] == 10 # max_version
        #assert decoded_response.api_versions[1].tags == {}
        assert decoded_response.throttle_time_ms == 0
        #assert decoded_response.tags == {}

    def test_new_system_encode_decode_response_v3(self):
        # New system encoding
        response_data = NewApiVersionsResponse(
            error_code=0,
            api_keys=[
                NewApiVersionsResponse.fields['api_keys'](
                    api_key=0, min_version=0, max_version=9
                ),
                NewApiVersionsResponse.fields['api_keys'](
                    api_key=1, min_version=0, max_version=10
                )
            ],
            throttle_time_ms=0
        )

        # Verify nested field access
        assert 'min_version' in NewApiVersionsResponse.fields['api_keys'].fields
        min_ver_field = NewApiVersionsResponse.fields['api_keys'].fields['min_version']
        assert min_ver_field.name == 'min_version'
        # assert min_ver_field.type == Int16

        new_system_encoded_body = NewApiVersionsResponse.encode(response_data, version=3)
        assert new_system_encoded_body == GOLDEN_API_VERSIONS_RESPONSE_V3_BODY_BYTES

        # New system decoding
        new_system_decoded_data = NewApiVersionsResponse.decode(
            BytesIO(GOLDEN_API_VERSIONS_RESPONSE_V3_BODY_BYTES), version=3
        )
        assert new_system_decoded_data.error_code == 0
        assert len(new_system_decoded_data.api_keys) == 2
        assert new_system_decoded_data.api_keys[0].api_key == 0
        assert new_system_decoded_data.api_keys[0].min_version == 0
        assert new_system_decoded_data.api_keys[0].max_version == 9
        assert new_system_decoded_data.api_keys[0].tags is None
        assert new_system_decoded_data.api_keys[1].api_key == 1
        assert new_system_decoded_data.api_keys[1].min_version == 0
        assert new_system_decoded_data.api_keys[1].max_version == 10
        assert new_system_decoded_data.api_keys[1].tags is None
        assert new_system_decoded_data.throttle_time_ms == 0
        assert new_system_decoded_data.tags is None

    def test_tagged_fields_retention(self):
        # 1. Verify no tags present (None)
        # Using GOLDEN_API_VERSIONS_RESPONSE_V3_BODY_BYTES which has empty tags
        decoded_data = NewApiVersionsResponse.decode(
            BytesIO(GOLDEN_API_VERSIONS_RESPONSE_V3_BODY_BYTES), version=3
        )
        assert decoded_data.tags is None
        assert decoded_data.unknown_tags is None

        # 2. Verify explicit tags present
        # ApiVersionsResponse_v3 has 'throttle_time_ms' (not tagged) and 'supported_features' (tagged, tag=0)
        # Let's manually construct a response with supported_features set
        # Using schema definition: SupportedFeatures (tag=0) -> []SupportedFeatureKey
        # SupportedFeatureKey: Name (compact string), MinVersion (int16), MaxVersion (int16)

        # We need to construct bytes for this.
        # Base: 0 error code, 0 api_keys (empty array), 0 throttle_time_ms
        # Tags: 1 tag (tag 0) -> SupportedFeatures array

        # Body:
        # error_code (Int16): 0 -> \x00\x00
        # api_keys (CompactArray): 0 length -> \x01 (varint 0+1? No, varint len+1. So 0 len is \x01)
        # throttle_time_ms (Int32): 0 -> \x00\x00\x00\x00
        # tags (TaggedFields):
        #   num_fields (varint): 1
        #   tag (varint): 0 (SupportedFeatures)
        #   size (varint): length of data
        #   data: CompactArray of SupportedFeatureKey

        # SupportedFeatureKey data:
        #   CompactArray len 1 -> \x02
        #   Item 1:
        #     Name: "feat" -> \x05feat (len 4+1)
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

        decoded_tagged = NewApiVersionsResponse.decode(BytesIO(tagged_bytes), version=3)
        assert decoded_tagged.tags == {'supported_features'}
        assert decoded_tagged.supported_features is not None
        assert len(decoded_tagged.supported_features) == 1
        assert decoded_tagged.supported_features[0].name == 'feat'
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

        decoded_unknown = NewApiVersionsResponse.decode(BytesIO(unknown_tagged_bytes), version=3)
        assert decoded_unknown.tags is None # No known tags explicitly set
        assert decoded_unknown.unknown_tags is not None
        assert '_99' in decoded_unknown.unknown_tags
        assert decoded_unknown.unknown_tags['_99'] == b'\x01\x02\x03'
