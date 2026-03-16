import pytest

from kafka.protocol.new.sasl import (
    SaslHandshakeRequest, SaslHandshakeResponse,
    SaslAuthenticateRequest, SaslAuthenticateResponse,
)


@pytest.mark.parametrize("version", range(SaslHandshakeRequest.min_version, SaslHandshakeRequest.max_version + 1))
def test_sasl_handshake_request_roundtrip(version):
    data = SaslHandshakeRequest(
        mechanism="PLAIN"
    )
    encoded = SaslHandshakeRequest.encode(data, version=version)
    decoded = SaslHandshakeRequest.decode(encoded, version=version)
    assert decoded == data


@pytest.mark.parametrize("version", range(SaslHandshakeResponse.min_version, SaslHandshakeResponse.max_version + 1))
def test_sasl_handshake_response_roundtrip(version):
    data = SaslHandshakeResponse(
        error_code=0,
        mechanisms=["PLAIN", "SCRAM-SHA-256"]
    )
    encoded = SaslHandshakeResponse.encode(data, version=version)
    decoded = SaslHandshakeResponse.decode(encoded, version=version)
    assert decoded == data


@pytest.mark.parametrize("version", range(SaslAuthenticateRequest.min_version, SaslAuthenticateRequest.max_version + 1))
def test_sasl_authenticate_request_roundtrip(version):
    data = SaslAuthenticateRequest(
        auth_bytes=b"sasl-payload"
    )
    encoded = SaslAuthenticateRequest.encode(data, version=version)
    decoded = SaslAuthenticateRequest.decode(encoded, version=version)
    assert decoded == data


@pytest.mark.parametrize("version", range(SaslAuthenticateResponse.min_version, SaslAuthenticateResponse.max_version + 1))
def test_sasl_authenticate_response_roundtrip(version):
    data = SaslAuthenticateResponse(
        error_code=0,
        error_message=None,
        auth_bytes=b"server-payload",
        session_lifetime_ms=3600000 if version >= 1 else 0
    )
    encoded = SaslAuthenticateResponse.encode(data, version=version)
    decoded = SaslAuthenticateResponse.decode(encoded, version=version)
    assert decoded == data
