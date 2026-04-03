import pytest

from kafka.protocol.sasl import (
    SaslHandshakeRequest, SaslHandshakeResponse,
    SaslAuthenticateRequest, SaslAuthenticateResponse,
)


@pytest.mark.parametrize("version", range(SaslHandshakeRequest.min_version, SaslHandshakeRequest.max_version + 1))
def test_sasl_handshake_request_roundtrip(version):
    request = SaslHandshakeRequest(
        mechanism="PLAIN"
    )
    encoded = request.encode(version=version)
    decoded = SaslHandshakeRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(SaslHandshakeResponse.min_version, SaslHandshakeResponse.max_version + 1))
def test_sasl_handshake_response_roundtrip(version):
    response = SaslHandshakeResponse(
        error_code=2,
        mechanisms=["PLAIN", "SCRAM-SHA-256"]
    )
    encoded = response.encode(version=version)
    decoded = SaslHandshakeResponse.decode(encoded, version=version)
    assert decoded == response


@pytest.mark.parametrize("version", range(SaslAuthenticateRequest.min_version, SaslAuthenticateRequest.max_version + 1))
def test_sasl_authenticate_request_roundtrip(version):
    request = SaslAuthenticateRequest(
        auth_bytes=b"sasl-payload"
    )
    encoded = request.encode(version=version)
    decoded = SaslAuthenticateRequest.decode(encoded, version=version)
    assert decoded == request


@pytest.mark.parametrize("version", range(SaslAuthenticateResponse.min_version, SaslAuthenticateResponse.max_version + 1))
def test_sasl_authenticate_response_roundtrip(version):
    response = SaslAuthenticateResponse(
        error_code=12,
        error_message='error',
        auth_bytes=b"server-payload",
        session_lifetime_ms=3600000 if version >= 1 else 0
    )
    encoded = response.encode(version=version)
    decoded = SaslAuthenticateResponse.decode(encoded, version=version)
    assert decoded == response
