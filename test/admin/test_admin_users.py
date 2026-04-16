import hashlib

import pytest

from kafka.admin import (
    KafkaAdminClient, ScramMechanism,
    UserScramCredentialDeletion, UserScramCredentialUpsertion,
)
from kafka.errors import IllegalArgumentError
from kafka.protocol.admin import (
    AlterUserScramCredentialsRequest, AlterUserScramCredentialsResponse,
)

from test.mock_broker import MockBroker


class TestScramMechanism:

    def test_hash_name(self):
        assert ScramMechanism.SCRAM_SHA_256.hash_name == 'sha256'
        assert ScramMechanism.SCRAM_SHA_512.hash_name == 'sha512'


class TestUserScramCredentialDeletion:

    @pytest.mark.parametrize("mechanism", [
        ScramMechanism.SCRAM_SHA_256,
        1,
        'SCRAM_SHA_256',
        'scram-sha-256',
    ])
    def test_mechanism_accepts_enum_int_and_str(self, mechanism):
        deletion = UserScramCredentialDeletion('alice', mechanism)
        assert deletion.user == 'alice'
        assert deletion.mechanism is ScramMechanism.SCRAM_SHA_256

    def test_invalid_mechanism_int(self):
        with pytest.raises(ValueError):
            UserScramCredentialDeletion('alice', 99)


class TestUserScramCredentialUpsertion:

    def test_default_iterations_and_random_salt(self):
        up = UserScramCredentialUpsertion(
            'alice', ScramMechanism.SCRAM_SHA_256, 'password')
        assert up.user == 'alice'
        assert up.mechanism is ScramMechanism.SCRAM_SHA_256
        assert up.iterations == UserScramCredentialUpsertion.DEFAULT_ITERATIONS
        assert isinstance(up.salt, bytes) and len(up.salt) == 24
        # salt is random -- two instances should (almost certainly) differ
        other = UserScramCredentialUpsertion(
            'alice', ScramMechanism.SCRAM_SHA_256, 'password')
        assert up.salt != other.salt

    def test_salted_password_matches_pbkdf2(self):
        salt = b'fixed-salt-bytes'
        up = UserScramCredentialUpsertion(
            'alice', ScramMechanism.SCRAM_SHA_512,
            password='password', iterations=1024, salt=salt)
        expected = hashlib.pbkdf2_hmac('sha512', b'password', salt, 1024)
        assert up.salted_password == expected
        assert up.salt == salt
        assert up.iterations == 1024

    def test_password_accepts_bytes(self):
        up = UserScramCredentialUpsertion(
            'alice', ScramMechanism.SCRAM_SHA_256,
            password=b'password', iterations=512, salt=b'salt')
        expected = hashlib.pbkdf2_hmac('sha256', b'password', b'salt', 512)
        assert up.salted_password == expected

    def test_mechanism_string_normalization(self):
        up = UserScramCredentialUpsertion(
            'alice', 'SCRAM-SHA-512', password='p', iterations=1, salt=b's')
        assert up.mechanism is ScramMechanism.SCRAM_SHA_512

    def test_unknown_mechanism_rejected(self):
        with pytest.raises(IllegalArgumentError):
            UserScramCredentialUpsertion(
                'alice', ScramMechanism.UNKNOWN, password='p')


# ---------------------------------------------------------------------------
# MockBroker tests exercising the full wire round-trip
# ---------------------------------------------------------------------------


def _make_admin(broker):
    return KafkaAdminClient(
        kafka_client=broker.client_factory(),
        bootstrap_servers='%s:%d' % (broker.host, broker.port),
        api_version=broker.broker_version,
        request_timeout_ms=5000,
    )


class TestAlterUserScramCredentialsMockBroker:

    def test_all_success_returns_none_values(self):
        broker = MockBroker()
        Result = AlterUserScramCredentialsResponse.AlterUserScramCredentialsResult
        broker.respond(
            AlterUserScramCredentialsRequest,
            AlterUserScramCredentialsResponse(
                throttle_time_ms=0,
                results=[
                    Result(user='alice', error_code=0, error_message=None),
                    Result(user='bob', error_code=0, error_message=None),
                ],
            ),
        )

        admin = _make_admin(broker)
        try:
            result = admin.alter_user_scram_credentials([
                UserScramCredentialDeletion('alice', ScramMechanism.SCRAM_SHA_256),
                UserScramCredentialUpsertion(
                    'bob', ScramMechanism.SCRAM_SHA_512,
                    password='secret', iterations=4096, salt=b'fixed-salt'),
            ])
        finally:
            admin.close()

        assert result == {'alice': None, 'bob': None}

    def test_partial_errors_returned_in_dict(self):
        broker = MockBroker()
        Result = AlterUserScramCredentialsResponse.AlterUserScramCredentialsResult
        broker.respond(
            AlterUserScramCredentialsRequest,
            AlterUserScramCredentialsResponse(
                throttle_time_ms=0,
                results=[
                    Result(user='alice', error_code=0, error_message=None),
                    Result(user='bob', error_code=58,
                           error_message='Unsupported SASL mechanism'),
                ],
            ),
        )

        admin = _make_admin(broker)
        try:
            result = admin.alter_user_scram_credentials([
                UserScramCredentialDeletion('alice', ScramMechanism.SCRAM_SHA_256),
                UserScramCredentialDeletion('bob', ScramMechanism.SCRAM_SHA_512),
            ])
        finally:
            admin.close()

        assert result == {
            'alice': None,
            'bob': 'Unsupported SASL mechanism',
        }

    def test_request_is_encoded_with_deletions_and_upsertions(self):
        broker = MockBroker()
        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            decoded = AlterUserScramCredentialsRequest.decode(
                request_bytes, version=api_version, header=True)
            captured['request'] = decoded
            Result = AlterUserScramCredentialsResponse.AlterUserScramCredentialsResult
            return AlterUserScramCredentialsResponse(
                throttle_time_ms=0,
                results=[
                    Result(user='alice', error_code=0, error_message=None),
                    Result(user='bob', error_code=0, error_message=None),
                ],
            )

        broker.respond_fn(AlterUserScramCredentialsRequest, handler)

        salt = b'fixed-salt-bytes'
        upsertion = UserScramCredentialUpsertion(
            'bob', ScramMechanism.SCRAM_SHA_512,
            password='secret', iterations=2048, salt=salt)

        admin = _make_admin(broker)
        try:
            admin.alter_user_scram_credentials([
                UserScramCredentialDeletion('alice', ScramMechanism.SCRAM_SHA_256),
                upsertion,
            ])
        finally:
            admin.close()

        request = captured['request']
        assert len(request.deletions) == 1
        assert request.deletions[0].name == 'alice'
        assert request.deletions[0].mechanism == int(ScramMechanism.SCRAM_SHA_256)

        assert len(request.upsertions) == 1
        ups = request.upsertions[0]
        assert ups.name == 'bob'
        assert ups.mechanism == int(ScramMechanism.SCRAM_SHA_512)
        assert ups.iterations == 2048
        assert ups.salt == salt
        assert ups.salted_password == hashlib.pbkdf2_hmac(
            'sha512', b'secret', salt, 2048)
