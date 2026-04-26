import hashlib

import pytest

from kafka.admin import (
    ScramMechanism, UserScramCredentialDeletion, UserScramCredentialUpsertion,
)
from kafka.errors import IllegalArgumentError
from kafka.protocol.admin import (
    AlterUserScramCredentialsRequest, AlterUserScramCredentialsResponse,
    DescribeUserScramCredentialsRequest, DescribeUserScramCredentialsResponse,
)


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


class TestAlterUserScramCredentialsMockBroker:
    def test_all_success_returns_none_values(self, broker, admin):
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
        result = admin.alter_user_scram_credentials([
            UserScramCredentialDeletion('alice', ScramMechanism.SCRAM_SHA_256),
            UserScramCredentialUpsertion(
                'bob', ScramMechanism.SCRAM_SHA_512,
                password='secret', iterations=4096, salt=b'fixed-salt'),
        ])
        assert result == {'alice': None, 'bob': None}

    def test_partial_errors_returned_in_dict(self, broker, admin):
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
        result = admin.alter_user_scram_credentials([
            UserScramCredentialDeletion('alice', ScramMechanism.SCRAM_SHA_256),
            UserScramCredentialDeletion('bob', ScramMechanism.SCRAM_SHA_512),
        ])
        assert result == {
            'alice': None,
            'bob': 'Unsupported SASL mechanism',
        }

    def test_request_is_encoded_with_deletions_and_upsertions(self, broker, admin):
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

        admin.alter_user_scram_credentials([
            UserScramCredentialDeletion('alice', ScramMechanism.SCRAM_SHA_256),
            upsertion,
        ])

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


class TestDescribeUserScramCredentialsMockBroker:
    def test_returns_credentials_per_user(self, broker, admin):
        Result = DescribeUserScramCredentialsResponse.DescribeUserScramCredentialsResult
        CI = Result.CredentialInfo
        broker.respond(
            DescribeUserScramCredentialsRequest,
            DescribeUserScramCredentialsResponse(
                throttle_time_ms=0,
                error_code=0,
                error_message=None,
                results=[
                    Result(user='alice', error_code=0, error_message=None,
                           credential_infos=[
                               CI(mechanism=1, iterations=4096),
                               CI(mechanism=2, iterations=8192),
                           ]),
                    Result(user='bob', error_code=0, error_message=None,
                           credential_infos=[CI(mechanism=2, iterations=16384)]),
                ],
            ),
        )

        result = admin.describe_user_scram_credentials(['alice', 'bob'])
        assert result == {
            'alice': {
                'error': None,
                'credential_infos': [
                    {'mechanism': ScramMechanism.SCRAM_SHA_256, 'iterations': 4096},
                    {'mechanism': ScramMechanism.SCRAM_SHA_512, 'iterations': 8192},
                ],
            },
            'bob': {
                'error': None,
                'credential_infos': [
                    {'mechanism': ScramMechanism.SCRAM_SHA_512, 'iterations': 16384},
                ],
            },
        }

    def test_per_user_error_reported(self, broker, admin):
        Result = DescribeUserScramCredentialsResponse.DescribeUserScramCredentialsResult
        broker.respond(
            DescribeUserScramCredentialsRequest,
            DescribeUserScramCredentialsResponse(
                throttle_time_ms=0,
                error_code=0,
                error_message=None,
                results=[
                    Result(user='missing', error_code=68,
                           error_message='resource not found',
                           credential_infos=[]),
                ],
            ),
        )

        result = admin.describe_user_scram_credentials(['missing'])
        assert result == {
            'missing': {
                'error': 'resource not found',
                'credential_infos': [],
            },
        }

    def test_top_level_error_raises(self, broker, admin):
        broker.respond(
            DescribeUserScramCredentialsRequest,
            DescribeUserScramCredentialsResponse(
                throttle_time_ms=0,
                error_code=58,  # UnsupportedSaslMechanismError
                error_message='SCRAM not configured',
                results=[],
            ),
        )

        with pytest.raises(Exception) as exc_info:
            admin.describe_user_scram_credentials(['alice'])
        assert 'SCRAM not configured' in str(exc_info.value)

    def test_describe_all_users_sends_null(self, broker, admin):
        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            decoded = DescribeUserScramCredentialsRequest.decode(
                request_bytes, version=api_version, header=True)
            captured['request'] = decoded
            return DescribeUserScramCredentialsResponse(
                throttle_time_ms=0,
                error_code=0,
                error_message=None,
                results=[],
            )

        broker.respond_fn(DescribeUserScramCredentialsRequest, handler)

        result = admin.describe_user_scram_credentials()
        assert result == {}
        assert captured['request'].users is None

    def test_describe_specific_users_encodes_names(self, broker, admin):
        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            decoded = DescribeUserScramCredentialsRequest.decode(
                request_bytes, version=api_version, header=True)
            captured['request'] = decoded
            return DescribeUserScramCredentialsResponse(
                throttle_time_ms=0,
                error_code=0,
                error_message=None,
                results=[],
            )

        broker.respond_fn(DescribeUserScramCredentialsRequest, handler)

        admin.describe_user_scram_credentials(['alice', 'bob'])
        request_users = captured['request'].users
        assert [u.name for u in request_users] == ['alice', 'bob']
