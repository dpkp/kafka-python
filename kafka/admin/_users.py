"""User management mixin for KafkaAdminClient.

Also defines ScramMechanism, UserCredentialDeletion,
and UserCredentialUpsertion data classes.
"""

from __future__ import annotations

from enum import IntEnum
import hashlib
import logging
import os
from typing import TYPE_CHECKING

import kafka.errors as Errors
from kafka.errors import IllegalArgumentError
from kafka.protocol.admin import (
    AlterUserScramCredentialsRequest,
    DescribeUserScramCredentialsRequest,
)

if TYPE_CHECKING:
    from kafka.net.manager import KafkaConnectionManager

log = logging.getLogger(__name__)


class UserAdminMixin:
    """Mixin providing user management methods for KafkaAdminClient."""
    _manager: KafkaConnectionManager

    async def _async_alter_user_scram_credentials(self, alterations):
        deletions = []
        upsertions = []
        for alt in alterations:
            if isinstance(alt, UserScramCredentialDeletion):
                deletions.append((alt.user, int(alt.mechanism)))
            elif isinstance(alt, UserScramCredentialUpsertion):
                upsertions.append((
                    alt.user,
                    int(alt.mechanism),
                    alt.iterations,
                    alt.salt,
                    alt.salted_password,
                ))
            else:
                raise IllegalArgumentError(
                    "alterations must be UserScramCredentialDeletion or "
                    "UserScramCredentialUpsertion, got %s" % type(alt).__name__)

        request = AlterUserScramCredentialsRequest(
            deletions=deletions,
            upsertions=upsertions,
        )
        response = await self._manager.send(request)

        ret = {}
        for result in response.results:
            ret[result.user] = result.error_message if result.error_code else None
        return ret

    def alter_user_scram_credentials(self, alterations):
        """Alter SCRAM credentials for one or more users.

        Arguments:
            alterations: A list of UserScramCredentialDeletion and/or
                UserScramCredentialUpsertion objects describing the
                credentials to delete and/or insert/update.

        Returns:
            A dict mapping user name -> error message (or None on success).
        """
        return self._manager.run(self._async_alter_user_scram_credentials, alterations)

    async def _async_describe_user_scram_credentials(self, users=None):
        if users is None:
            users_field = None
        else:
            users_field = [(user,) for user in users]
        request = DescribeUserScramCredentialsRequest(users=users_field)
        response = await self._manager.send(request)

        error_type = Errors.for_code(response.error_code)
        if error_type is not Errors.NoError:
            raise error_type(
                "DescribeUserScramCredentialsRequest failed: %s"
                % (response.error_message,))

        ret = {}
        for result in response.results:
            if result.error_code:
                ret[result.user] = {
                    'error': result.error_message,
                    'credential_infos': [],
                }
            else:
                ret[result.user] = {
                    'error': None,
                    'credential_infos': [
                        {
                            'mechanism': ScramMechanism(ci.mechanism),
                            'iterations': ci.iterations,
                        }
                        for ci in result.credential_infos
                    ],
                }
        return ret

    def describe_user_scram_credentials(self, users=None):
        """Describe SCRAM credentials for one or more users.

        Arguments:
            users (list of str, optional): User names to describe. If None,
                describe all users with SCRAM credentials.

        Returns:
            A dict mapping user name to a dict with keys
            ``'error'`` (None or error message) and ``'credential_infos'``
            (list of {'mechanism': ScramMechanism, 'iterations': int}).
        """
        return self._manager.run(self._async_describe_user_scram_credentials, users)


class ScramMechanism(IntEnum):
    UNKNOWN = 0
    SCRAM_SHA_256 = 1
    SCRAM_SHA_512 = 2

    @property
    def hash_name(self):
        return {
            ScramMechanism.SCRAM_SHA_256: 'sha256',
            ScramMechanism.SCRAM_SHA_512: 'sha512',
        }[self]


class UserScramCredentialDeletion:
    """Specifies that a SCRAM credential should be deleted.

    Arguments:
        user (str): The user name.
        mechanism (ScramMechanism or int or str): The SCRAM mechanism to
            delete for this user.
    """
    def __init__(self, user, mechanism):
        if not isinstance(mechanism, ScramMechanism):
            if isinstance(mechanism, str):
                mechanism = ScramMechanism[mechanism.upper().replace('-', '_')]
            else:
                mechanism = ScramMechanism(mechanism)
        self.user = user
        self.mechanism = mechanism

    def __repr__(self):
        return f"UserScramCredentialDeletion({self.user}, {self.mechanism.name})"


class UserScramCredentialUpsertion:
    """Specifies that a SCRAM credential should be inserted or updated.

    Arguments:
        user (str): The user name.
        mechanism (ScramMechanism or int or str): The SCRAM mechanism.
        password (bytes or str): The plaintext password. The salted
            password sent to the broker is derived via PBKDF2-HMAC using
            the given salt and iteration count.

    Keyword Arguments:
        iterations (int, optional): PBKDF2 iteration count. Default: 4096.
        salt (bytes, optional): Salt to use. If omitted, a random 24-byte
            salt is generated.
    """
    DEFAULT_ITERATIONS = 4096

    def __init__(self, user, mechanism, password, iterations=None, salt=None):
        if not isinstance(mechanism, ScramMechanism):
            if isinstance(mechanism, str):
                mechanism = ScramMechanism[mechanism.upper().replace('-', '_')]
            else:
                mechanism = ScramMechanism(mechanism)
        if mechanism == ScramMechanism.UNKNOWN:
            raise IllegalArgumentError("SCRAM mechanism must not be UNKNOWN")
        self.user = user
        self.mechanism = mechanism
        self.iterations = iterations if iterations is not None else self.DEFAULT_ITERATIONS
        self.salt = salt if salt is not None else os.urandom(24)
        if isinstance(password, str):
            password = password.encode('utf-8')
        self.salted_password = hashlib.pbkdf2_hmac(
            mechanism.hash_name, password, self.salt, self.iterations)

    def __repr__(self):
        return (f"UserScramCredentialUpsertion({self.user}, "
                f"{self.mechanism.name}, iterations={self.iterations})")
