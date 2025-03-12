from __future__ import absolute_import

import abc

from kafka.sasl.abc import SaslMechanism


class SaslMechanismOAuth(SaslMechanism):

    def __init__(self, **config):
        assert 'sasl_oauth_token_provider' in config, 'sasl_oauth_token_provider required for OAUTHBEARER sasl'
        assert isinstance(config['sasl_oauth_token_provider'], AbstractTokenProvider), \
            'sasl_oauth_token_provider must implement kafka.sasl.oauth.AbstractTokenProvider'
        self.token_provider = config['sasl_oauth_token_provider']
        self._is_done = False
        self._is_authenticated = False

    def auth_bytes(self):
        token = self.token_provider.token()
        extensions = self._token_extensions()
        return "n,,\x01auth=Bearer {}{}\x01\x01".format(token, extensions).encode('utf-8')

    def receive(self, auth_bytes):
        self._is_done = True
        self._is_authenticated = auth_bytes == b''

    def is_done(self):
        return self._is_done

    def is_authenticated(self):
        return self._is_authenticated

    def _token_extensions(self):
        """
        Return a string representation of the OPTIONAL key-value pairs that can be sent with an OAUTHBEARER
        initial request.
        """
        # Builds up a string separated by \x01 via a dict of key value pairs
        extensions = self.token_provider.extensions()
        msg = '\x01'.join(['{}={}'.format(k, v) for k, v in extensions.items()])
        return '\x01' + msg if msg else ''

    def auth_details(self):
        if not self.is_authenticated:
            raise RuntimeError('Not authenticated yet!')
        return 'Authenticated via SASL / OAuth'

# This statement is compatible with both Python 2.7 & 3+
ABC = abc.ABCMeta('ABC', (object,), {'__slots__': ()})

class AbstractTokenProvider(ABC):
    """
    A Token Provider must be used for the SASL OAuthBearer protocol.

    The implementation should ensure token reuse so that multiple
    calls at connect time do not create multiple tokens. The implementation
    should also periodically refresh the token in order to guarantee
    that each call returns an unexpired token. A timeout error should
    be returned after a short period of inactivity so that the
    broker can log debugging info and retry.

    Token Providers MUST implement the token() method
    """

    def __init__(self, **config):
        pass

    @abc.abstractmethod
    def token(self):
        """
        Returns a (str) ID/Access Token to be sent to the Kafka
        client.
        """
        pass

    def extensions(self):
        """
        This is an OPTIONAL method that may be implemented.

        Returns a map of key-value pairs that can
        be sent with the SASL/OAUTHBEARER initial client request. If
        not implemented, the values are ignored. This feature is only available
        in Kafka >= 2.1.0.

        All returned keys and values should be type str
        """
        return {}
