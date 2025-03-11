from __future__ import absolute_import

from kafka.sasl.abc import SaslMechanism


class SaslMechanismOAuth(SaslMechanism):

    def __init__(self, **config):
        assert 'sasl_oauth_token_provider' in config, 'sasl_oauth_token_provider required for OAUTHBEARER sasl'
        self.token_provider = config['sasl_oauth_token_provider']
        assert callable(getattr(self.token_provider, 'token', None)), 'sasl_oauth_token_provider must implement method #token()'
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
        # Only run if the #extensions() method is implemented by the clients Token Provider class
        # Builds up a string separated by \x01 via a dict of key value pairs
        extensions = getattr(self.token_provider, 'extensions', lambda: [])()
        msg = '\x01'.join(['{}={}'.format(k, v) for k, v in extensions.items()])
        return '\x01' + msg if msg else ''
