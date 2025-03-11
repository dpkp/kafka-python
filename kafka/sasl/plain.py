from __future__ import absolute_import

import logging

from kafka.sasl.abc import SaslMechanism


log = logging.getLogger(__name__)


class SaslMechanismPlain(SaslMechanism):

    def __init__(self, **config):
        if config['security_protocol'] == 'SASL_PLAINTEXT':
            log.warning('Sending username and password in the clear')
        assert config['sasl_plain_username'] is not None, 'sasl_plain_username required for PLAIN sasl'
        assert config['sasl_plain_password'] is not None, 'sasl_plain_password required for PLAIN sasl'

        self.username = config['sasl_plain_username']
        self.password = config['sasl_plain_password']
        self._is_done = False
        self._is_authenticated = False

    def auth_bytes(self):
        # Send PLAIN credentials per RFC-4616
        return bytes('\0'.join([self.username, self.username, self.password]).encode('utf-8'))

    def receive(self, auth_bytes):
        self._is_done = True
        self._is_authenticated = auth_bytes == b''

    def is_done(self):
        return self._is_done

    def is_authenticated(self):
        return self._is_authenticated
