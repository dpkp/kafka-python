from __future__ import absolute_import

import base64
import hashlib
import hmac
import logging
import uuid


from kafka.sasl.abc import SaslMechanism
from kafka.vendor import six


log = logging.getLogger(__name__)


if six.PY2:
    def xor_bytes(left, right):
        return bytearray(ord(lb) ^ ord(rb) for lb, rb in zip(left, right))
else:
    def xor_bytes(left, right):
        return bytes(lb ^ rb for lb, rb in zip(left, right))


class SaslMechanismScram(SaslMechanism):
    def __init__(self, **config):
        assert 'sasl_plain_username' in config, 'sasl_plain_username required for SCRAM sasl'
        assert 'sasl_plain_password' in config, 'sasl_plain_password required for SCRAM sasl'
        assert config.get('sasl_mechanism', '') in ScramClient.MECHANISMS, 'Unrecognized SCRAM mechanism'
        if config.get('security_protocol', '') == 'SASL_PLAINTEXT':
            log.warning('Exchanging credentials in the clear during Sasl Authentication')

        self.username = config['sasl_plain_username']
        self.mechanism = config['sasl_mechanism']
        self._scram_client = ScramClient(
            config['sasl_plain_username'],
            config['sasl_plain_password'],
            config['sasl_mechanism']
        )
        self._state = 0

    def auth_bytes(self):
        if self._state == 0:
            return self._scram_client.first_message()
        elif self._state == 1:
            return self._scram_client.final_message()
        else:
            raise ValueError('No auth_bytes for state: %s' % self._state)

    def receive(self, auth_bytes):
        if self._state == 0:
            self._scram_client.process_server_first_message(auth_bytes)
        elif self._state == 1:
            self._scram_client.process_server_final_message(auth_bytes)
        else:
            raise ValueError('Cannot receive bytes in state: %s' % self._state)
        self._state += 1
        return self.is_done()

    def is_done(self):
        return self._state == 2

    def is_authenticated(self):
        # receive raises if authentication fails...?
        return self._state == 2

    def auth_details(self):
        if not self.is_authenticated:
            raise RuntimeError('Not authenticated yet!')
        return 'Authenticated as %s via SASL / %s' % (self.username, self.mechanism)


class ScramClient:
    MECHANISMS = {
        'SCRAM-SHA-256': hashlib.sha256,
        'SCRAM-SHA-512': hashlib.sha512
    }

    def __init__(self, user, password, mechanism):
        self.nonce = str(uuid.uuid4()).replace('-', '').encode('utf-8')
        self.auth_message = b''
        self.salted_password = None
        self.user = user.encode('utf-8')
        self.password = password.encode('utf-8')
        self.hashfunc = self.MECHANISMS[mechanism]
        self.hashname = ''.join(mechanism.lower().split('-')[1:3])
        self.stored_key = None
        self.client_key = None
        self.client_signature = None
        self.client_proof = None
        self.server_key = None
        self.server_signature = None

    def first_message(self):
        client_first_bare = b'n=' + self.user + b',r=' + self.nonce
        self.auth_message += client_first_bare
        return b'n,,' + client_first_bare

    def process_server_first_message(self, server_first_message):
        self.auth_message += b',' + server_first_message
        params = dict(pair.split('=', 1) for pair in server_first_message.decode('utf-8').split(','))
        server_nonce = params['r'].encode('utf-8')
        if not server_nonce.startswith(self.nonce):
            raise ValueError("Server nonce, did not start with client nonce!")
        self.nonce = server_nonce
        self.auth_message += b',c=biws,r=' + self.nonce

        salt = base64.b64decode(params['s'].encode('utf-8'))
        iterations = int(params['i'])
        self.create_salted_password(salt, iterations)

        self.client_key = self.hmac(self.salted_password, b'Client Key')
        self.stored_key = self.hashfunc(self.client_key).digest()
        self.client_signature = self.hmac(self.stored_key, self.auth_message)
        self.client_proof = xor_bytes(self.client_key, self.client_signature)
        self.server_key = self.hmac(self.salted_password, b'Server Key')
        self.server_signature = self.hmac(self.server_key, self.auth_message)

    def hmac(self, key, msg):
        return hmac.new(key, msg, digestmod=self.hashfunc).digest()

    def create_salted_password(self, salt, iterations):
        self.salted_password = hashlib.pbkdf2_hmac(
            self.hashname, self.password, salt, iterations
        )

    def final_message(self):
        return b'c=biws,r=' + self.nonce + b',p=' + base64.b64encode(self.client_proof)

    def process_server_final_message(self, server_final_message):
        params = dict(pair.split('=', 1) for pair in server_final_message.decode('utf-8').split(','))
        if self.server_signature != base64.b64decode(params['v'].encode('utf-8')):
            raise ValueError("Server sent wrong signature!")
