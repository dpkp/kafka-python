from __future__ import absolute_import

# needed for SASL_GSSAPI authentication:
try:
    import gssapi
    from gssapi.raw.misc import GSSError
except (ImportError, OSError):
    #no gssapi available, will disable gssapi mechanism
    gssapi = None
    GSSError = None

from kafka.sasl.abc import SaslMechanism


class SaslMechanismGSSAPI(SaslMechanism):
    # Establish security context and negotiate protection level
    # For reference RFC 2222, section 7.2.1

    SASL_QOP_AUTH = 1
    SASL_QOP_AUTH_INT = 2
    SASL_QOP_AUTH_CONF = 4

    def __init__(self, **config):
        assert gssapi is not None, 'GSSAPI lib not available'
        if 'sasl_kerberos_name' not in config and 'sasl_kerberos_service_name' not in config:
            raise ValueError('sasl_kerberos_service_name or sasl_kerberos_name required for GSSAPI sasl configuration')
        self._is_done = False
        self._is_authenticated = False
        if config.get('sasl_kerberos_name', None) is not None:
            self.auth_id = str(config['sasl_kerberos_name'])
        else:
            kerberos_domain_name = config.get('sasl_kerberos_domain_name', '') or config.get('host', '')
            self.auth_id = config['sasl_kerberos_service_name'] + '@' + kerberos_domain_name
        if isinstance(config.get('sasl_kerberos_name', None), gssapi.Name):
            self.gssapi_name = config['sasl_kerberos_name']
        else:
            self.gssapi_name = gssapi.Name(self.auth_id, name_type=gssapi.NameType.hostbased_service).canonicalize(gssapi.MechType.kerberos)
        self._client_ctx = gssapi.SecurityContext(name=self.gssapi_name, usage='initiate')
        self._next_token = self._client_ctx.step(None)

    def auth_bytes(self):
        # GSSAPI Auth does not have a final broker->client message
        # so mark is_done after the final auth_bytes are provided
        # in practice we'll still receive a response when using SaslAuthenticate
        # but not when using the prior unframed approach.
        if self._client_ctx.complete:
            self._is_done = True
            self._is_authenticated = True
        return self._next_token or b''

    def receive(self, auth_bytes):
        if not self._client_ctx.complete:
            # The server will send a token back. Processing of this token either
            # establishes a security context, or it needs further token exchange.
            # The gssapi will be able to identify the needed next step.
            self._next_token = self._client_ctx.step(auth_bytes)
        elif self._is_done:
            # The final step of gssapi is send, so we do not expect any additional bytes
            # however, allow an empty message to support SaslAuthenticate response
            if auth_bytes != b'':
                raise ValueError("Unexpected receive auth_bytes after sasl/gssapi completion")
        else:
            # unwraps message containing supported protection levels and msg size
            msg = self._client_ctx.unwrap(auth_bytes).message
            # Kafka currently doesn't support integrity or confidentiality security layers, so we
            # simply set QoP to 'auth' only (first octet). We reuse the max message size proposed
            # by the server
            client_flags = self.SASL_QOP_AUTH
            server_flags = msg[0]
            message_parts = [
                bytes(client_flags & server_flags),
                msg[:1],
                self.auth_id.encode('utf-8'),
            ]
            # add authorization identity to the response, and GSS-wrap
            self._next_token = self._client_ctx.wrap(b''.join(message_parts), False).message

    def is_done(self):
        return self._is_done

    def is_authenticated(self):
        return self._is_authenticated

    def auth_details(self):
        if not self.is_authenticated:
            raise RuntimeError('Not authenticated yet!')
        return 'Authenticated as %s to %s via SASL / GSSAPI' % (self._client_ctx.initiator_name, self._client_ctx.target_name)
