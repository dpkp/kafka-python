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
        assert config['sasl_kerberos_service_name'] is not None, 'sasl_kerberos_service_name required for GSSAPI sasl'
        self._is_done = False
        self._is_authenticated = False
        self.kerberos_damin_name = config['sasl_kerberos_domain_name'] or config['host']
        self.auth_id = config['sasl_kerberos_service_name'] + '@' + kerberos_damin_name
        self.gssapi_name = gssapi.Name(auth_id, name_type=gssapi.NameType.hostbased_service).canonicalize(gssapi.MechType.kerberos)
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
            msg = client_ctx.unwrap(received_token).message
            # Kafka currently doesn't support integrity or confidentiality security layers, so we
            # simply set QoP to 'auth' only (first octet). We reuse the max message size proposed
            # by the server
            message_parts = [
                Int8.encode(self.SASL_QOP_AUTH & Int8.decode(io.BytesIO(msg[0:1]))),
                msg[:1],
                self.auth_id.encode(),
            ]
            # add authorization identity to the response, and GSS-wrap
            self._next_token = self._client_ctx.wrap(b''.join(message_parts), False).message

    def is_done(self):
        return self._is_done

    def is_authenticated(self):
        return self._is_authenticated
