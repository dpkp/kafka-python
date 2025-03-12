from __future__ import absolute_import

import logging

# Windows-only
try:
    import sspi
    import pywintypes
    import sspicon
    import win32security
except ImportError:
    sspi = None

from kafka.sasl.abc import SaslMechanism


log = logging.getLogger(__name__)


class SaslMechanismSSPI(SaslMechanism):
    # Establish security context and negotiate protection level
    # For reference see RFC 4752, section 3

    SASL_QOP_AUTH = 1
    SASL_QOP_AUTH_INT = 2
    SASL_QOP_AUTH_CONF = 4

    def __init__(self, **config):
        assert sspi is not None, 'No GSSAPI lib available (gssapi or sspi)'
        if 'sasl_kerberos_name' not in config and 'sasl_kerberos_service_name' not in config:
            raise ValueError('sasl_kerberos_service_name or sasl_kerberos_name required for GSSAPI sasl configuration')
        self._is_done = False
        self._is_authenticated = False
        if config.get('sasl_kerberos_name', None) is not None:
            self.auth_id = str(config['sasl_kerberos_name'])
        else:
            kerberos_domain_name = config.get('sasl_kerberos_domain_name', '') or config.get('host', '')
            self.auth_id = config['sasl_kerberos_service_name'] + '/' + kerberos_domain_name
        scheme = "Kerberos"  # Do not try with Negotiate for SASL authentication. Tokens are different.
        # https://docs.microsoft.com/en-us/windows/win32/secauthn/context-requirements
        flags = (
            sspicon.ISC_REQ_MUTUAL_AUTH |      # mutual authentication
            sspicon.ISC_REQ_INTEGRITY |        # check for integrity
            sspicon.ISC_REQ_SEQUENCE_DETECT |  # enable out-of-order messages
            sspicon.ISC_REQ_CONFIDENTIALITY    # request confidentiality
        )
        self._client_ctx = sspi.ClientAuth(scheme, targetspn=self.auth_id, scflags=flags)
        self._next_token = self._client_ctx.step(None)

    def auth_bytes(self):
        # GSSAPI Auth does not have a final broker->client message
        # so mark is_done after the final auth_bytes are provided
        # in practice we'll still receive a response when using SaslAuthenticate
        # but not when using the prior unframed approach.
        if self._client_ctx.authenticated:
            self._is_done = True
            self._is_authenticated = True
        return self._next_token or b''

    def receive(self, auth_bytes):
        log.debug("Received token from server (size %s)", len(auth_bytes))
        if not self._client_ctx.authenticated:
            # calculate an output token from kafka token (or None on first iteration)
            # https://docs.microsoft.com/en-us/windows/win32/api/sspi/nf-sspi-initializesecuritycontexta
            # https://docs.microsoft.com/en-us/windows/win32/secauthn/initializesecuritycontext--kerberos
            # authorize method will wrap for us our token in sspi structures
            error, auth = self._client_ctx.authorize(auth_bytes)
            if len(auth) > 0 and len(auth[0].Buffer):
                log.debug("Got token from context")
                # this buffer must be sent to the server whatever the result is
                self._next_token = auth[0].Buffer
            else:
                log.debug("Got no token, exchange finished")
                # seems to be the end of the loop
                self._next_token = b''
        elif self._is_done:
            # The final step of gssapi is send, so we do not expect any additional bytes
            # however, allow an empty message to support SaslAuthenticate response
            if auth_bytes != b'':
                raise ValueError("Unexpected receive auth_bytes after sasl/gssapi completion")
        else:
            # Process the security layer negotiation token, sent by the server
            # once the security context is established.

            # The following part is required by SASL, but not by classic Kerberos.
            # See RFC 4752

            # unwraps message containing supported protection levels and msg size
            msg, _was_encrypted = self._client_ctx.unwrap(auth_bytes)

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
            self._next_token = self._client_ctx.wrap(b''.join(message_parts), False)

    def is_done(self):
        return self._is_done

    def is_authenticated(self):
        return self._is_authenticated

    def auth_details(self):
        return 'Authenticated as %s to %s via SASL / SSPI/GSSAPI \\o/' % (self._client_ctx.initiator_name, self._client_ctx.service_name)
