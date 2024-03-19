import io
import logging
import struct

import kafka.errors as Errors
from kafka.protocol.types import Int8, Int32

try:
    import gssapi
    from gssapi.raw.misc import GSSError
except ImportError:
    gssapi = None
    GSSError = None

log = logging.getLogger(__name__)

SASL_QOP_AUTH = 1


def validate_config(conn):
    assert gssapi is not None, (
        'gssapi library required when sasl_mechanism=GSSAPI'
    )
    assert conn.config['sasl_kerberos_service_name'] is not None, (
        'sasl_kerberos_service_name required when sasl_mechanism=GSSAPI'
    )


def try_authenticate(conn, future):
    kerberos_damin_name = conn.config['sasl_kerberos_domain_name'] or conn.host
    auth_id = conn.config['sasl_kerberos_service_name'] + '@' + kerberos_damin_name
    gssapi_name = gssapi.Name(
        auth_id,
        name_type=gssapi.NameType.hostbased_service
    ).canonicalize(gssapi.MechType.kerberos)
    log.debug('%s: GSSAPI name: %s', conn, gssapi_name)

    err = None
    close = False
    with conn._lock:
        if not conn._can_send_recv():
            err = Errors.NodeNotReadyError(str(conn))
            close = False
        else:
            # Establish security context and negotiate protection level
            # For reference RFC 2222, section 7.2.1
            try:
                # Exchange tokens until authentication either succeeds or fails
                client_ctx = gssapi.SecurityContext(name=gssapi_name, usage='initiate')
                received_token = None
                while not client_ctx.complete:
                    # calculate an output token from kafka token (or None if first iteration)
                    output_token = client_ctx.step(received_token)

                    # pass output token to kafka, or send empty response if the security
                    # context is complete (output token is None in that case)
                    if output_token is None:
                        conn._send_bytes_blocking(Int32.encode(0))
                    else:
                        msg = output_token
                        size = Int32.encode(len(msg))
                        conn._send_bytes_blocking(size + msg)

                    # The server will send a token back. Processing of this token either
                    # establishes a security context, or it needs further token exchange.
                    # The gssapi will be able to identify the needed next step.
                    # The connection is closed on failure.
                    header = conn._recv_bytes_blocking(4)
                    (token_size,) = struct.unpack('>i', header)
                    received_token = conn._recv_bytes_blocking(token_size)

                # Process the security layer negotiation token, sent by the server
                # once the security context is established.

                # unwraps message containing supported protection levels and msg size
                msg = client_ctx.unwrap(received_token).message
                # Kafka currently doesn't support integrity or confidentiality
                # security layers, so we simply set QoP to 'auth' only (first octet).
                # We reuse the max message size proposed by the server
                msg = Int8.encode(SASL_QOP_AUTH & Int8.decode(io.BytesIO(msg[0:1]))) + msg[1:]
                # add authorization identity to the response, GSS-wrap and send it
                msg = client_ctx.wrap(msg + auth_id.encode(), False).message
                size = Int32.encode(len(msg))
                conn._send_bytes_blocking(size + msg)

            except (ConnectionError, TimeoutError) as e:
                log.exception("%s: Error receiving reply from server",  conn)
                err = Errors.KafkaConnectionError(f"{conn}: {e}")
                close = True
            except Exception as e:
                err = e
                close = True

    if err is not None:
        if close:
            conn.close(error=err)
        return future.failure(err)

    log.info('%s: Authenticated as %s via GSSAPI', conn, gssapi_name)
    return future.success(True)
