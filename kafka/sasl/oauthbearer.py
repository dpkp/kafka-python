import logging

import kafka.errors as Errors
from kafka.protocol.types import Int32

log = logging.getLogger(__name__)


def validate_config(conn):
    token_provider = conn.config.get('sasl_oauth_token_provider')
    assert token_provider is not None, (
        'sasl_oauth_token_provider required when sasl_mechanism=OAUTHBEARER'
    )
    assert callable(getattr(token_provider, 'token', None)), (
        'sasl_oauth_token_provider must implement method #token()'
    )


def try_authenticate(conn, future):
    data = b''

    msg = bytes(_build_oauth_client_request(conn).encode("utf-8"))
    size = Int32.encode(len(msg))

    err = None
    close = False
    with conn._lock:
        if not conn._can_send_recv():
            err = Errors.NodeNotReadyError(str(conn))
            close = False
        else:
            try:
                # Send SASL OAuthBearer request with OAuth token
                conn._send_bytes_blocking(size + msg)

                # The server will send a zero sized message (that is Int32(0)) on success.
                # The connection is closed on failure
                data = conn._recv_bytes_blocking(4)

            except (ConnectionError, TimeoutError) as e:
                log.exception("%s: Error receiving reply from server", conn)
                err = Errors.KafkaConnectionError(f"{conn}: {e}")
                close = True

    if err is not None:
        if close:
            conn.close(error=err)
        return future.failure(err)

    if data != b'\x00\x00\x00\x00':
        error = Errors.AuthenticationFailedError('Unrecognized response during authentication')
        return future.failure(error)

    log.info('%s: Authenticated via OAuth', conn)
    return future.success(True)


def _build_oauth_client_request(conn):
    token_provider = conn.config['sasl_oauth_token_provider']
    return "n,,\x01auth=Bearer {}{}\x01\x01".format(
        token_provider.token(),
        _token_extensions(conn),
    )


def _token_extensions(conn):
    """
    Return a string representation of the OPTIONAL key-value pairs that can be
    sent with an OAUTHBEARER initial request.
    """
    token_provider = conn.config['sasl_oauth_token_provider']

    # Only run if the #extensions() method is implemented by the clients Token Provider class
    # Builds up a string separated by \x01 via a dict of key value pairs
    if (callable(getattr(token_provider, "extensions", None))
            and len(token_provider.extensions()) > 0):
        msg = "\x01".join([f"{k}={v}" for k, v in token_provider.extensions().items()])
        return "\x01" + msg
    else:
        return ""
