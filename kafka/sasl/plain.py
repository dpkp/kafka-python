import logging

import kafka.errors as Errors
from kafka.protocol.types import Int32

log = logging.getLogger(__name__)


def validate_config(conn):
    assert conn.config['sasl_plain_username'] is not None, (
        'sasl_plain_username required when sasl_mechanism=PLAIN'
    )
    assert conn.config['sasl_plain_password'] is not None, (
        'sasl_plain_password required when sasl_mechanism=PLAIN'
    )


def try_authenticate(conn, future):
    if conn.config['security_protocol'] == 'SASL_PLAINTEXT':
        log.warning('%s: Sending username and password in the clear', conn)

    data = b''
    # Send PLAIN credentials per RFC-4616
    msg = bytes('\0'.join([conn.config['sasl_plain_username'],
                           conn.config['sasl_plain_username'],
                           conn.config['sasl_plain_password']]).encode('utf-8'))
    size = Int32.encode(len(msg))

    err = None
    close = False
    with conn._lock:
        if not conn._can_send_recv():
            err = Errors.NodeNotReadyError(str(conn))
            close = False
        else:
            try:
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

    log.info('%s: Authenticated as %s via PLAIN', conn, conn.config['sasl_plain_username'])
    return future.success(True)
