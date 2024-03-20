import logging
import struct

import kafka.errors as Errors
from kafka.protocol.types import Int32
from kafka.scram import ScramClient

log = logging.getLogger()


def validate_config(conn):
    assert conn.config['sasl_plain_username'] is not None, (
        'sasl_plain_username required when sasl_mechanism=SCRAM-*'
    )
    assert conn.config['sasl_plain_password'] is not None, (
        'sasl_plain_password required when sasl_mechanism=SCRAM-*'
    )


def try_authenticate(conn, future):
    if conn.config['security_protocol'] == 'SASL_PLAINTEXT':
        log.warning('%s: Exchanging credentials in the clear', conn)

    scram_client = ScramClient(
        conn.config['sasl_plain_username'],
        conn.config['sasl_plain_password'],
        conn.config['sasl_mechanism'],
    )

    err = None
    close = False
    with conn._lock:
        if not conn._can_send_recv():
            err = Errors.NodeNotReadyError(str(conn))
            close = False
        else:
            try:
                client_first = scram_client.first_message().encode('utf-8')
                size = Int32.encode(len(client_first))
                conn._send_bytes_blocking(size + client_first)

                (data_len,) = struct.unpack('>i', conn._recv_bytes_blocking(4))
                server_first = conn._recv_bytes_blocking(data_len).decode('utf-8')
                scram_client.process_server_first_message(server_first)

                client_final = scram_client.final_message().encode('utf-8')
                size = Int32.encode(len(client_final))
                conn._send_bytes_blocking(size + client_final)

                (data_len,) = struct.unpack('>i', conn._recv_bytes_blocking(4))
                server_final = conn._recv_bytes_blocking(data_len).decode('utf-8')
                scram_client.process_server_final_message(server_final)

            except (ConnectionError, TimeoutError) as e:
                log.exception("%s: Error receiving reply from server", conn)
                err = Errors.KafkaConnectionError(f"{conn}: {e}")
                close = True

    if err is not None:
        if close:
            conn.close(error=err)
        return future.failure(err)

    log.info(
        '%s: Authenticated as %s via %s',
        conn, conn.config['sasl_plain_username'], conn.config['sasl_mechanism']
    )
    return future.success(True)
