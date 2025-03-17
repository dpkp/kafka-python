import errno
import logging
import socket

import kafka.errors as Errors


log = logging.getLogger(__name__)


async def create_connection(net, host, port, socket_options=()):
    """Connect to host:port; raises KafkaConnectionError on failure"""
    exceptions = [Errors.KafkaConnectionError('DNS Resolution failure')]
    for res in dns_lookup(host, port):
        af, _socktype, _proto, _canonname, sa = res
        # TODO: socks5 proxy
        try:
            sock = socket.socket(af, socket.SOCK_STREAM)
            for option in socket_options:
                sock.setsockopt(*option)
            sock.setblocking(False)
        except (socket.error, OSError) as e:
            exceptions.append(Errors.KafkaConnectionError('unable to initialize socket object: %s' % (e,)))
            continue
        try:
            sock = await connect_sock(net, sock, sa)
        except Errors.KafkaConnectionError as e:
            exceptions.append(e)
        else:
            exceptions = []
            return sock
    else:
        raise exceptions[-1]


async def connect_sock(net, sock, sockaddr):
    while True:
        ret = None
        try:
            ret = sock.connect_ex(sockaddr)
        except socket.error as err:
            ret = err.errno

        # Connection succeeded
        if not ret or ret == errno.EISCONN:
            log.debug('Established TCP connection to %s', sockaddr)
            return sock

        # Needs retry
        # WSAEINVAL == 10022, but errno.WSAEINVAL is not available on non-win systems
        elif ret in (errno.EINPROGRESS, errno.EALREADY, errno.EWOULDBLOCK, 10022):
            await net.wait_write(sock)

        # Connection failed
        else:
            errstr = errno.errorcode.get(ret, 'UNKNOWN')
            raise Errors.KafkaConnectionError('{} {}'.format(ret, errstr))


def dns_lookup(host, port):
    # currently blocking...
    try:
        return socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM)
    except socket.gaierror:
        return []
