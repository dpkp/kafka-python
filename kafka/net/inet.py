import errno
import logging
import socket

import kafka.errors as Errors
from kafka.socks5_wrapper import Socks5Wrapper


log = logging.getLogger(__name__)


async def create_connection(net, host, port, socket_options=(), socks5_proxy=None):
    """Connect to host:port; raises KafkaConnectionError on failure"""
    if socks5_proxy and Socks5Wrapper.use_remote_lookup(socks5_proxy):
        addrs = [(socket.AF_UNSPEC, socket.SOCK_STREAM, 0, '', (host, port))]
    else:
        addrs = dns_lookup(host, port)

    exceptions = [Errors.KafkaConnectionError('DNS Resolution failure')]
    for res in addrs:
        af, _socktype, _proto, _canonname, sa = res
        try:
            proxy = Socks5Wrapper(socks5_proxy, af) if socks5_proxy else None
            sock = (proxy or socket).socket(af, socket.SOCK_STREAM)
            sock.setblocking(False)
            for option in socket_options:
                sock.setsockopt(*option)
            log.debug('Attempting to connect %s -> %s (proxy=%s)', sock, sa, proxy)
            sock = await connect_sock(net, sock, sa, proxy=proxy)
        except (socket.error, OSError) as e:
            exceptions.append(Errors.KafkaConnectionError('unable to connect: %s' % (e,)))
            continue
        except Errors.KafkaConnectionError as e:
            exceptions.append(e)
            continue
        else:
            return sock
    raise exceptions[-1]


async def connect_sock(net, sock, sockaddr, proxy=None):
    while True:
        ret = None
        try:
            ret = (proxy or sock).connect_ex(sockaddr)
        except BlockingIOError:
            ret = errno.EWOULDBLOCK
        except socket.error as err:
            ret = err.errno

        # Connection succeeded
        if not ret or ret == errno.EISCONN:
            log.debug('Connected: %s', sock)
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
