import errno
import logging
import socket
import time
from urllib.parse import urlparse

import kafka.errors as Errors


log = logging.getLogger(__name__)


async def create_connection(net, host, port, socket_options=(), proxy_url=None, timeout_at=None):
    """Connect to host:port; raises KafkaConnectionError on failure"""
    socket_factory = KafkaNetSocket(proxy_url)
    addrs = socket_factory.dns_lookup(host, port)
    exceptions = [Errors.KafkaConnectionError('DNS Resolution failure')]
    for res in addrs:
        try:
            log.debug('%s: Attempting to connect to %s (options: %s)', socket_factory, res, socket_options)
            sock = await socket_factory.connect(net, res, socket_options, timeout_at=timeout_at)
        except (socket.error, OSError) as e:
            exceptions.append(Errors.KafkaConnectionError('unable to connect: %s' % (e,)))
            continue
        except Errors.KafkaTimeoutError:
            raise Errors.KafkaConnectionError('Connection timed out')
        except Errors.KafkaConnectionError as e:
            exceptions.append(e)
            continue
        else:
            return sock
    raise exceptions[-1]


class KafkaNetSocket:
    # scheme => handling class
    _registry = {}

    @classmethod
    def register_class(cls, klass):
        for scheme in klass.SCHEMES:
            cls._registry[scheme] = klass

    def __init_subclass__(cls, **kw):
        super().__init_subclass__(**kw)
        KafkaNetSocket.register_class(cls)

    def __new__(cls, proxy_url=None):
        if proxy_url is None:
            return super().__new__(cls)
        try:
            parsed = urlparse(proxy_url)
        except Exception:
            raise ValueError('Unable to parse proxy_url: %s' % (proxy_url,))
        if not parsed.scheme:
            raise ValueError('proxy_url requires scheme:// (%s)' % (proxy_url,))
        try:
            klass = KafkaNetSocket._registry[parsed.scheme]
        except KeyError:
            raise ValueError('Unsupported proxy url scheme: %s' % (parsed.scheme))
        return super().__new__(klass)

    def __init__(self, proxy_url=None):
        pass

    # simple sockets / no proxy
    def dns_lookup(self, host, port, raise_error=False):
        # XXX: all DNS functions in Python are blocking. If we really
        # want to be non-blocking here, we need to use a 3rd-party
        # library like python-adns, or move resolution onto its
        # own thread. This will be subject to the default libc
        # name resolution timeout (5s on most Linux boxes)
        try:
            return socket.getaddrinfo(host, port, socket.AF_UNSPEC, socket.SOCK_STREAM)
        except socket.gaierror as ex:
            err_str = "DNS lookup failed for %s:%d, %r" % (host, port, ex)
            if not raise_error:
                log.warning(err_str)
                return []
            raise Errors.KafkaConnectionError(err_str)

    def socket(self, family=socket.AF_UNSPEC, sock_type=socket.SOCK_STREAM, proto=socket.IPPROTO_TCP):
        return socket.socket(family, sock_type, proto)

    async def connect(self, net, addrinfo, socket_options=(), timeout_at=None):
        """Create non-blocking socket (with options) and connect to addrinfo tuple"""
        family, sock_type, proto, _canonname, sockaddr = addrinfo
        sock = self.socket(family, sock_type, proto)
        sock.setblocking(False)
        for option in socket_options:
            sock.setsockopt(*option)
        return await self.sock_connect(net, sock, sockaddr, timeout_at=timeout_at)

    async def sock_connect(self, net, sock, sockaddr, timeout_at=None):
        while timeout_at is None or time.monotonic() < timeout_at:
            ret = None
            try:
                ret = self.connect_ex(sock, sockaddr)
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
                await net.wait_write(sock, timeout_at=timeout_at)

            # Connection failed
            else:
                errstr = errno.errorcode.get(ret, 'UNKNOWN')
                raise Errors.KafkaConnectionError('{} {}'.format(ret, errstr))
        else:
            raise Errors.KafkaTimeoutError('Connection timed out')

    def connect_ex(self, sock, sockaddr):
        return sock.connect_ex(sockaddr)
