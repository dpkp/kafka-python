try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

import errno
import logging
import random
import socket
import struct

log = logging.getLogger(__name__)


class ProxyConnectionStates:
    DISCONNECTED = '<disconnected>'
    CONNECTING = '<connecting>'
    NEGOTIATE_PROPOSE = '<negotiate_propose>'
    NEGOTIATING = '<negotiating>'
    AUTHENTICATING = '<authenticating>'
    REQUEST_SUBMIT = '<request_submit>'
    REQUESTING = '<requesting>'
    READ_ADDRESS = '<read_address>'
    COMPLETE = '<complete>'


class Socks5Wrapper:
    """Socks5 proxy wrapper

    Manages connection through socks5 proxy with support for username/password
    authentication.
    """

    def __init__(self, proxy_url, afi):
        self._buffer_in = b''
        self._buffer_out = b''
        self._proxy_url = urlparse(proxy_url)
        self._sock = None
        self._state = ProxyConnectionStates.DISCONNECTED
        self._target_afi = socket.AF_UNSPEC

        proxy_addrs = self.dns_lookup(self._proxy_url.hostname, self._proxy_url.port, afi)
        # TODO raise error on lookup failure
        self._proxy_addr = random.choice(proxy_addrs)

    @classmethod
    def is_inet_4_or_6(cls, gai):
        """Given a getaddrinfo struct, return True iff ipv4 or ipv6"""
        return gai[0] in (socket.AF_INET, socket.AF_INET6)

    @classmethod
    def dns_lookup(cls, host, port, afi=socket.AF_UNSPEC):
        """Returns a list of getaddrinfo structs, optionally filtered to an afi (ipv4 / ipv6)"""
        # XXX: all DNS functions in Python are blocking. If we really
        # want to be non-blocking here, we need to use a 3rd-party
        # library like python-adns, or move resolution onto its
        # own thread. This will be subject to the default libc
        # name resolution timeout (5s on most Linux boxes)
        try:
            return list(filter(cls.is_inet_4_or_6,
                               socket.getaddrinfo(host, port, afi,
                                                  socket.SOCK_STREAM)))
        except socket.gaierror as ex:
            log.warning("DNS lookup failed for proxy %s:%d, %r", host, port, ex)
            return []

    def socket(self, family, sock_type):
        """Open and record a socket.

        Returns the actual underlying socket
        object to ensure e.g. selects and ssl wrapping works as expected.
        """
        self._target_afi = family  # Store the address family of the target
        afi, _, _, _, _ = self._proxy_addr
        self._sock = socket.socket(afi, sock_type)
        return self._sock

    def _flush_buf(self):
        """Send out all data that is stored in the outgoing buffer.

        It is expected that the caller handles error handling, including non-blocking
        as well as connection failure exceptions.
        """
        while self._buffer_out:
            sent_bytes = self._sock.send(self._buffer_out)
            self._buffer_out = self._buffer_out[sent_bytes:]

    def _peek_buf(self, datalen):
        """Ensure local inbound buffer has enough data, and return that data without
        consuming the local buffer

        It's expected that the caller handles e.g. blocking exceptions"""
        while True:
            bytes_remaining = datalen - len(self._buffer_in)
            if bytes_remaining <= 0:
                break
            data = self._sock.recv(bytes_remaining)
            if not data:
                break
            self._buffer_in = self._buffer_in + data

        return self._buffer_in[:datalen]

    def _read_buf(self, datalen):
        """Read and consume bytes from socket connection

        It's expected that the caller handles e.g. blocking exceptions"""
        buf = self._peek_buf(datalen)
        if buf:
            self._buffer_in = self._buffer_in[len(buf):]
        return buf

    def connect_ex(self, addr):
        """Runs a state machine through connection to authentication to
        proxy connection request.

        The somewhat strange setup is to facilitate non-intrusive use from
        BrokerConnection state machine.

        This function is called with a socket in non-blocking mode. Both
        send and receive calls can return in EWOULDBLOCK/EAGAIN which we
        specifically avoid handling here. These are handled in main
        BrokerConnection connection loop, which then would retry calls
        to this function."""

        if self._state == ProxyConnectionStates.DISCONNECTED:
            self._state = ProxyConnectionStates.CONNECTING

        if self._state == ProxyConnectionStates.CONNECTING:
            _, _, _, _, sockaddr = self._proxy_addr
            ret = self._sock.connect_ex(sockaddr)
            if not ret or ret == errno.EISCONN:
                self._state = ProxyConnectionStates.NEGOTIATE_PROPOSE
            else:
                return ret

        if self._state == ProxyConnectionStates.NEGOTIATE_PROPOSE:
            if self._proxy_url.username and self._proxy_url.password:
                # Propose username/password
                self._buffer_out = b"\x05\x01\x02"
            else:
                # Propose no auth
                self._buffer_out = b"\x05\x01\x00"
            self._state = ProxyConnectionStates.NEGOTIATING

        if self._state == ProxyConnectionStates.NEGOTIATING:
            self._flush_buf()
            buf = self._read_buf(2)
            if buf[0:1] != b"\x05":
                log.error("Unrecognized SOCKS version")
                self._state = ProxyConnectionStates.DISCONNECTED
                self._sock.close()
                return errno.ECONNREFUSED

            if buf[1:2] == b"\x00":
                # No authentication required
                self._state = ProxyConnectionStates.REQUEST_SUBMIT
            elif buf[1:2] == b"\x02":
                # Username/password authentication selected
                userlen = len(self._proxy_url.username)
                passlen = len(self._proxy_url.password)
                self._buffer_out = struct.pack(
                    "!bb{}sb{}s".format(userlen, passlen),
                    1,  # version
                    userlen,
                    self._proxy_url.username.encode(),
                    passlen,
                    self._proxy_url.password.encode(),
                )
                self._state = ProxyConnectionStates.AUTHENTICATING
            else:
                log.error("Unrecognized SOCKS authentication method")
                self._state = ProxyConnectionStates.DISCONNECTED
                self._sock.close()
                return errno.ECONNREFUSED

        if self._state == ProxyConnectionStates.AUTHENTICATING:
            self._flush_buf()
            buf = self._read_buf(2)
            if buf == b"\x01\x00":
                # Authentication succesful
                self._state = ProxyConnectionStates.REQUEST_SUBMIT
            else:
                log.error("Socks5 proxy authentication failure")
                self._state = ProxyConnectionStates.DISCONNECTED
                self._sock.close()
                return errno.ECONNREFUSED

        if self._state == ProxyConnectionStates.REQUEST_SUBMIT:
            if self._target_afi == socket.AF_INET:
                addr_type = 1
                addr_len = 4
            elif self._target_afi == socket.AF_INET6:
                addr_type = 4
                addr_len = 16
            else:
                log.error("Unknown address family, %r", self._target_afi)
                self._state = ProxyConnectionStates.DISCONNECTED
                self._sock.close()
                return errno.ECONNREFUSED

            self._buffer_out = struct.pack(
                "!bbbb{}sh".format(addr_len),
                5,  # version
                1,  # command: connect
                0,  # reserved
                addr_type,  # 1 for ipv4, 4 for ipv6 address
                socket.inet_pton(self._target_afi, addr[0]),  # either 4 or 16 bytes of actual address
                addr[1],  # port
            )
            self._state = ProxyConnectionStates.REQUESTING

        if self._state == ProxyConnectionStates.REQUESTING:
            self._flush_buf()
            buf = self._read_buf(2)
            if buf[0:2] == b"\x05\x00":
                self._state = ProxyConnectionStates.READ_ADDRESS
            else:
                log.error("Proxy request failed: %r", buf[1:2])
                self._state = ProxyConnectionStates.DISCONNECTED
                self._sock.close()
                return errno.ECONNREFUSED

        if self._state == ProxyConnectionStates.READ_ADDRESS:
            # we don't really care about the remote endpoint address, but need to clear the stream
            buf = self._peek_buf(2)
            if buf[0:2] == b"\x00\x01":
                _ = self._read_buf(2 + 4 + 2)  # ipv4 address + port
            elif buf[0:2] == b"\x00\x05":
                _ = self._read_buf(2 + 16 + 2)  # ipv6 address + port
            else:
                log.error("Unrecognized remote address type %r", buf[1:2])
                self._state = ProxyConnectionStates.DISCONNECTED
                self._sock.close()
                return errno.ECONNREFUSED
            self._state = ProxyConnectionStates.COMPLETE

        if self._state == ProxyConnectionStates.COMPLETE:
            return 0

        # not reached;
        # Send and recv will raise socket error on EWOULDBLOCK/EAGAIN that is assumed to be handled by
        # the caller. The caller re-enters this state machine from retry logic with timer or via select & family
        log.error("Internal error, state %r not handled correctly", self._state)
        self._state = ProxyConnectionStates.DISCONNECTED
        if self._sock:
            self._sock.close()
        return errno.ECONNREFUSED
