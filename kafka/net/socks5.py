import logging
import socket
import struct

from kafka.errors import KafkaConnectionError
from kafka.net.proxy import KafkaTCPProxy, KafkaTCPProxyStates


log = logging.getLogger(__name__)



class Socks5ProxyProtocol(KafkaTCPProxy):
    """Socks5 proxy sans-IO protocol handler."""

    # socks5h for remote dns
    SCHEMES = ('socks5', 'socks5h')

    def use_remote_lookup(self):
        return self.proxy_scheme == 'socks5h'

    def _read_buf(self, num_bytes):
        if len(self._buf) < num_bytes:
            raise IndexError('Not enought bytes in buffer')
        data, self._buf = self._buf[0:num_bytes], self._buf[num_bytes:]
        return data

    def _run_state_machine(self):
        if self._state in (KafkaTCPProxyStates.DISCONNECTED, KafkaTCPProxyStates.COMPLETE):
            return False

        if self._state == KafkaTCPProxyStates.CONNECTING:
            if self._proxy_url.username and self._proxy_url.password:
                # Propose username/password
                self._transport.write(b"\x05\x01\x02")
            else:
                # Propose no auth
                self._transport.write(b"\x05\x01\x00")
            self._state = KafkaTCPProxyStates.NEGOTIATING

        if self._state == KafkaTCPProxyStates.NEGOTIATING:
            try:
                buf = self._read_buf(2)
            except IndexError:
                return False

            if buf[0:1] != b"\x05":
                log.error("Unrecognized SOCKS version")
                raise KafkaConnectionError('Unrecognized SOCKS version')

            if buf[1:2] == b"\x00":
                # No authentication required
                self._state = KafkaTCPProxyStates.REQUEST_SUBMIT
            elif buf[1:2] == b"\x02":
                # Username/password authentication selected
                userlen = len(self._proxy_url.username)
                passlen = len(self._proxy_url.password)
                self._transport.write(struct.pack(
                    "!bb{}sb{}s".format(userlen, passlen),
                    1,  # version
                    userlen,
                    self._proxy_url.username.encode(),
                    passlen,
                    self._proxy_url.password.encode(),
                ))
                self._state = KafkaTCPProxyStates.AUTHENTICATING
            else:
                log.error("Unrecognized SOCKS authentication method")
                raise KafkaConnectionError('Unrecognized SOCKS authentication method')

        if self._state == KafkaTCPProxyStates.AUTHENTICATING:
            try:
                buf = self._read_buf(2)
            except IndexError:
                return False
            if buf == b"\x01\x00":
                # Authentication succesful
                self._state = KafkaTCPProxyStates.REQUEST_SUBMIT
            else:
                log.error("Socks5 proxy authentication failure")
                raise KafkaConnectionError('Socks5 proxy authentication failure')

        if self._state == KafkaTCPProxyStates.REQUEST_SUBMIT:
            if self.use_remote_lookup():
                addr_type = 3
                addr_len = len(self._host)
            elif self._afi == socket.AF_INET:
                addr_type = 1
                addr_len = 4
            elif self._afi == socket.AF_INET6:
                addr_type = 4
                addr_len = 16
            else:
                log.error("Unknown address family, %r", self._afi)
                raise KafkaConnectionError("Unknown address family, %r" % self._afi)

            self._transport.write(struct.pack(
                "!bbbb",
                5,  # version
                1,  # command: connect
                0,  # reserved
                addr_type,  # 1 for ipv4, 4 for ipv6 address, 3 for domain name
            ))
            # Addr format depends on type
            if addr_type == 3:
                # len + domain name (no null terminator)
                self._transport.write(struct.pack(
                    "!b{}s".format(addr_len),
                    addr_len,
                    self._host.encode('ascii'),
                ))
            else:
                # either 4 (type 1) or 16 (type 4) bytes of actual address
                self._transport.write(struct.pack(
                    "!{}s".format(addr_len),
                    socket.inet_pton(self._afi, self._host),
                ))
            self._transport.write(struct.pack("!H", self._port))  # port
            self._state = KafkaTCPProxyStates.REQUESTING

        if self._state == KafkaTCPProxyStates.REQUESTING:
            try:
                buf = self._read_buf(2)
            except IndexError:
                return False
            if buf[0:2] == b"\x05\x00":
                self._state = KafkaTCPProxyStates.READ_ADDRESS
            else:
                log.error("Proxy request failed: %r", buf[1:2])
                raise KafkaConnectionError("Proxy request failed: %r" % buf[1:2])

        if self._state == KafkaTCPProxyStates.READ_ADDRESS:
            # we don't really care about the remote endpoint address, but need to clear the stream
            if len(self._buf) < 2:
                return False
            if self._buf[0:2] == b"\x00\x01":
                addrbytes = 4
            elif self._buf[0:2] == b"\x00\x05":
                addrbytes = 16
            else:
                log.error("Unrecognized remote address type %r", buf[1:2])
                raise KafkaConnectionError("Unrecognized remote address type %r", self._buf[1:2])
            try:
                self._read_buf(2 + addrbytes + 2)  # header + address + port
            except IndexError:
                return False
            self._state = KafkaTCPProxyStates.COMPLETE
            assert not self._buf, 'unexpected bytes remaining in buffer'

        if self._state == KafkaTCPProxyStates.COMPLETE:
            return True

        # not reached;
        # Send and recv will raise socket error on EWOULDBLOCK/EAGAIN that is assumed to be handled by
        # the caller. The caller re-enters this state machine from retry logic with timer or via select & family
        log.error("Internal error, state %r not handled correctly", self._state)
        raise KafkaConnectionError("Internal Socks5 proxy state error (%r)" % (self._state,))
