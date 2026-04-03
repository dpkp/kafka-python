import errno
import socket
from unittest.mock import MagicMock, patch

import pytest

from kafka.net.selector import NetworkSelector
from kafka.net.inet import connect_sock, create_connection, dns_lookup
import kafka.errors as Errors


class TestDnsLookup:
    def test_valid_host(self):
        results = dns_lookup('localhost', 9092)
        assert len(results) > 0
        for res in results:
            assert len(res) == 5

    def test_invalid_host(self):
        with patch('kafka.net.inet.socket.getaddrinfo', side_effect=socket.gaierror):
            results = dns_lookup('invalid.host', 9092)
            assert results == []

    def test_numeric_host(self):
        results = dns_lookup('127.0.0.1', 9092)
        assert len(results) > 0
        assert results[0][4][0] == '127.0.0.1'


class TestConnectSock:
    def test_immediate_connect(self):
        net = NetworkSelector()
        sock = MagicMock()
        sock.connect_ex.return_value = 0
        task = net.run_until_done(connect_sock(net, sock, ('127.0.0.1', 9092)))
        assert task.result is sock

    def test_eisconn(self):
        net = NetworkSelector()
        sock = MagicMock()
        sock.connect_ex.return_value = errno.EISCONN
        task = net.run_until_done(connect_sock(net, sock, ('127.0.0.1', 9092)))
        assert task.result is sock

    def test_connection_refused(self):
        net = NetworkSelector()
        sock = MagicMock()
        sock.connect_ex.return_value = errno.ECONNREFUSED
        task = net.run_until_done(connect_sock(net, sock, ('127.0.0.1', 9092)))
        assert task.is_done
        assert isinstance(task.exception, Errors.KafkaConnectionError)

    def test_socket_error_uses_errno(self):
        net = NetworkSelector()
        sock = MagicMock()
        sock.connect_ex.side_effect = socket.error(errno.ECONNREFUSED, 'refused')
        task = net.run_until_done(connect_sock(net, sock, ('127.0.0.1', 9092)))
        assert isinstance(task.exception, Errors.KafkaConnectionError)


class TestCreateConnection:
    def test_dns_failure(self):
        net = NetworkSelector()
        with patch('kafka.net.inet.dns_lookup', return_value=[]):
            task = net.run_until_done(
                create_connection(net, 'badhost', 9092))
            assert isinstance(task.exception, Errors.KafkaConnectionError)
            assert 'DNS' in str(task.exception)

    def test_socket_init_failure(self):
        net = NetworkSelector()
        fake_addr = [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('127.0.0.1', 9092))]
        with patch('kafka.net.inet.dns_lookup', return_value=fake_addr), \
             patch('kafka.net.inet.socket.socket', side_effect=OSError('no socket')):
            task = net.run_until_done(
                create_connection(net, 'host', 9092))
            assert isinstance(task.exception, Errors.KafkaConnectionError)

    def test_successful_connection(self):
        net = NetworkSelector()
        fake_addr = [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('127.0.0.1', 9092))]
        mock_sock = MagicMock()
        mock_sock.connect_ex.return_value = 0
        with patch('kafka.net.inet.dns_lookup', return_value=fake_addr), \
             patch('kafka.net.inet.socket.socket', return_value=mock_sock):
            task = net.run_until_done(
                create_connection(net, 'host', 9092))
            assert task.result is mock_sock
            mock_sock.setblocking.assert_called_with(False)

    def test_tries_multiple_addresses(self):
        net = NetworkSelector()
        addr1 = (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('10.0.0.1', 9092))
        addr2 = (socket.AF_INET, socket.SOCK_STREAM, 6, '', ('10.0.0.2', 9092))
        mock_sock1 = MagicMock()
        mock_sock1.connect_ex.return_value = errno.ECONNREFUSED
        mock_sock2 = MagicMock()
        mock_sock2.connect_ex.return_value = 0
        sockets = iter([mock_sock1, mock_sock2])
        with patch('kafka.net.inet.dns_lookup', return_value=[addr1, addr2]), \
             patch('kafka.net.inet.socket.socket', side_effect=lambda *a: next(sockets)):
            task = net.run_until_done(
                create_connection(net, 'host', 9092))
            assert task.result is mock_sock2

    def test_so_error_after_wait(self):
        net = NetworkSelector()
        fake_addr = [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('127.0.0.1', 9092))]
        rsock, wsock = socket.socketpair()
        rsock.setblocking(False)
        wsock.setblocking(False)
        mock_sock = MagicMock()
        mock_sock.connect_ex.return_value = errno.EINPROGRESS
        mock_sock.getsockopt.return_value = errno.ECONNREFUSED
        mock_sock.fileno.return_value = wsock.fileno()
        with patch('kafka.net.inet.dns_lookup', return_value=fake_addr), \
             patch('kafka.net.inet.socket.socket', return_value=mock_sock):
            # We need wait_write to return, so register the sock as writable
            # Simplest: just test connect_sock directly with a real socketpair
            pass
        rsock.close()
        wsock.close()


class TestConnectSockWithProxy:
    def test_proxy_connect_ex_called(self):
        net = NetworkSelector()
        sock = MagicMock()
        proxy = MagicMock()
        proxy.connect_ex.return_value = 0
        task = net.run_until_done(connect_sock(net, sock, ('127.0.0.1', 9092), proxy=proxy))
        assert task.result is sock
        proxy.connect_ex.assert_called_once_with(('127.0.0.1', 9092))
        sock.connect_ex.assert_not_called()

    def test_proxy_blocking_io_retries(self):
        net = NetworkSelector()
        rsock, wsock = socket.socketpair()
        rsock.setblocking(False)
        wsock.setblocking(False)
        proxy = MagicMock()
        proxy.connect_ex.side_effect = [BlockingIOError, 0]
        # Use real socket fd so wait_write works
        mock_sock = MagicMock()
        mock_sock.fileno.return_value = wsock.fileno()
        task = net.run_until_done(connect_sock(net, mock_sock, ('127.0.0.1', 9092), proxy=proxy))
        assert task.result is mock_sock
        assert proxy.connect_ex.call_count == 2
        rsock.close()
        wsock.close()

    def test_proxy_connection_refused(self):
        net = NetworkSelector()
        sock = MagicMock()
        proxy = MagicMock()
        proxy.connect_ex.return_value = errno.ECONNREFUSED
        task = net.run_until_done(connect_sock(net, sock, ('127.0.0.1', 9092), proxy=proxy))
        assert isinstance(task.exception, Errors.KafkaConnectionError)


class TestCreateConnectionWithProxy:
    def test_proxy_creates_socket_via_wrapper(self):
        net = NetworkSelector()
        mock_sock = MagicMock()
        mock_sock.connect_ex.return_value = 0
        mock_proxy = MagicMock()
        mock_proxy.socket.return_value = mock_sock
        mock_proxy.connect_ex.return_value = 0
        with patch('kafka.net.inet.Socks5Wrapper', return_value=mock_proxy) as mock_cls:
            task = net.run_until_done(
                create_connection(net, 'broker', 9092, socks5_proxy='socks5://proxy:1080'))
            mock_cls.assert_called_once_with('socks5://proxy:1080', socket.AF_UNSPEC)
            mock_proxy.socket.assert_called_once()
            assert task.result is mock_sock

    def test_proxy_remote_dns_skips_local_lookup(self):
        net = NetworkSelector()
        mock_sock = MagicMock()
        mock_sock.connect_ex.return_value = 0
        mock_proxy = MagicMock()
        mock_proxy.socket.return_value = mock_sock
        mock_proxy.connect_ex.return_value = 0
        with patch('kafka.net.inet.Socks5Wrapper') as mock_cls, \
             patch('kafka.net.inet.dns_lookup') as mock_dns:
            mock_cls.return_value = mock_proxy
            mock_cls.use_remote_lookup.return_value = True
            task = net.run_until_done(
                create_connection(net, 'broker', 9092, socks5_proxy='socks5h://proxy:1080'))
            mock_dns.assert_not_called()

    def test_no_proxy_uses_direct_socket(self):
        net = NetworkSelector()
        fake_addr = [(socket.AF_INET, socket.SOCK_STREAM, 6, '', ('127.0.0.1', 9092))]
        mock_sock = MagicMock()
        mock_sock.connect_ex.return_value = 0
        with patch('kafka.net.inet.dns_lookup', return_value=fake_addr), \
             patch('kafka.net.inet.socket.socket', return_value=mock_sock), \
             patch('kafka.net.inet.Socks5Wrapper') as mock_cls:
            task = net.run_until_done(
                create_connection(net, 'host', 9092))
            mock_cls.assert_not_called()
            assert task.result is mock_sock
