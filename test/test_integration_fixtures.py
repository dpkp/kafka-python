import pytest

from test.integration import fixtures


class PortSocket:
    def __init__(self, port, sockets):
        self.port = port
        self.sockets = sockets
        self.bound = False
        self.closed = False

    def bind(self, address):
        assert address == ('127.0.0.1', 0)
        assert not any(sock.bound and not sock.closed and sock.port == self.port
                       for sock in self.sockets)
        self.bound = True

    def getsockname(self):
        return ('127.0.0.1', self.port)

    def close(self):
        self.closed = True


def port_sockets(monkeypatch, ports):
    sockets = []
    ports = iter(ports)

    def socket():
        sock = PortSocket(next(ports), sockets)
        sockets.append(sock)
        return sock

    monkeypatch.setattr(fixtures.socket, 'socket', socket)
    return sockets


def test_get_open_port_keeps_excluded_ports_bound_until_selection(monkeypatch):
    sockets = port_sockets(monkeypatch, [12345, 23456])
    original_getsockname = PortSocket.getsockname

    def getsockname(self):
        if self.port == 23456:
            assert not sockets[0].closed
        return original_getsockname(self)

    monkeypatch.setattr(PortSocket, 'getsockname', getsockname)
    assert fixtures.get_open_port(exclude=(12345,)) == 23456
    assert all(sock.closed for sock in sockets)


def test_get_open_port_closes_socket_when_bind_fails(monkeypatch):
    sockets = port_sockets(monkeypatch, [12345])

    def fail_bind(self, address):
        raise OSError('bind failed')

    monkeypatch.setattr(PortSocket, 'bind', fail_bind)
    with pytest.raises(OSError, match='bind failed'):
        fixtures.get_open_port()
    assert all(sock.closed for sock in sockets)


@pytest.mark.parametrize('port,ports', [
    (None, [12345, 12345, 23456]),
    (12345, [12345, 23456]),
])
def test_broker_and_controller_ports_are_distinct(monkeypatch, tmp_path, port, ports):
    sockets = port_sockets(monkeypatch, ports)
    monkeypatch.setattr(fixtures, 'env_kafka_version', lambda: (4, 3, 0))
    monkeypatch.setattr(fixtures.Fixture, 'kafka_root', str(tmp_path))
    broker = fixtures.KafkaFixture('localhost', port, 0)
    assert broker.port == 12345
    assert broker.controller_port == 23456
    assert all(sock.closed for sock in sockets)
