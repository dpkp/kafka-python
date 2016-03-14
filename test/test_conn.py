# pylint: skip-file
from __future__ import absolute_import

from errno import EALREADY, EINPROGRESS, EISCONN, ECONNRESET
import socket
import time

import pytest

from kafka.conn import BrokerConnection, ConnectionStates


@pytest.fixture
def socket(mocker):
    socket = mocker.MagicMock()
    socket.connect_ex.return_value = 0
    mocker.patch('socket.socket', return_value=socket)
    return socket


@pytest.fixture
def conn(socket):
    conn = BrokerConnection('localhost', 9092, socket.AF_INET)
    return conn


@pytest.mark.parametrize("states", [
  (([EINPROGRESS, EALREADY], ConnectionStates.CONNECTING),),
  (([EALREADY, EALREADY], ConnectionStates.CONNECTING),),
  (([0], ConnectionStates.CONNECTED),),
  (([EINPROGRESS, EALREADY], ConnectionStates.CONNECTING),
   ([ECONNRESET], ConnectionStates.DISCONNECTED)),
  (([EINPROGRESS, EALREADY], ConnectionStates.CONNECTING),
   ([EALREADY], ConnectionStates.CONNECTING),
   ([EISCONN], ConnectionStates.CONNECTED)),
])
def test_connect(socket, conn, states):
    assert conn.state is ConnectionStates.DISCONNECTED

    for errno, state in states:
        socket.connect_ex.side_effect = errno
        conn.connect()
        assert conn.state is state


def test_connect_timeout(socket, conn):
    assert conn.state is ConnectionStates.DISCONNECTED

    # Initial connect returns EINPROGRESS
    # immediate inline connect returns EALREADY
    # second explicit connect returns EALREADY
    # third explicit connect returns EALREADY and times out via last_attempt
    socket.connect_ex.side_effect = [EINPROGRESS, EALREADY, EALREADY, EALREADY]
    conn.connect()
    assert conn.state is ConnectionStates.CONNECTING
    conn.connect()
    assert conn.state is ConnectionStates.CONNECTING
    conn.last_attempt = 0
    conn.connect()
    assert conn.state is ConnectionStates.DISCONNECTED


def test_blacked_out(conn):
    assert not conn.blacked_out()
    conn.last_attempt = time.time()
    assert conn.blacked_out()


def test_connected(conn):
    assert not conn.connected()
    conn.state = ConnectionStates.CONNECTED
    assert conn.connected()


def test_connecting(conn):
    assert not conn.connecting()
    conn.state = ConnectionStates.CONNECTING
    assert conn.connecting()
    conn.state = ConnectionStates.CONNECTED
    assert not conn.connecting()

# TODO: test_send, test_recv, test_can_send_more, test_close
