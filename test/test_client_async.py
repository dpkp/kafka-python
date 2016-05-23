# selectors in stdlib as of py3.4
try:
    import selectors # pylint: disable=import-error
except ImportError:
    # vendored backport module
    import kafka.selectors34 as selectors

import socket
import time

import pytest

from kafka.client_async import KafkaClient
from kafka.conn import ConnectionStates
import kafka.errors as Errors
from kafka.future import Future
from kafka.protocol.metadata import MetadataResponse, MetadataRequest
from kafka.protocol.produce import ProduceRequest
from kafka.structs import BrokerMetadata


@pytest.mark.parametrize("bootstrap,expected_hosts", [
    (None, [('localhost', 9092, socket.AF_UNSPEC)]),
    ('foobar:1234', [('foobar', 1234, socket.AF_UNSPEC)]),
    ('fizzbuzz', [('fizzbuzz', 9092, socket.AF_UNSPEC)]),
    ('foo:12,bar:34', [('foo', 12, socket.AF_UNSPEC), ('bar', 34, socket.AF_UNSPEC)]),
    (['fizz:56', 'buzz'], [('fizz', 56, socket.AF_UNSPEC), ('buzz', 9092, socket.AF_UNSPEC)]),
])
def test_bootstrap_servers(mocker, bootstrap, expected_hosts):
    mocker.patch.object(KafkaClient, '_bootstrap')
    if bootstrap is None:
        KafkaClient()
    else:
        KafkaClient(bootstrap_servers=bootstrap)

    # host order is randomized internally, so resort before testing
    (hosts,), _ = KafkaClient._bootstrap.call_args  # pylint: disable=no-member
    assert sorted(hosts) == sorted(expected_hosts)


def test_bootstrap_success(conn):
    conn.state = ConnectionStates.CONNECTED
    cli = KafkaClient()
    args, kwargs = conn.call_args
    assert args == ('localhost', 9092, socket.AF_UNSPEC)
    kwargs.pop('state_change_callback')
    assert kwargs == cli.config
    conn.connect.assert_called_with()
    conn.send.assert_called_once_with(MetadataRequest[0]([]))
    assert cli._bootstrap_fails == 0
    assert cli.cluster.brokers() == set([BrokerMetadata(0, 'foo', 12),
                                         BrokerMetadata(1, 'bar', 34)])

def test_bootstrap_failure(conn):
    conn.state = ConnectionStates.DISCONNECTED
    cli = KafkaClient()
    args, kwargs = conn.call_args
    assert args == ('localhost', 9092, socket.AF_UNSPEC)
    kwargs.pop('state_change_callback')
    assert kwargs == cli.config
    conn.connect.assert_called_with()
    conn.close.assert_called_with()
    assert cli._bootstrap_fails == 1
    assert cli.cluster.brokers() == set()


def test_can_connect(conn):
    cli = KafkaClient()

    # Node is not in broker metadata - cant connect
    assert not cli._can_connect(2)

    # Node is in broker metadata but not in _conns
    assert 0 not in cli._conns
    assert cli._can_connect(0)

    # Node is connected, can't reconnect
    assert cli._maybe_connect(0) is True
    assert not cli._can_connect(0)

    # Node is disconnected, can connect
    cli._conns[0].state = ConnectionStates.DISCONNECTED
    assert cli._can_connect(0)

    # Node is disconnected, but blacked out
    conn.blacked_out.return_value = True
    assert not cli._can_connect(0)

def test_maybe_connect(conn):
    cli = KafkaClient()
    try:
        # Node not in metadata, raises AssertionError
        cli._maybe_connect(2)
    except AssertionError:
        pass
    else:
        assert False, 'Exception not raised'

    # New node_id creates a conn object
    assert 0 not in cli._conns
    conn.state = ConnectionStates.DISCONNECTED
    conn.connect.side_effect = lambda: conn._set_conn_state(ConnectionStates.CONNECTING)
    assert cli._maybe_connect(0) is False
    assert cli._conns[0] is conn


def test_conn_state_change(mocker, conn):
    cli = KafkaClient()
    sel = mocker.patch.object(cli, '_selector')

    node_id = 0
    conn.state = ConnectionStates.CONNECTING
    cli._conn_state_change(node_id, conn)
    assert node_id in cli._connecting
    sel.register.assert_called_with(conn._sock, selectors.EVENT_WRITE)

    conn.state = ConnectionStates.CONNECTED
    cli._conn_state_change(node_id, conn)
    assert node_id not in cli._connecting
    sel.unregister.assert_called_with(conn._sock)
    sel.register.assert_called_with(conn._sock, selectors.EVENT_READ, conn)

    # Failure to connect should trigger metadata update
    assert cli.cluster._need_update is False
    conn.state = ConnectionStates.DISCONNECTING
    cli._conn_state_change(node_id, conn)
    assert node_id not in cli._connecting
    assert cli.cluster._need_update is True
    sel.unregister.assert_called_with(conn._sock)

    conn.state = ConnectionStates.CONNECTING
    cli._conn_state_change(node_id, conn)
    assert node_id in cli._connecting
    conn.state = ConnectionStates.DISCONNECTING
    cli._conn_state_change(node_id, conn)
    assert node_id not in cli._connecting


def test_ready(mocker, conn):
    cli = KafkaClient()
    maybe_connect = mocker.patch.object(cli, '_maybe_connect')
    node_id = 1
    cli.ready(node_id)
    maybe_connect.assert_called_with(node_id)


def test_is_ready(mocker, conn):
    cli = KafkaClient()
    cli._maybe_connect(0)
    cli._maybe_connect(1)

    # metadata refresh blocks ready nodes
    assert cli.is_ready(0)
    assert cli.is_ready(1)
    cli._metadata_refresh_in_progress = True
    assert not cli.is_ready(0)
    assert not cli.is_ready(1)

    # requesting metadata update also blocks ready nodes
    cli._metadata_refresh_in_progress = False
    assert cli.is_ready(0)
    assert cli.is_ready(1)
    cli.cluster.request_update()
    cli.cluster.config['retry_backoff_ms'] = 0
    assert not cli._metadata_refresh_in_progress
    assert not cli.is_ready(0)
    assert not cli.is_ready(1)
    cli.cluster._need_update = False

    # if connection can't send more, not ready
    assert cli.is_ready(0)
    conn.can_send_more.return_value = False
    assert not cli.is_ready(0)
    conn.can_send_more.return_value = True

    # disconnected nodes, not ready
    assert cli.is_ready(0)
    conn.state = ConnectionStates.DISCONNECTED
    assert not cli.is_ready(0)


def test_close(mocker, conn):
    cli = KafkaClient()
    mocker.patch.object(cli, '_selector')

    # bootstrap connection should have been closed
    assert conn.close.call_count == 1

    # Unknown node - silent
    cli.close(2)

    # Single node close
    cli._maybe_connect(0)
    assert conn.close.call_count == 1
    cli.close(0)
    assert conn.close.call_count == 2

    # All node close
    cli._maybe_connect(1)
    cli.close()
    assert conn.close.call_count == 4


def test_is_disconnected(conn):
    cli = KafkaClient()

    # False if not connected yet
    conn.state = ConnectionStates.DISCONNECTED
    assert not cli.is_disconnected(0)

    cli._maybe_connect(0)
    assert cli.is_disconnected(0)

    conn.state = ConnectionStates.CONNECTING
    assert not cli.is_disconnected(0)

    conn.state = ConnectionStates.CONNECTED
    assert not cli.is_disconnected(0)


def test_send(conn):
    cli = KafkaClient()

    # Send to unknown node => raises AssertionError
    try:
        cli.send(2, None)
        assert False, 'Exception not raised'
    except AssertionError:
        pass

    # Send to disconnected node => NodeNotReady
    conn.state = ConnectionStates.DISCONNECTED
    f = cli.send(0, None)
    assert f.failed()
    assert isinstance(f.exception, Errors.NodeNotReadyError)

    conn.state = ConnectionStates.CONNECTED
    cli._maybe_connect(0)
    # ProduceRequest w/ 0 required_acks -> no response
    request = ProduceRequest[0](0, 0, [])
    ret = cli.send(0, request)
    assert conn.send.called_with(request, expect_response=False)
    assert isinstance(ret, Future)

    request = MetadataRequest[0]([])
    cli.send(0, request)
    assert conn.send.called_with(request, expect_response=True)


def test_poll(mocker):
    mocker.patch.object(KafkaClient, '_bootstrap')
    metadata = mocker.patch.object(KafkaClient, '_maybe_refresh_metadata')
    _poll = mocker.patch.object(KafkaClient, '_poll')
    cli = KafkaClient()
    tasks = mocker.patch.object(cli._delayed_tasks, 'next_at')

    # metadata timeout wins
    metadata.return_value = 1000
    tasks.return_value = 2
    cli.poll()
    _poll.assert_called_with(1.0, sleep=True)

    # user timeout wins
    cli.poll(250)
    _poll.assert_called_with(0.25, sleep=True)

    # tasks timeout wins
    tasks.return_value = 0
    cli.poll(250)
    _poll.assert_called_with(0, sleep=True)

    # default is request_timeout_ms
    metadata.return_value = 1000000
    tasks.return_value = 10000
    cli.poll()
    _poll.assert_called_with(cli.config['request_timeout_ms'] / 1000.0,
                             sleep=True)


def test__poll():
    pass


def test_in_flight_request_count():
    pass


def test_least_loaded_node():
    pass


def test_set_topics():
    pass


def test_maybe_refresh_metadata():
    pass


def test_schedule():
    pass


def test_unschedule():
    pass
