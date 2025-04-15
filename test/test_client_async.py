from __future__ import absolute_import, division

# selectors in stdlib as of py3.4
try:
    import selectors # pylint: disable=import-error
except ImportError:
    # vendored backport module
    import kafka.vendor.selectors34 as selectors

import socket
import time

import pytest

from kafka.client_async import KafkaClient, IdleConnectionManager
from kafka.cluster import ClusterMetadata
from kafka.conn import ConnectionStates
import kafka.errors as Errors
from kafka.future import Future
from kafka.protocol.metadata import MetadataRequest
from kafka.protocol.produce import ProduceRequest
from kafka.structs import BrokerMetadata


@pytest.fixture
def client_poll_mocked(mocker):
    cli = KafkaClient(request_timeout_ms=9999999,
                      reconnect_backoff_ms=2222,
                      connections_max_idle_ms=float('inf'),
                      api_version=(0, 9))
    mocker.patch.object(cli, '_poll')
    ttl = mocker.patch.object(cli.cluster, 'ttl')
    ttl.return_value = 0
    try:
        yield cli
    finally:
        cli._close()


@pytest.fixture
def client_selector_mocked(mocker, conn):
    client = KafkaClient(api_version=(0, 9))
    mocker.patch.object(client, '_selector')
    client.poll(future=client.cluster.request_update())
    try:
        yield client
    finally:
        client._close()

def test_bootstrap(mocker, conn):
    conn.state = ConnectionStates.CONNECTED
    cli = KafkaClient(api_version=(2, 1))
    mocker.patch.object(cli, '_selector')
    future = cli.cluster.request_update()
    cli.poll(future=future)

    assert future.succeeded()
    args, kwargs = conn.call_args
    assert args == ('localhost', 9092, socket.AF_UNSPEC)
    kwargs.pop('state_change_callback')
    kwargs.pop('node_id')
    assert kwargs == cli.config
    conn.send.assert_called_once_with(MetadataRequest[7]([], True), blocking=False, request_timeout_ms=None)
    assert cli._bootstrap_fails == 0
    assert cli.cluster.brokers() == set([BrokerMetadata(0, 'foo', 12, None),
                                         BrokerMetadata(1, 'bar', 34, None)])


def test_can_connect(client_selector_mocked, conn):
    # Node is not in broker metadata - can't connect
    assert not client_selector_mocked._can_connect(2)

    # Node is in broker metadata but not in _conns
    assert 0 not in client_selector_mocked._conns
    assert client_selector_mocked._can_connect(0)

    # Node is connected, can't reconnect
    assert client_selector_mocked._init_connect(0) is True
    assert not client_selector_mocked._can_connect(0)

    # Node is disconnected, can connect
    client_selector_mocked._conns[0].state = ConnectionStates.DISCONNECTED
    assert client_selector_mocked._can_connect(0)

    # Node is disconnected, but blacked out
    conn.blacked_out.return_value = True
    assert not client_selector_mocked._can_connect(0)


def test_init_connect(client_selector_mocked, conn):
    # Node not in metadata, return False
    assert not client_selector_mocked._init_connect(2)

    # New node_id creates a conn object
    assert 0 not in client_selector_mocked._conns
    conn.state = ConnectionStates.DISCONNECTED
    conn.connect.side_effect = lambda: conn._set_conn_state(ConnectionStates.CONNECTING)
    assert client_selector_mocked._init_connect(0) is True
    assert client_selector_mocked._conns[0] is conn


def test_conn_state_change(client_selector_mocked, conn):
    sel = client_selector_mocked._selector

    node_id = 0
    client_selector_mocked._conns[node_id] = conn
    conn.state = ConnectionStates.CONNECTING
    sock = conn._sock
    client_selector_mocked._conn_state_change(node_id, sock, conn)
    assert node_id in client_selector_mocked._connecting
    sel.register.assert_called_with(sock, selectors.EVENT_WRITE, conn)

    conn.state = ConnectionStates.CONNECTED
    client_selector_mocked._conn_state_change(node_id, sock, conn)
    assert node_id not in client_selector_mocked._connecting
    sel.modify.assert_called_with(sock, selectors.EVENT_READ, conn)

    # Failure to connect should trigger metadata update
    assert client_selector_mocked.cluster._need_update is False
    conn.state = ConnectionStates.DISCONNECTED
    client_selector_mocked._conn_state_change(node_id, sock, conn)
    assert node_id not in client_selector_mocked._connecting
    assert client_selector_mocked.cluster._need_update is True
    sel.unregister.assert_called_with(sock)

    conn.state = ConnectionStates.CONNECTING
    client_selector_mocked._conn_state_change(node_id, sock, conn)
    assert node_id in client_selector_mocked._connecting
    conn.state = ConnectionStates.DISCONNECTED
    client_selector_mocked._conn_state_change(node_id, sock, conn)
    assert node_id not in client_selector_mocked._connecting


def test_ready(mocker, client_selector_mocked, conn):
    maybe_connect = mocker.patch.object(client_selector_mocked, 'maybe_connect')
    node_id = 1
    client_selector_mocked.ready(node_id)
    maybe_connect.assert_called_with(node_id)


def test_is_ready(client_selector_mocked, conn):
    client_selector_mocked._init_connect(0)
    client_selector_mocked._init_connect(1)

    # metadata refresh blocks ready nodes
    assert client_selector_mocked.is_ready(0)
    assert client_selector_mocked.is_ready(1)
    client_selector_mocked._metadata_refresh_in_progress = True
    assert not client_selector_mocked.is_ready(0)
    assert not client_selector_mocked.is_ready(1)

    # requesting metadata update also blocks ready nodes
    client_selector_mocked._metadata_refresh_in_progress = False
    assert client_selector_mocked.is_ready(0)
    assert client_selector_mocked.is_ready(1)
    client_selector_mocked.cluster.request_update()
    client_selector_mocked.cluster.config['retry_backoff_ms'] = 0
    assert not client_selector_mocked._metadata_refresh_in_progress
    assert not client_selector_mocked.is_ready(0)
    assert not client_selector_mocked.is_ready(1)
    client_selector_mocked.cluster._need_update = False

    # if connection can't send more, not ready
    assert client_selector_mocked.is_ready(0)
    conn.can_send_more.return_value = False
    assert not client_selector_mocked.is_ready(0)
    conn.can_send_more.return_value = True

    # disconnected nodes, not ready
    assert client_selector_mocked.is_ready(0)
    conn.state = ConnectionStates.DISCONNECTED
    assert not client_selector_mocked.is_ready(0)


def test_close(client_selector_mocked, conn):
    call_count = conn.close.call_count

    # Unknown node - silent
    client_selector_mocked.close(2)
    call_count += 0
    assert conn.close.call_count == call_count

    # Single node close
    client_selector_mocked._init_connect(0)
    assert conn.close.call_count == call_count
    client_selector_mocked.close(0)
    call_count += 1
    assert conn.close.call_count == call_count

    # All node close
    client_selector_mocked._init_connect(1)
    client_selector_mocked.close()
    # +2 close: node 1, node bootstrap (node 0 already closed)
    call_count += 2
    assert conn.close.call_count == call_count


def test_is_disconnected(client_selector_mocked, conn):
    # False if not connected yet
    conn.state = ConnectionStates.DISCONNECTED
    assert not client_selector_mocked.is_disconnected(0)

    client_selector_mocked._init_connect(0)
    assert client_selector_mocked.is_disconnected(0)

    conn.state = ConnectionStates.CONNECTING
    assert not client_selector_mocked.is_disconnected(0)

    conn.state = ConnectionStates.CONNECTED
    assert not client_selector_mocked.is_disconnected(0)


def test_send(client_selector_mocked, conn):
    # Send to unknown node => raises AssertionError
    try:
        client_selector_mocked.send(2, None)
        assert False, 'Exception not raised'
    except AssertionError:
        pass

    # Send to disconnected node => NodeNotReady
    conn.state = ConnectionStates.DISCONNECTED
    f = client_selector_mocked.send(0, None)
    assert f.failed()
    assert isinstance(f.exception, Errors.NodeNotReadyError)

    conn.state = ConnectionStates.CONNECTED
    client_selector_mocked._init_connect(0)
    # ProduceRequest w/ 0 required_acks -> no response
    request = ProduceRequest[0](0, 0, [])
    assert request.expect_response() is False
    ret = client_selector_mocked.send(0, request)
    conn.send.assert_called_with(request, blocking=False, request_timeout_ms=None)
    assert isinstance(ret, Future)

    request = MetadataRequest[0]([])
    client_selector_mocked.send(0, request)
    conn.send.assert_called_with(request, blocking=False, request_timeout_ms=None)


def test_poll(mocker, client_poll_mocked):
    metadata = mocker.patch.object(client_poll_mocked, '_maybe_refresh_metadata')
    ifr_request_timeout = mocker.patch.object(client_poll_mocked, '_next_ifr_request_timeout_ms')
    now = time.time()
    t = mocker.patch('time.time')
    t.return_value = now

    # metadata timeout wins
    ifr_request_timeout.return_value = float('inf')
    metadata.return_value = 1000
    client_poll_mocked.poll()
    client_poll_mocked._poll.assert_called_with(1.0)

    # user timeout wins
    client_poll_mocked.poll(timeout_ms=250)
    client_poll_mocked._poll.assert_called_with(0.25)

    # ifr request timeout wins
    ifr_request_timeout.return_value = 30000
    metadata.return_value = 1000000
    client_poll_mocked.poll()
    client_poll_mocked._poll.assert_called_with(30.0)


def test__poll():
    pass


def test_in_flight_request_count():
    pass


def test_least_loaded_node():
    pass


def test_set_topics(mocker):
    request_update = mocker.patch.object(ClusterMetadata, 'request_update')
    request_update.side_effect = lambda: Future()
    cli = KafkaClient(api_version=(0, 10, 0))

    # replace 'empty' with 'non empty'
    request_update.reset_mock()
    fut = cli.set_topics(['t1', 't2'])
    assert not fut.is_done
    request_update.assert_called_with()

    # replace 'non empty' with 'same'
    request_update.reset_mock()
    fut = cli.set_topics(['t1', 't2'])
    assert fut.is_done
    assert fut.value == set(['t1', 't2'])
    request_update.assert_not_called()

    # replace 'non empty' with 'empty'
    request_update.reset_mock()
    fut = cli.set_topics([])
    assert fut.is_done
    assert fut.value == set()
    request_update.assert_not_called()


def test_maybe_refresh_metadata_ttl(client_poll_mocked):
    client_poll_mocked.cluster.ttl.return_value = 1234

    client_poll_mocked.poll(timeout_ms=12345678)
    client_poll_mocked._poll.assert_called_with(1.234)


def test_maybe_refresh_metadata_backoff(mocker, client_poll_mocked):
    mocker.patch.object(client_poll_mocked, 'least_loaded_node', return_value=None)
    mocker.patch.object(client_poll_mocked, 'least_loaded_node_refresh_ms', return_value=4321)
    now = time.time()
    t = mocker.patch('time.time')
    t.return_value = now

    client_poll_mocked.poll(timeout_ms=12345678)
    client_poll_mocked._poll.assert_called_with(4.321)


def test_maybe_refresh_metadata_in_progress(client_poll_mocked):
    client_poll_mocked._metadata_refresh_in_progress = True

    client_poll_mocked.poll(timeout_ms=12345678)
    client_poll_mocked._poll.assert_called_with(9999.999) # request_timeout_ms


def test_maybe_refresh_metadata_update(mocker, client_poll_mocked):
    mocker.patch.object(client_poll_mocked, 'least_loaded_node', return_value='foobar')
    mocker.patch.object(client_poll_mocked, '_can_send_request', return_value=True)
    send = mocker.patch.object(client_poll_mocked, 'send')
    client_poll_mocked.cluster.need_all_topic_metadata = True

    client_poll_mocked.poll(timeout_ms=12345678)
    client_poll_mocked._poll.assert_called_with(9999.999) # request_timeout_ms
    assert client_poll_mocked._metadata_refresh_in_progress
    request = MetadataRequest[0]([])
    send.assert_called_once_with('foobar', request, wakeup=False)


def test_maybe_refresh_metadata_cant_send(mocker, client_poll_mocked):
    mocker.patch.object(client_poll_mocked, 'least_loaded_node', return_value='foobar')
    mocker.patch.object(client_poll_mocked, '_can_send_request', return_value=False)
    mocker.patch.object(client_poll_mocked, '_can_connect', return_value=True)
    mocker.patch.object(client_poll_mocked, '_init_connect', return_value=True)

    now = time.time()
    t = mocker.patch('time.time')
    t.return_value = now

    # first poll attempts connection
    client_poll_mocked.poll()
    client_poll_mocked._poll.assert_called()
    client_poll_mocked._init_connect.assert_called_once_with('foobar')

    # poll while connecting should not attempt a new connection
    client_poll_mocked._connecting.add('foobar')
    client_poll_mocked._can_connect.reset_mock()
    client_poll_mocked.poll()
    client_poll_mocked._poll.assert_called()
    assert not client_poll_mocked._can_connect.called
    assert not client_poll_mocked._metadata_refresh_in_progress


def test_schedule():
    pass


def test_unschedule():
    pass


def test_idle_connection_manager(mocker):
    t = mocker.patch.object(time, 'time')
    t.return_value = 0

    idle = IdleConnectionManager(100)
    assert idle.next_check_ms() == float('inf')

    idle.update('foo')
    assert not idle.is_expired('foo')
    assert idle.poll_expired_connection() is None
    assert idle.next_check_ms() == 100

    t.return_value = 90 / 1000
    assert not idle.is_expired('foo')
    assert idle.poll_expired_connection() is None
    assert idle.next_check_ms() == 10

    t.return_value = 100 / 1000
    assert idle.is_expired('foo')
    assert idle.next_check_ms() == 0

    conn_id, conn_ts = idle.poll_expired_connection()
    assert conn_id == 'foo'
    assert conn_ts == 0

    idle.remove('foo')
    assert idle.next_check_ms() == float('inf')
