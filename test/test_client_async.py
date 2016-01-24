import time

import pytest

from kafka.client_async import KafkaClient
from kafka.common import BrokerMetadata
import kafka.common as Errors
from kafka.conn import ConnectionStates
from kafka.future import Future
from kafka.protocol.metadata import MetadataResponse, MetadataRequest
from kafka.protocol.produce import ProduceRequest


@pytest.mark.parametrize("bootstrap,expected_hosts", [
    (None, [('localhost', 9092)]),
    ('foobar:1234', [('foobar', 1234)]),
    ('fizzbuzz', [('fizzbuzz', 9092)]),
    ('foo:12,bar:34', [('foo', 12), ('bar', 34)]),
    (['fizz:56', 'buzz'], [('fizz', 56), ('buzz', 9092)]),
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


@pytest.fixture
def conn(mocker):
    conn = mocker.patch('kafka.client_async.BrokerConnection')
    conn.return_value = conn
    conn.state = ConnectionStates.CONNECTED
    conn.send.return_value = Future().success(
        MetadataResponse(
            [(0, 'foo', 12), (1, 'bar', 34)],  # brokers
            []))  # topics
    conn.blacked_out.return_value = False
    conn.connect.return_value = conn.state
    return conn


def test_bootstrap_success(conn):
    conn.state = ConnectionStates.CONNECTED
    cli = KafkaClient()
    conn.assert_called_once_with('localhost', 9092, **cli.config)
    conn.connect.assert_called_with()
    conn.send.assert_called_once_with(MetadataRequest([]))
    assert cli._bootstrap_fails == 0
    assert cli.cluster.brokers() == set([BrokerMetadata(0, 'foo', 12),
                                         BrokerMetadata(1, 'bar', 34)])

def test_bootstrap_failure(conn):
    conn.state = ConnectionStates.DISCONNECTED
    cli = KafkaClient()
    conn.assert_called_once_with('localhost', 9092, **cli.config)
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
    cli._initiate_connect(0)
    assert not cli._can_connect(0)

    # Node is disconnected, can connect
    cli._conns[0].state = ConnectionStates.DISCONNECTED
    assert cli._can_connect(0)

    # Node is disconnected, but blacked out
    conn.blacked_out.return_value = True
    assert not cli._can_connect(0)

def test_initiate_connect(conn):
    cli = KafkaClient()
    try:
        # Node not in metadata, raises AssertionError
        cli._initiate_connect(2)
    except AssertionError:
        pass
    else:
        assert False, 'Exception not raised'

    assert 0 not in cli._conns
    state = cli._initiate_connect(0)
    assert cli._conns[0] is conn
    assert state is conn.state


def test_finish_connect(conn):
    cli = KafkaClient()
    try:
        # Node not in metadata, raises AssertionError
        cli._initiate_connect(2)
    except AssertionError:
        pass
    else:
        assert False, 'Exception not raised'

    assert 0 not in cli._conns
    cli._initiate_connect(0)

    conn.connect.return_value = ConnectionStates.CONNECTING
    state = cli._finish_connect(0)
    assert 0 in cli._connecting
    assert state is ConnectionStates.CONNECTING

    conn.connect.return_value = ConnectionStates.CONNECTED
    state = cli._finish_connect(0)
    assert 0 not in cli._connecting
    assert state is ConnectionStates.CONNECTED

    # Failure to connect should trigger metadata update
    assert not cli.cluster._need_update
    cli._connecting.add(0)
    conn.connect.return_value = ConnectionStates.DISCONNECTED
    state = cli._finish_connect(0)
    assert 0 not in cli._connecting
    assert state is ConnectionStates.DISCONNECTED
    assert cli.cluster._need_update


def test_ready(conn):
    cli = KafkaClient()

    # Node not in metadata
    assert not cli.ready(2)

    # Node in metadata will connect
    assert 0 not in cli._conns
    assert cli.ready(0)
    assert 0 in cli._conns
    assert cli._conns[0].state is ConnectionStates.CONNECTED

    # metadata refresh blocks ready nodes
    assert cli.ready(0)
    assert cli.ready(1)
    cli._metadata_refresh_in_progress = True
    assert not cli.ready(0)
    assert not cli.ready(1)

    # requesting metadata update also blocks ready nodes
    cli._metadata_refresh_in_progress = False
    assert cli.ready(0)
    assert cli.ready(1)
    cli.cluster.request_update()
    cli.cluster.config['retry_backoff_ms'] = 0
    assert not cli._metadata_refresh_in_progress
    assert not cli.ready(0)
    assert not cli.ready(1)
    cli.cluster._need_update = False

    # if connection can't send more, not ready
    assert cli.ready(0)
    assert cli.ready(1)
    conn.can_send_more.return_value = False
    assert not cli.ready(0)
    conn.can_send_more.return_value = True

    # disconnected nodes, not ready
    assert cli.ready(0)
    assert cli.ready(1)
    conn.connected.return_value = False
    assert not cli.ready(0)
    conn.connected.return_value = True

    # connecting node connects
    cli._connecting.add(0)
    conn.connected.return_value = False
    cli.ready(0)
    assert 0 not in cli._connecting
    assert cli._conns[0].connect.called_with()


def test_close(conn):
    cli = KafkaClient()

    # Unknown node - silent
    cli.close(2)

    # Single node close
    cli._initiate_connect(0)
    assert not conn.close.call_count
    cli.close(0)
    assert conn.close.call_count == 1

    # All node close
    cli._initiate_connect(1)
    cli.close()
    assert conn.close.call_count == 3


def test_is_disconnected(conn):
    cli = KafkaClient()

    # False if not connected yet
    conn.state = ConnectionStates.DISCONNECTED
    assert not cli.is_disconnected(0)

    cli._initiate_connect(0)
    assert cli.is_disconnected(0)

    conn.state = ConnectionStates.CONNECTING
    assert not cli.is_disconnected(0)

    conn.state = ConnectionStates.CONNECTED
    assert not cli.is_disconnected(0)


def test_send(conn):
    cli = KafkaClient()
    try:
        cli.send(2, None)
    except Errors.NodeNotReadyError:
        pass
    else:
        assert False, 'NodeNotReadyError not raised'

    cli._initiate_connect(0)
    # ProduceRequest w/ 0 required_acks -> no response
    request = ProduceRequest(0, 0, [])
    ret = cli.send(0, request)
    assert conn.send.called_with(request, expect_response=False)
    assert isinstance(ret, Future)

    request = MetadataRequest([])
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
    _poll.assert_called_with(1.0, sleep=False)

    # user timeout wins
    cli.poll(250)
    _poll.assert_called_with(0.25, sleep=False)

    # tasks timeout wins
    tasks.return_value = 0
    cli.poll(250)
    _poll.assert_called_with(0, sleep=False)

    # default is request_timeout_ms
    metadata.return_value = 1000000
    tasks.return_value = 10000
    cli.poll()
    _poll.assert_called_with(cli.config['request_timeout_ms'] / 1000.0,
                             sleep=False)


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
