
import pytest

from kafka.client_async import KafkaClient
from kafka.common import BrokerMetadata
from kafka.conn import ConnectionStates
from kafka.future import Future
from kafka.protocol.metadata import MetadataResponse, MetadataRequest


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


def test_can_connect():
    pass


def test_initiate_connect():
    pass


def test_finish_connect():
    pass


def test_ready():
    pass


def test_close():
    pass


def test_is_disconnected():
    pass


def test_is_ready():
    pass


def test_can_send_request():
    pass


def test_send():
    pass


def test_poll():
    pass


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
