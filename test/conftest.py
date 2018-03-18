from __future__ import absolute_import

import inspect

import pytest

from test.fixtures import KafkaFixture, ZookeeperFixture
from test.testutil import kafka_version, random_string

@pytest.fixture(scope="module")
def version():
    """Return the Kafka version set in the OS environment"""
    return kafka_version()

@pytest.fixture(scope="module")
def zookeeper():
    """Return a Zookeeper fixture"""
    zk_instance = ZookeeperFixture.instance()
    yield zk_instance
    zk_instance.close()

@pytest.fixture(scope="module")
def kafka_broker(kafka_broker_factory):
    """Return a Kafka broker fixture"""
    return kafka_broker_factory()[0]

@pytest.fixture(scope="module")
def kafka_broker_factory(version, zookeeper):
    """Return a Kafka broker fixture factory"""
    assert version, 'KAFKA_VERSION must be specified to run integration tests'

    _brokers = []
    def factory(**broker_params):
        params = {} if broker_params is None else broker_params.copy()
        params.setdefault('partitions', 4)
        num_brokers = params.pop('num_brokers', 1)
        brokers = tuple(KafkaFixture.instance(x, zookeeper, **params)
                        for x in range(num_brokers))
        _brokers.extend(brokers)
        return brokers

    yield factory

    for broker in _brokers:
        broker.close()

@pytest.fixture
def simple_client(kafka_broker, request, topic):
    """Return a SimpleClient fixture"""
    client = kafka_broker.get_simple_client(client_id='%s_client' % (request.node.name,))
    client.ensure_topic_exists(topic)
    yield client
    client.close()

@pytest.fixture
def kafka_client(kafka_broker, request):
    """Return a KafkaClient fixture"""
    (client,) = kafka_broker.get_clients(cnt=1, client_id='%s_client' % (request.node.name,))
    yield client
    client.close()

@pytest.fixture
def kafka_consumer(kafka_consumer_factory):
    """Return a KafkaConsumer fixture"""
    return kafka_consumer_factory()

@pytest.fixture
def kafka_consumer_factory(kafka_broker, topic, request):
    """Return a KafkaConsumer factory fixture"""
    _consumer = [None]

    def factory(**kafka_consumer_params):
        params = {} if kafka_consumer_params is None else kafka_consumer_params.copy()
        params.setdefault('client_id', 'consumer_%s' % (request.node.name,))
        params.setdefault('auto_offset_reset', 'earliest')
        _consumer[0] = next(kafka_broker.get_consumers(cnt=1, topics=[topic], **params))
        return _consumer[0]

    yield factory

    if _consumer[0]:
        _consumer[0].close()

@pytest.fixture
def kafka_producer(kafka_producer_factory):
    """Return a KafkaProducer fixture"""
    yield kafka_producer_factory()

@pytest.fixture
def kafka_producer_factory(kafka_broker, request):
    """Return a KafkaProduce factory fixture"""
    _producer = [None]

    def factory(**kafka_producer_params):
        params = {} if kafka_producer_params is None else kafka_producer_params.copy()
        params.setdefault('client_id', 'producer_%s' % (request.node.name,))
        _producer[0] = next(kafka_broker.get_producers(cnt=1, **params))
        return _producer[0]

    yield factory

    if _producer[0]:
        _producer[0].close()

@pytest.fixture
def topic(kafka_broker, request):
    """Return a topic fixture"""
    topic_name = '%s_%s' % (request.node.name, random_string(10))
    kafka_broker.create_topics([topic_name])
    return topic_name

@pytest.fixture
def conn(mocker):
    """Return a connection mocker fixture"""
    from kafka.conn import ConnectionStates
    from kafka.future import Future
    from kafka.protocol.metadata import MetadataResponse
    conn = mocker.patch('kafka.client_async.BrokerConnection')
    conn.return_value = conn
    conn.state = ConnectionStates.CONNECTED
    conn.send.return_value = Future().success(
        MetadataResponse[0](
            [(0, 'foo', 12), (1, 'bar', 34)],  # brokers
            []))  # topics
    conn.blacked_out.return_value = False
    def _set_conn_state(state):
        conn.state = state
        return state
    conn._set_conn_state = _set_conn_state
    conn.connect.side_effect = lambda: conn.state
    conn.connect_blocking.return_value = True
    conn.connecting = lambda: conn.state in (ConnectionStates.CONNECTING,
                                             ConnectionStates.HANDSHAKE)
    conn.connected = lambda: conn.state is ConnectionStates.CONNECTED
    conn.disconnected = lambda: conn.state is ConnectionStates.DISCONNECTED
    return conn
