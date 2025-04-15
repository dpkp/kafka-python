from __future__ import absolute_import

import os
import uuid

import pytest

from kafka.vendor.six.moves.urllib.parse import urlparse  # pylint: disable=E0611,F0401
from test.testutil import env_kafka_version, random_string
from test.integration.fixtures import KafkaFixture, ZookeeperFixture


@pytest.fixture(scope="module")
def zookeeper():
    """Return a Zookeeper fixture"""
    if "ZOOKEEPER_URI" in os.environ:
        parse = urlparse(os.environ["ZOOKEEPER_URI"])
        (host, port) = (parse.hostname, parse.port)
        yield ZookeeperFixture.instance(host=host, port=port, external=True)
    else:
        zk_instance = ZookeeperFixture.instance()
        yield zk_instance
        zk_instance.close()


@pytest.fixture(scope="module")
def kafka_broker(kafka_broker_factory):
    """Return a Kafka broker fixture"""
    if "KAFKA_URI" in os.environ:
        parse = urlparse(os.environ["KAFKA_URI"])
        (host, port) = (parse.hostname, parse.port)
        return KafkaFixture.instance(0, host=host, port=port, external=True)
    else:
        return kafka_broker_factory()


@pytest.fixture(scope="module")
def kafka_broker_factory():
    """Return a Kafka broker fixture factory"""
    assert env_kafka_version(), 'KAFKA_VERSION must be specified to run integration tests'

    _brokers = []
    def factory(**broker_params):
        params = {} if broker_params is None else broker_params.copy()
        params.setdefault('partitions', 4)
        node_id = params.pop('node_id', 0)
        broker = KafkaFixture.instance(node_id, **params)
        _brokers.append(broker)
        return broker

    yield factory

    zks = set()
    for broker in _brokers:
        zks.add(broker.zookeeper)
        broker.close()
    for zk in zks:
        if zk:
            zk.close()


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

    def factory(topics=(topic,), **kafka_consumer_params):
        params = {} if kafka_consumer_params is None else kafka_consumer_params.copy()
        params.setdefault('client_id', 'consumer_%s' % (request.node.name,))
        params.setdefault('auto_offset_reset', 'earliest')
        _consumer[0] = next(kafka_broker.get_consumers(cnt=1, topics=list(topics), **params))
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
def kafka_admin_client(kafka_admin_client_factory):
    """Return a KafkaAdminClient fixture"""
    yield kafka_admin_client_factory()


@pytest.fixture
def kafka_admin_client_factory(kafka_broker):
    """Return a KafkaAdminClient factory fixture"""
    _admin_client = [None]

    def factory(**kafka_admin_client_params):
        params = {} if kafka_admin_client_params is None else kafka_admin_client_params.copy()
        _admin_client[0] = next(kafka_broker.get_admin_clients(cnt=1, **params))
        return _admin_client[0]

    yield factory

    if _admin_client[0]:
        _admin_client[0].close()


@pytest.fixture
def topic(kafka_broker, request):
    """Return a topic fixture"""
    topic_name = '%s_%s' % (request.node.name, random_string(10))
    kafka_broker.create_topics([topic_name])
    return topic_name


@pytest.fixture()
def send_messages(topic, kafka_producer, request):
    """A factory that returns a send_messages function with a pre-populated
    topic topic / producer."""

    def _send_messages(number_range, partition=0, topic=topic, producer=kafka_producer, request=request):
        """
            messages is typically `range(0,100)`
            partition is an int
        """
        messages_and_futures = []  # [(message, produce_future),]
        for i in number_range:
            # request.node.name provides the test name (including parametrized values)
            encoded_msg = '{}-{}-{}'.format(i, request.node.name, uuid.uuid4()).encode('utf-8')
            future = kafka_producer.send(topic, value=encoded_msg, partition=partition)
            messages_and_futures.append((encoded_msg, future))
        kafka_producer.flush()
        for (msg, f) in messages_and_futures:
            assert f.succeeded()
        return [msg for (msg, f) in messages_and_futures]

    return _send_messages
