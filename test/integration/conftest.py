import os
from urllib.parse import urlparse
import uuid

import pytest

from kafka import KafkaAdminClient, KafkaClient, KafkaConsumer, KafkaProducer
from kafka.util import TOPIC_LEGAL_CHARS, TOPIC_MAX_LENGTH, ensure_valid_topic_name
from test.testutil import env_kafka_version, random_string
from test.integration.fixtures import KafkaFixture, ZookeeperFixture, create_topics, client_params


def pytest_collection_modifyitems(items):
    current_dir = os.path.dirname(__file__)
    for item in items:
        if not str(item.fspath).startswith(current_dir):
            continue
        if not env_kafka_version():
            item.add_marker(pytest.mark.skip(reason="No KAFKA_VERSION set"))


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
    return kafka_broker_factory()


@pytest.fixture(scope="module")
def kafka_broker_factory():
    """Return a Kafka broker fixture factory"""
    assert env_kafka_version(), 'KAFKA_VERSION must be specified to run integration tests'

    _brokers = []
    def factory(**broker_params):
        if "KAFKA_URI" in os.environ:
            parse = urlparse(os.environ["KAFKA_URI"])
            (host, port) = (parse.hostname, parse.port)
            return KafkaFixture.instance(0, host=host, port=port, external=True)
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
    client = KafkaClient(**client_params(kafka_broker, request.node.name))
    yield client
    client.close()


@pytest.fixture
def consumer(kafka_consumer_factory):
    """Return a KafkaConsumer fixture"""
    return kafka_consumer_factory()


@pytest.fixture
def kafka_consumer_factory(kafka_broker, topic, request):
    """Return a KafkaConsumer factory fixture"""
    consumer = None

    def factory(topics=(topic,), **override_params):
        nonlocal consumer
        params = {
            'client_id': f'{request.node.name}_{random_string(4)}',
            'api_version': env_kafka_version(),
            'heartbeat_interval_ms': 500,
            'auto_offset_reset': 'earliest',
        }
        params.update(override_params)
        params = client_params(kafka_broker, **params)
        consumer = KafkaConsumer(*topics, **params)
        return consumer

    yield factory

    if consumer:
        consumer.close()


@pytest.fixture
def producer(kafka_producer_factory):
    """Return a KafkaProducer fixture"""
    yield kafka_producer_factory()


@pytest.fixture
def kafka_producer_factory(kafka_broker, request):
    """Return a KafkaProducer factory fixture"""
    producer = None

    def factory(**override_params):
        nonlocal producer
        params = {
            'client_id': f'{request.node.name}_{random_string(4)}',
            'api_version': env_kafka_version(),
        }
        params.update(override_params)
        params = client_params(kafka_broker, **params)
        producer = KafkaProducer(**params)
        return producer

    yield factory

    if producer:
        producer.close()


@pytest.fixture
def kafka_admin_client(kafka_admin_client_factory):
    """Return a KafkaAdminClient fixture"""
    yield kafka_admin_client_factory()


@pytest.fixture
def kafka_admin_client_factory(kafka_broker):
    """Return a KafkaAdminClient factory fixture"""
    admin_client = None

    def factory(**override_params):
        nonlocal admin_client
        params = {
            'client_id': f'admin_{random_string(4)}',
            'api_version': env_kafka_version(),
        }
        params.update(override_params)
        params = client_params(kafka_broker, **params)
        admin_client = KafkaAdminClient(**params)
        return admin_client

    yield factory

    if admin_client:
        admin_client.close()


@pytest.fixture
def topic(kafka_broker, request):
    """Return a topic fixture"""
    topic_name = ''.join(TOPIC_LEGAL_CHARS.findall(request.node.name))
    topic_name = topic_name[:TOPIC_MAX_LENGTH - 11] + '_' + random_string(10)
    ensure_valid_topic_name(topic_name)
    create_topics(kafka_broker, [topic_name])
    return topic_name


@pytest.fixture()
def send_messages(topic, producer, request):
    """A factory that returns a send_messages function with a pre-populated
    topic topic / producer."""

    def _send_messages(number_range, partition=0, topic=topic, producer=producer, request=request):
        """
            messages is typically `range(0,100)`
            partition is an int
        """
        messages_and_futures = []  # [(message, produce_future),]
        for i in number_range:
            # request.node.name provides the test name (including parametrized values)
            encoded_msg = '{}-{}-{}'.format(i, request.node.name, uuid.uuid4()).encode('utf-8')
            future = producer.send(topic, value=encoded_msg, partition=partition)
            messages_and_futures.append((encoded_msg, future))
        producer.flush()
        for (msg, f) in messages_and_futures:
            assert f.succeeded()
        return [msg for (msg, f) in messages_and_futures]

    return _send_messages
