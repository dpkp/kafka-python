import contextlib
import os
from urllib.parse import urlparse
import uuid

import pytest

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.net.compat import KafkaNetClient
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
    """Return a KafkaNetClient fixture"""
    client = KafkaNetClient(**client_params(kafka_broker, request.node.name))
    yield client
    client.close()


@pytest.fixture
def consumer(kafka_consumer_factory):
    """Return a KafkaConsumer fixture; closed at fixture teardown."""
    with kafka_consumer_factory() as c:
        yield c


@pytest.fixture
def kafka_consumer_factory(kafka_broker, topic, request):
    """Return a KafkaConsumer factory fixture.

    Each call returns a context manager that yields a fresh KafkaConsumer
    and closes it when the ``with`` block exits::

        with kafka_consumer_factory(group_id='foo') as consumer:
            consumer.subscribe([topic])
            ...

    As a defensive safety net, any consumers still open at fixture
    teardown (e.g. if a test forgot ``with``) are also closed.
    """
    consumers = []

    @contextlib.contextmanager
    def factory(topics=(topic,), **override_params):
        params = {
            'client_id': f'{request.node.name}_{random_string(4)}',
            'api_version': env_kafka_version(),
            'heartbeat_interval_ms': 500,
            'auto_offset_reset': 'earliest',
        }
        params.update(override_params)
        params = client_params(kafka_broker, **params)
        c = KafkaConsumer(*topics, **params)
        consumers.append(c)
        try:
            yield c
        finally:
            c.close()

    yield factory

    for c in consumers:
        try:
            c.close()
        except Exception:
            pass


@pytest.fixture
def producer(kafka_producer_factory):
    """Return a KafkaProducer fixture; closed at fixture teardown."""
    with kafka_producer_factory() as p:
        yield p


@pytest.fixture
def kafka_producer_factory(kafka_broker, request):
    """Return a KafkaProducer factory fixture.

    Each call returns a context manager that yields a fresh KafkaProducer
    and closes it when the ``with`` block exits::

        with kafka_producer_factory(compression_type='gzip') as producer:
            producer.send(...)

    As a defensive safety net, any producers still open at fixture
    teardown are also closed.
    """
    producers = []

    @contextlib.contextmanager
    def factory(**override_params):
        params = {
            'client_id': f'{request.node.name}_{random_string(4)}',
            'api_version': env_kafka_version(),
        }
        params.update(override_params)
        params = client_params(kafka_broker, **params)
        p = KafkaProducer(**params)
        producers.append(p)
        try:
            yield p
        finally:
            p.close()

    yield factory

    for p in producers:
        try:
            p.close()
        except Exception:
            pass


@pytest.fixture
def kafka_admin_client(kafka_admin_client_factory):
    """Return a KafkaAdminClient fixture; closed at fixture teardown."""
    with kafka_admin_client_factory() as c:
        yield c


@pytest.fixture
def kafka_admin_client_factory(kafka_broker):
    """Return a KafkaAdminClient factory fixture.

    Each call returns a context manager that yields a fresh
    KafkaAdminClient and closes it when the ``with`` block exits::

        with kafka_admin_client_factory() as admin:
            admin.create_topics(...)

    As a defensive safety net, any admin clients still open at fixture
    teardown are also closed.
    """
    admin_clients = []

    @contextlib.contextmanager
    def factory(**override_params):
        params = {
            'client_id': f'admin_{random_string(4)}',
            'api_version': env_kafka_version(),
        }
        params.update(override_params)
        params = client_params(kafka_broker, **params)
        c = KafkaAdminClient(**params)
        admin_clients.append(c)
        try:
            yield c
        finally:
            c.close()

    yield factory

    for c in admin_clients:
        try:
            c.close()
        except Exception:
            pass


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
