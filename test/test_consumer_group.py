import collections
import logging
import threading
import os
import time

import pytest
import six

from kafka import KafkaClient, SimpleProducer
from kafka.common import TopicPartition
from kafka.conn import BrokerConnection, ConnectionStates
from kafka.consumer.group import KafkaConsumer

from test.fixtures import KafkaFixture, ZookeeperFixture
from test.testutil import random_string


@pytest.fixture(scope="module")
def version():
    if 'KAFKA_VERSION' not in os.environ:
        return ()
    return tuple(map(int, os.environ['KAFKA_VERSION'].split('.')))


@pytest.fixture(scope="module")
def zookeeper(version, request):
    assert version
    zk = ZookeeperFixture.instance()
    def fin():
        zk.close()
    request.addfinalizer(fin)
    return zk


@pytest.fixture(scope="module")
def kafka_broker(version, zookeeper, request):
    assert version
    k = KafkaFixture.instance(0, zookeeper.host, zookeeper.port,
                              partitions=4)
    def fin():
        k.close()
    request.addfinalizer(fin)
    return k


@pytest.fixture
def simple_client(kafka_broker):
    connect_str = 'localhost:' + str(kafka_broker.port)
    return KafkaClient(connect_str)


@pytest.fixture
def topic(simple_client):
    topic = random_string(5)
    simple_client.ensure_topic_exists(topic)
    return topic


@pytest.fixture
def topic_with_messages(simple_client, topic):
    producer = SimpleProducer(simple_client)
    for i in six.moves.xrange(100):
        producer.send_messages(topic, 'msg_%d' % i)
    return topic


@pytest.mark.skipif(not version(), reason="No KAFKA_VERSION set")
def test_consumer(kafka_broker, version):

    # 0.8.2 brokers need a topic to function well
    if version >= (0, 8, 2) and version < (0, 9):
        topic(simple_client(kafka_broker))

    connect_str = 'localhost:' + str(kafka_broker.port)
    consumer = KafkaConsumer(bootstrap_servers=connect_str)
    consumer.poll(500)
    assert len(consumer._client._conns) > 0
    node_id = list(consumer._client._conns.keys())[0]
    assert consumer._client._conns[node_id].state is ConnectionStates.CONNECTED


@pytest.mark.skipif(version() < (0, 9), reason='Unsupported Kafka Version')
@pytest.mark.skipif(not version(), reason="No KAFKA_VERSION set")
def test_group(kafka_broker, topic):
    num_partitions = 4
    connect_str = 'localhost:' + str(kafka_broker.port)
    consumers = {}
    stop = {}
    messages = collections.defaultdict(list)
    def consumer_thread(i):
        assert i not in consumers
        assert i not in stop
        stop[i] = threading.Event()
        consumers[i] = KafkaConsumer(topic,
                                     bootstrap_servers=connect_str,
                                     heartbeat_interval_ms=500)
        while not stop[i].is_set():
            for tp, records in six.itervalues(consumers[i].poll()):
                messages[i][tp].extend(records)
        consumers[i].close()
        del consumers[i]
        del stop[i]

    num_consumers = 4
    for i in range(num_consumers):
        threading.Thread(target=consumer_thread, args=(i,)).start()

    try:
        timeout = time.time() + 35
        while True:
            for c in range(num_consumers):
                if c not in consumers:
                    break
                elif not consumers[c].assignment():
                    break
            else:
                for c in range(num_consumers):
                    logging.info("%s: %s", c, consumers[c].assignment())
                break
            assert time.time() < timeout, "timeout waiting for assignments"

        group_assignment = set()
        for c in range(num_consumers):
            assert len(consumers[c].assignment()) != 0
            assert set.isdisjoint(consumers[c].assignment(), group_assignment)
            group_assignment.update(consumers[c].assignment())

        assert group_assignment == set([
            TopicPartition(topic, partition)
            for partition in range(num_partitions)])

    finally:
        for c in range(num_consumers):
            stop[c].set()


@pytest.mark.skipif(not version(), reason="No KAFKA_VERSION set")
def test_correlation_id_rollover(kafka_broker):
    logging.getLogger('kafka.conn').setLevel(logging.ERROR)
    from kafka.protocol.metadata import MetadataRequest
    conn = BrokerConnection('localhost', kafka_broker.port,
                            receive_buffer_bytes=131072,
                            max_in_flight_requests_per_connection=100)
    req = MetadataRequest([])
    while not conn.connected():
        conn.connect()
    futures = collections.deque()
    start = time.time()
    done = 0
    for i in six.moves.xrange(2**13):
        if not conn.can_send_more():
            conn.recv(timeout=None)
        futures.append(conn.send(req))
        conn.recv()
        while futures and futures[0].is_done:
            f = futures.popleft()
            if not f.succeeded():
                raise f.exception
            done += 1
        if time.time() > start + 10:
            print ("%d done" % done)
            start = time.time()

    while futures:
        conn.recv()
        if futures[0].is_done:
            f = futures.popleft()
            if not f.succeeded():
                raise f.exception
