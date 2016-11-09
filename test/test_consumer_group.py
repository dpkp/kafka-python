import collections
import logging
import threading
import time

import pytest
import six

from kafka import SimpleClient
from kafka.conn import ConnectionStates
from kafka.consumer.group import KafkaConsumer
from kafka.structs import TopicPartition

from test.conftest import version
from test.testutil import random_string


def get_connect_str(kafka_broker):
    return 'localhost:' + str(kafka_broker.port)


@pytest.fixture
def simple_client(kafka_broker):
    return SimpleClient(get_connect_str(kafka_broker))


@pytest.fixture
def topic(simple_client):
    topic = random_string(5)
    simple_client.ensure_topic_exists(topic)
    return topic


@pytest.mark.skipif(not version(), reason="No KAFKA_VERSION set")
def test_consumer(kafka_broker, version):

    # 0.8.2 brokers need a topic to function well
    if version >= (0, 8, 2) and version < (0, 9):
        topic(simple_client(kafka_broker))

    consumer = KafkaConsumer(bootstrap_servers=get_connect_str(kafka_broker))
    consumer.poll(500)
    assert len(consumer._client._conns) > 0
    node_id = list(consumer._client._conns.keys())[0]
    assert consumer._client._conns[node_id].state is ConnectionStates.CONNECTED


@pytest.mark.skipif(version() < (0, 9), reason='Unsupported Kafka Version')
@pytest.mark.skipif(not version(), reason="No KAFKA_VERSION set")
def test_group(kafka_broker, topic):
    num_partitions = 4
    connect_str = get_connect_str(kafka_broker)
    consumers = {}
    stop = {}
    threads = {}
    messages = collections.defaultdict(list)
    def consumer_thread(i):
        assert i not in consumers
        assert i not in stop
        stop[i] = threading.Event()
        consumers[i] = KafkaConsumer(topic,
                                     bootstrap_servers=connect_str,
                                     heartbeat_interval_ms=500)
        while not stop[i].is_set():
            for tp, records in six.itervalues(consumers[i].poll(100)):
                messages[i][tp].extend(records)
        consumers[i].close()
        del consumers[i]
        del stop[i]

    num_consumers = 4
    for i in range(num_consumers):
        t = threading.Thread(target=consumer_thread, args=(i,))
        t.start()
        threads[i] = t

    try:
        timeout = time.time() + 35
        while True:
            for c in range(num_consumers):

                # Verify all consumers have been created
                if c not in consumers:
                    break

                # Verify all consumers have an assignment
                elif not consumers[c].assignment():
                    break

            # If all consumers exist and have an assignment
            else:

                # Verify all consumers are in the same generation
                # then log state and break while loop
                generations = set([consumer._coordinator.generation
                                   for consumer in list(consumers.values())])

                # New generation assignment is not complete until
                # coordinator.rejoining = False
                rejoining = any([consumer._coordinator.rejoining
                                 for consumer in list(consumers.values())])

                if not rejoining and len(generations) == 1:
                    for c, consumer in list(consumers.items()):
                        logging.info("[%s] %s %s: %s", c,
                                     consumer._coordinator.generation,
                                     consumer._coordinator.member_id,
                                     consumer.assignment())
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
            threads[c].join()


@pytest.mark.skipif(not version(), reason="No KAFKA_VERSION set")
def test_paused(kafka_broker, topic):
    consumer = KafkaConsumer(bootstrap_servers=get_connect_str(kafka_broker))
    topics = [TopicPartition(topic, 1)]
    consumer.assign(topics)
    assert set(topics) == consumer.assignment()
    assert set() == consumer.paused()

    consumer.pause(topics[0])
    assert set([topics[0]]) == consumer.paused()

    consumer.resume(topics[0])
    assert set() == consumer.paused()

    consumer.unsubscribe()
    assert set() == consumer.paused()
