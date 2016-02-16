import collections
import logging
import threading
import os
import time

import pytest
import six

from kafka import SimpleClient, SimpleProducer
from kafka.common import TopicPartition
from kafka.conn import BrokerConnection, ConnectionStates
from kafka.consumer.group import KafkaConsumer

from test.conftest import version
from test.testutil import random_string


@pytest.fixture
def simple_client(kafka_broker):
    connect_str = 'localhost:' + str(kafka_broker.port)
    return SimpleClient(connect_str)


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
        t = threading.Thread(target=consumer_thread, args=(i,))
        t.daemon = True
        t.start()

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

                # Verify all consumers are in the same generation
                generations = set()
                for consumer in six.itervalues(consumers):
                    generations.add(consumer._coordinator.generation)
                if len(generations) != 1:
                    break

            # If all checks passed, log state and break while loop
            else:
                for c in range(num_consumers):
                    logging.info("[%s] %s %s: %s", c,
                                 consumers[c]._coordinator.generation,
                                 consumers[c]._coordinator.member_id,
                                 consumers[c].assignment())
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
