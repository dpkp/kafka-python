import collections
import logging
import threading
import time

import pytest
from kafka.vendor import six

from kafka.conn import ConnectionStates
from kafka.consumer.group import KafkaConsumer
from kafka.coordinator.base import MemberState
from kafka.structs import TopicPartition

from test.testutil import env_kafka_version, random_string


def get_connect_str(kafka_broker):
    return kafka_broker.host + ':' + str(kafka_broker.port)


@pytest.mark.skipif(not env_kafka_version(), reason="No KAFKA_VERSION set")
def test_consumer(kafka_broker, topic):
    # The `topic` fixture is included because
    # 0.8.2 brokers need a topic to function well
    consumer = KafkaConsumer(bootstrap_servers=get_connect_str(kafka_broker))
    consumer.poll(timeout_ms=500)
    assert len(consumer._client._conns) > 0
    node_id = list(consumer._client._conns.keys())[0]
    assert consumer._client._conns[node_id].state is ConnectionStates.CONNECTED
    consumer.close()


@pytest.mark.skipif(not env_kafka_version(), reason="No KAFKA_VERSION set")
def test_consumer_topics(kafka_broker, topic):
    consumer = KafkaConsumer(bootstrap_servers=get_connect_str(kafka_broker))
    # Necessary to drive the IO
    consumer.poll(timeout_ms=500)
    assert topic in consumer.topics()
    assert len(consumer.partitions_for_topic(topic)) > 0
    consumer.close()


@pytest.mark.skipif(env_kafka_version() < (0, 9), reason='Unsupported Kafka Version')
def test_group(kafka_broker, topic):
    num_partitions = 4
    connect_str = get_connect_str(kafka_broker)
    consumers = {}
    stop = {}
    threads = {}
    messages = collections.defaultdict(lambda: collections.defaultdict(list))
    group_id = 'test-group-' + random_string(6)
    def consumer_thread(i):
        assert i not in consumers
        assert i not in stop
        stop[i] = threading.Event()
        consumers[i] = KafkaConsumer(topic,
                                     bootstrap_servers=connect_str,
                                     group_id=group_id,
                                     client_id="consumer_thread-%s" % i,
                                     api_version_auto_timeout_ms=5000,
                                     heartbeat_interval_ms=500)
        while not stop[i].is_set():
            for tp, records in six.iteritems(consumers[i].poll(timeout_ms=200)):
                messages[i][tp].extend(records)
        consumers[i].close(timeout_ms=500)
        consumers[i] = None
        stop[i] = None

    num_consumers = 4
    for i in range(num_consumers):
        t = threading.Thread(target=consumer_thread, args=(i,))
        t.daemon = True
        t.start()
        threads[i] = t

    try:
        timeout = time.time() + 15
        while True:
            assert time.time() < timeout, "timeout waiting for assignments"
            # Verify all consumers have been created
            missing_consumers = set(consumers.keys()) - set(range(num_consumers))
            if missing_consumers:
                logging.info('Waiting on consumer threads: %s', missing_consumers)
                time.sleep(1)
                continue

            unassigned_consumers = {c for c, consumer in six.iteritems(consumers) if not consumer.assignment()}
            if unassigned_consumers:
                logging.info('Waiting for consumer assignments: %s', unassigned_consumers)
                time.sleep(1)
                continue

            # If all consumers exist and have an assignment
            logging.info('All consumers have assignment... checking for stable group')
            # Verify all consumers are in the same generation
            # then log state and break while loop
            generations = set([consumer._coordinator._generation.generation_id
                               for consumer in six.itervalues(consumers)])

            # New generation assignment is not complete until
            # coordinator.rejoining = False
            rejoining = set([c for c, consumer in six.iteritems(consumers) if consumer._coordinator.rejoining])

            if not rejoining and len(generations) == 1:
                for c, consumer in six.iteritems(consumers):
                    logging.info("[%s] %s %s: %s", c,
                                 consumer._coordinator._generation.generation_id,
                                 consumer._coordinator._generation.member_id,
                                 consumer.assignment())
                break
            else:
                logging.info('Rejoining: %s, generations: %s', rejoining, generations)
                time.sleep(1)
                continue

        logging.info('Group stabilized; verifying assignment')
        group_assignment = set()
        for c in range(num_consumers):
            assert len(consumers[c].assignment()) != 0
            assert set.isdisjoint(consumers[c].assignment(), group_assignment)
            group_assignment.update(consumers[c].assignment())

        assert group_assignment == set([
            TopicPartition(topic, partition)
            for partition in range(num_partitions)])
        logging.info('Assignment looks good!')

    finally:
        logging.info('Shutting down %s consumers', num_consumers)
        for c in range(num_consumers):
            logging.info('Stopping consumer %s', c)
            stop[c].set()
            threads[c].join(timeout=5)
            assert not threads[c].is_alive()
            threads[c] = None


@pytest.mark.skipif(not env_kafka_version(), reason="No KAFKA_VERSION set")
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
    consumer.close()


@pytest.mark.skipif(env_kafka_version() < (0, 9), reason='Unsupported Kafka Version')
def test_heartbeat_thread(kafka_broker, topic):
    group_id = 'test-group-' + random_string(6)
    consumer = KafkaConsumer(topic,
                             bootstrap_servers=get_connect_str(kafka_broker),
                             group_id=group_id,
                             heartbeat_interval_ms=500)

    # poll until we have joined group / have assignment
    while not consumer.assignment():
        consumer.poll(timeout_ms=100)

    assert consumer._coordinator.state is MemberState.STABLE
    last_poll = consumer._coordinator.heartbeat.last_poll
    last_beat = consumer._coordinator.heartbeat.last_send

    timeout = time.time() + 30
    while True:
        if time.time() > timeout:
            raise RuntimeError('timeout waiting for heartbeat')
        if consumer._coordinator.heartbeat.last_send > last_beat:
            break
        time.sleep(0.5)

    assert consumer._coordinator.heartbeat.last_poll == last_poll
    consumer.poll(timeout_ms=100)
    assert consumer._coordinator.heartbeat.last_poll > last_poll
    consumer.close(timeout_ms=100)
