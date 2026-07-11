import logging
import os
import uuid

import pytest

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from kafka.net.manager import KafkaConnectionManager
from kafka.protocol.metadata import MetadataRequest
from test.testutil import assert_message_count, env_kafka_version, random_string, special_to_underscore
from test.integration.fixtures import client_params, create_topics

pytestmark = pytest.mark.skipif("KAFKA_URI" in os.environ, reason="Testing on external Kafka Broker")

@pytest.fixture(
    params=[
        pytest.param(
            "PLAIN", marks=pytest.mark.skipif(env_kafka_version() < (0, 10), reason="Requires KAFKA_VERSION >= 0.10")
        ),
        pytest.param(
            "SCRAM-SHA-256",
            marks=pytest.mark.skipif(env_kafka_version() < (0, 10, 2), reason="Requires KAFKA_VERSION >= 0.10.2"),
        ),
        pytest.param(
            "SCRAM-SHA-512",
            marks=pytest.mark.skipif(env_kafka_version() < (0, 10, 2), reason="Requires KAFKA_VERSION >= 0.10.2"),
        ),
    ]
)
def sasl_kafka(request, kafka_broker_factory):
    sasl_kafka = kafka_broker_factory(transport="SASL_PLAINTEXT", sasl_mechanism=request.param)
    yield sasl_kafka
    sasl_kafka.child.dump_logs()


def test_admin(request, sasl_kafka):
    topic_name = special_to_underscore(request.node.name + random_string(4))
    admin = KafkaAdminClient(**client_params(sasl_kafka, 'admin'))
    admin.create_topics([NewTopic(topic_name, 1, 1)])
    assert topic_name in sasl_kafka.get_topic_names()
    admin.close()


def test_produce_and_consume(request, sasl_kafka):
    topic_name = special_to_underscore(request.node.name + random_string(4))
    create_topics(sasl_kafka, [topic_name], num_partitions=2)
    producer = KafkaProducer(**client_params(sasl_kafka, 'producer'))

    messages_and_futures = []  # [(message, produce_future),]
    for i in range(100):
        encoded_msg = "{}-{}-{}".format(i, request.node.name, uuid.uuid4()).encode("utf-8")
        future = producer.send(topic_name, value=encoded_msg, partition=i % 2)
        messages_and_futures.append((encoded_msg, future))
    producer.flush()
    producer.close()

    for (msg, f) in messages_and_futures:
        assert f.succeeded()

    consumer = KafkaConsumer(topic_name, **client_params(sasl_kafka, 'consumer', auto_offset_reset='earliest'))
    messages = {0: [], 1: []}
    for i, message in enumerate(consumer, 1):
        logging.debug("Consumed message %s", repr(message))
        messages[message.partition].append(message)
        if i >= 100:
            break

    assert_message_count(messages[0], 50)
    assert_message_count(messages[1], 50)
    consumer.close()


def test_client(request, sasl_kafka):
    topic_name = special_to_underscore(request.node.name + random_string(4))
    create_topics(sasl_kafka, [topic_name], num_partitions=1)

    # Low-level SASL round-trip via KafkaConnectionManager directly (no compat
    # shim, no poll()): the started-loop + manager.run(coro) pattern the real
    # clients use, so it runs on any net backend (selector or asyncio).
    manager = KafkaConnectionManager(**client_params(sasl_kafka, 'client'))
    manager._net.start()
    try:
        manager.bootstrap(timeout_ms=5000)

        async def fetch_metadata():
            future = manager.send(MetadataRequest(topics=None, version=1), node_id=None)
            return await manager.wait_for(future, 10000)

        result = manager.run(fetch_metadata)
        assert topic_name in [t[1] for t in result.topics]
    finally:
        manager.close()
        manager._net.close()
