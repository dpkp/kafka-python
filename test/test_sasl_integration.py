import logging
import uuid

import pytest

from kafka.admin import NewTopic
from kafka.protocol.metadata import MetadataRequest_v1
from test.testutil import assert_message_count, env_kafka_version, random_string, special_to_underscore


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
    sasl_kafka = kafka_broker_factory(transport="SASL_PLAINTEXT", sasl_mechanism=request.param)[0]
    yield sasl_kafka
    sasl_kafka.child.dump_logs()


def test_admin(request, sasl_kafka):
    topic_name = special_to_underscore(request.node.name + random_string(4))
    admin, = sasl_kafka.get_admin_clients(1)
    admin.create_topics([NewTopic(topic_name, 1, 1)])
    assert topic_name in sasl_kafka.get_topic_names()


def test_produce_and_consume(request, sasl_kafka):
    topic_name = special_to_underscore(request.node.name + random_string(4))
    sasl_kafka.create_topics([topic_name], num_partitions=2)
    producer, = sasl_kafka.get_producers(1)

    messages_and_futures = []  # [(message, produce_future),]
    for i in range(100):
        encoded_msg = "{}-{}-{}".format(i, request.node.name, uuid.uuid4()).encode("utf-8")
        future = producer.send(topic_name, value=encoded_msg, partition=i % 2)
        messages_and_futures.append((encoded_msg, future))
    producer.flush()

    for (msg, f) in messages_and_futures:
        assert f.succeeded()

    consumer, = sasl_kafka.get_consumers(1, [topic_name])
    messages = {0: [], 1: []}
    for i, message in enumerate(consumer, 1):
        logging.debug("Consumed message %s", repr(message))
        messages[message.partition].append(message)
        if i >= 100:
            break

    assert_message_count(messages[0], 50)
    assert_message_count(messages[1], 50)


def test_client(request, sasl_kafka):
    topic_name = special_to_underscore(request.node.name + random_string(4))
    sasl_kafka.create_topics([topic_name], num_partitions=1)

    client, = sasl_kafka.get_clients(1)
    request = MetadataRequest_v1(None)
    client.send(0, request)
    for _ in range(10):
        result = client.poll(timeout_ms=10000)
        if len(result) > 0:
            break
    else:
        raise RuntimeError("Couldn't fetch topic response from Broker.")
    result = result[0]
    assert topic_name in [t[1] for t in result.topics]
