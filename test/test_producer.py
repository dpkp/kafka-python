import pytest

from kafka import KafkaConsumer, KafkaProducer
from test.conftest import version
from test.testutil import random_string


@pytest.mark.skipif(not version(), reason="No KAFKA_VERSION set")
def test_end_to_end(kafka_broker):
    connect_str = 'localhost:' + str(kafka_broker.port)
    producer = KafkaProducer(bootstrap_servers=connect_str,
                             max_block_ms=10000,
                             value_serializer=str.encode)
    consumer = KafkaConsumer(bootstrap_servers=connect_str,
                             consumer_timeout_ms=10000,
                             auto_offset_reset='earliest',
                             value_deserializer=bytes.decode)

    topic = random_string(5)

    for i in range(1000):
        producer.send(topic, 'msg %d' % i)
    producer.flush()
    producer.close()

    consumer.subscribe([topic])
    msgs = set()
    for i in range(1000):
        try:
            msgs.add(next(consumer).value)
        except StopIteration:
            break

    assert msgs == set(['msg %d' % i for i in range(1000)])
