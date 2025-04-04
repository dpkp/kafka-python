from contextlib import contextmanager
import gc
import platform
import time
import threading

import pytest

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from test.testutil import env_kafka_version, random_string, maybe_skip_unsupported_compression


@contextmanager
def producer_factory(**kwargs):
    producer = KafkaProducer(**kwargs)
    try:
        yield producer
    finally:
        producer.close(timeout=0)


@contextmanager
def consumer_factory(**kwargs):
    consumer = KafkaConsumer(**kwargs)
    try:
        yield consumer
    finally:
        consumer.close(timeout_ms=0)


@pytest.mark.skipif(not env_kafka_version(), reason="No KAFKA_VERSION set")
@pytest.mark.parametrize("compression", [None, 'gzip', 'snappy', 'lz4', 'zstd'])
def test_end_to_end(kafka_broker, compression):
    maybe_skip_unsupported_compression(compression)
    if compression == 'lz4':
        if env_kafka_version() < (0, 8, 2):
            pytest.skip('LZ4 requires 0.8.2')
        elif platform.python_implementation() == 'PyPy':
            pytest.skip('python-lz4 crashes on older versions of pypy')

    if compression == 'zstd' and env_kafka_version() < (2, 1, 0):
        pytest.skip('zstd requires kafka 2.1.0 or newer')

    connect_str = ':'.join([kafka_broker.host, str(kafka_broker.port)])
    producer_args = {
        'bootstrap_servers': connect_str,
        'retries': 5,
        'max_block_ms': 30000,
        'compression_type': compression,
        'value_serializer': str.encode,
    }
    consumer_args = {
        'bootstrap_servers': connect_str,
        'group_id': None,
        'consumer_timeout_ms': 30000,
        'auto_offset_reset': 'earliest',
        'value_deserializer': bytes.decode,
    }
    with producer_factory(**producer_args) as producer, consumer_factory(**consumer_args) as consumer:
        topic = random_string(5)

        messages = 100
        futures = []
        for i in range(messages):
            futures.append(producer.send(topic, 'msg %d' % i))
        ret = [f.get(timeout=30) for f in futures]
        assert len(ret) == messages

        consumer.subscribe([topic])
        msgs = set()
        for i in range(messages):
            try:
                msgs.add(next(consumer).value)
            except StopIteration:
                break

        assert msgs == set(['msg %d' % (i,) for i in range(messages)])


@pytest.mark.skipif(platform.python_implementation() != 'CPython',
                    reason='Test relies on CPython-specific gc policies')
def test_kafka_producer_gc_cleanup():
    gc.collect()
    threads = threading.active_count()
    producer = KafkaProducer(api_version='0.9') # set api_version explicitly to avoid auto-detection
    assert threading.active_count() == threads + 1
    del(producer)
    gc.collect()
    assert threading.active_count() == threads


@pytest.mark.skipif(not env_kafka_version(), reason="No KAFKA_VERSION set")
@pytest.mark.parametrize("compression", [None, 'gzip', 'snappy', 'lz4', 'zstd'])
def test_kafka_producer_proper_record_metadata(kafka_broker, compression):
    maybe_skip_unsupported_compression(compression)
    if compression == 'zstd' and env_kafka_version() < (2, 1, 0):
        pytest.skip('zstd requires 2.1.0 or more')
    connect_str = ':'.join([kafka_broker.host, str(kafka_broker.port)])
    with producer_factory(bootstrap_servers=connect_str,
                          retries=5,
                          max_block_ms=30000,
                          compression_type=compression) as producer:
        magic = producer._max_usable_produce_magic()

        # record headers are supported in 0.11.0
        if env_kafka_version() < (0, 11, 0):
            headers = None
        else:
            headers = [("Header Key", b"Header Value")]

        topic = random_string(5)
        future = producer.send(
            topic,
            value=b"Simple value", key=b"Simple key", headers=headers, timestamp_ms=9999999,
            partition=0)
        record = future.get(timeout=5)
        assert record is not None
        assert record.topic == topic
        assert record.partition == 0
        assert record.topic_partition == TopicPartition(topic, 0)
        assert record.offset == 0
        if magic >= 1:
            assert record.timestamp == 9999999
        else:
            assert record.timestamp == -1  # NO_TIMESTAMP

        if magic >= 2:
            assert record.checksum is None
        elif magic == 1:
            assert record.checksum == 1370034956
        else:
            assert record.checksum == 3296137851

        assert record.serialized_key_size == 10
        assert record.serialized_value_size == 12
        if headers:
            assert record.serialized_header_size == 22

        if magic == 0:
            pytest.skip('generated timestamp case is skipped for broker 0.9 and below')
        send_time = time.time() * 1000
        future = producer.send(
            topic,
            value=b"Simple value", key=b"Simple key", timestamp_ms=None,
            partition=0)
        record = future.get(timeout=5)
        assert abs(record.timestamp - send_time) <= 1000  # Allow 1s deviation
