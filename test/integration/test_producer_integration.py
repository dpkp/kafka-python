from __future__ import absolute_import

from contextlib import contextmanager
import platform
import time

import pytest

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition, OffsetAndMetadata
from test.testutil import env_kafka_version, random_string, maybe_skip_unsupported_compression


@contextmanager
def producer_factory(**kwargs):
    producer = KafkaProducer(**kwargs)
    try:
        yield producer
    finally:
        producer.close(timeout=1)


@contextmanager
def consumer_factory(**kwargs):
    consumer = KafkaConsumer(**kwargs)
    try:
        yield consumer
    finally:
        consumer.close(timeout_ms=100)


@contextmanager
def admin_factory(**kwargs):
    admin = KafkaAdminClient(**kwargs)
    try:
        yield admin
    finally:
        admin.close()


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
        magic = producer.max_usable_produce_magic(producer.config['api_version'])

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


@pytest.mark.skipif(env_kafka_version() < (0, 11), reason="Idempotent producer requires broker >=0.11")
def test_idempotent_producer(kafka_broker):
    connect_str = ':'.join([kafka_broker.host, str(kafka_broker.port)])
    with producer_factory(bootstrap_servers=connect_str, enable_idempotence=True) as producer:
        for _ in range(10):
            producer.send('idempotent_test_topic', value=b'idempotent_msg').get(timeout=1)


@pytest.mark.skipif(env_kafka_version() < (0, 11), reason="Idempotent producer requires broker >=0.11")
def test_transactional_producer_messages(kafka_broker):
    connect_str = ':'.join([kafka_broker.host, str(kafka_broker.port)])
    with producer_factory(bootstrap_servers=connect_str, transactional_id='testing') as producer:
        producer.init_transactions()
        producer.begin_transaction()
        producer.send('transactional_test_topic', partition=0, value=b'msg1').get()
        producer.send('transactional_test_topic', partition=0, value=b'msg2').get()
        producer.abort_transaction()
        producer.begin_transaction()
        producer.send('transactional_test_topic', partition=0, value=b'msg3').get()
        producer.send('transactional_test_topic', partition=0, value=b'msg4').get()
        producer.commit_transaction()

    messages = set()
    consumer_opts = {
        'bootstrap_servers': connect_str,
        'group_id': None,
        'consumer_timeout_ms': 10000,
        'auto_offset_reset': 'earliest',
        'isolation_level': 'read_committed',
    }
    with consumer_factory(**consumer_opts) as consumer:
        consumer.assign([TopicPartition('transactional_test_topic', 0)])
        for msg in consumer:
            assert msg.value in {b'msg3', b'msg4'}
            messages.add(msg.value)
            if messages == {b'msg3', b'msg4'}:
                break
    assert messages == {b'msg3', b'msg4'}


@pytest.mark.skipif(env_kafka_version() < (0, 11), reason="Idempotent producer requires broker >=0.11")
def test_transactional_producer_offsets(kafka_broker):
    connect_str = ':'.join([kafka_broker.host, str(kafka_broker.port)])
    # Setting leader_epoch only supported in 2.1+
    if env_kafka_version() >= (2, 1):
        leader_epoch = 0
    else:
        leader_epoch = -1
    offsets = {TopicPartition('transactional_test_topic', 0): OffsetAndMetadata(0, 'metadata', leader_epoch)}
    with producer_factory(bootstrap_servers=connect_str, transactional_id='testing') as producer:
        producer.init_transactions()
        producer.begin_transaction()
        producer.send_offsets_to_transaction(offsets, 'txn-test-group')
        producer.commit_transaction()

        producer.begin_transaction()
        producer.send_offsets_to_transaction({TopicPartition('transactional_test_topic', 1): OffsetAndMetadata(1, 'bad', 1)}, 'txn-test-group')
        producer.abort_transaction()

    with admin_factory(bootstrap_servers=connect_str) as admin:
        assert admin.list_consumer_group_offsets('txn-test-group') == offsets
