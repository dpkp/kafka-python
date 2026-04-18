from contextlib import contextmanager
import platform
import time

import pytest

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition, OffsetAndMetadata
from test.testutil import env_kafka_version, random_string, maybe_skip_unsupported_compression


@pytest.mark.parametrize("compression", [None, 'gzip', 'snappy', 'lz4', 'zstd'])
def test_end_to_end(kafka_consumer_factory, kafka_producer_factory, compression):
    maybe_skip_unsupported_compression(compression)
    if compression == 'lz4':
        if env_kafka_version() < (0, 8, 2):
            pytest.skip('LZ4 requires 0.8.2')
        elif platform.python_implementation() == 'PyPy':
            pytest.skip('python-lz4 crashes on older versions of pypy')

    if compression == 'zstd' and env_kafka_version() < (2, 1, 0):
        pytest.skip('zstd requires kafka 2.1.0 or newer')

    producer_args = {
        'retries': 5,
        'max_block_ms': 30000,
        'compression_type': compression,
        'value_serializer': str.encode,
    }
    consumer_args = {
        'group_id': None,
        'consumer_timeout_ms': 30000,
        'auto_offset_reset': 'earliest',
        'value_deserializer': bytes.decode,
    }
    producer = kafka_producer_factory(**producer_args)
    consumer = kafka_consumer_factory(topics=(), **consumer_args)
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


@pytest.mark.parametrize("compression", [None, 'gzip', 'snappy', 'lz4', 'zstd'])
def test_kafka_producer_proper_record_metadata(kafka_producer_factory, compression):
    maybe_skip_unsupported_compression(compression)
    if compression == 'zstd' and env_kafka_version() < (2, 1, 0):
        pytest.skip('zstd requires 2.1.0 or more')
    producer = kafka_producer_factory(max_block_ms=30000, compression_type=compression)
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
def test_idempotent_producer(kafka_producer_factory):
    producer = kafka_producer_factory(enable_idempotence=True)
    for _ in range(10):
        producer.send('idempotent_test_topic', value=b'idempotent_msg').get(timeout=1)


@pytest.mark.skipif(env_kafka_version() < (0, 11), reason="Idempotent producer requires broker >=0.11")
def test_transactional_producer_messages(kafka_producer_factory, kafka_consumer_factory):
    producer = kafka_producer_factory(transactional_id='testing')
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
        'group_id': None,
        'consumer_timeout_ms': 10000,
        'auto_offset_reset': 'earliest',
        'isolation_level': 'read_committed',
    }
    consumer = kafka_consumer_factory(topics=(), **consumer_opts)
    consumer.assign([TopicPartition('transactional_test_topic', 0)])
    for msg in consumer:
        assert msg.value in {b'msg3', b'msg4'}
        messages.add(msg.value)
        if messages == {b'msg3', b'msg4'}:
            break
    assert messages == {b'msg3', b'msg4'}


@pytest.mark.skipif(env_kafka_version() < (0, 11), reason="Idempotent producer requires broker >=0.11")
@pytest.mark.parametrize("max_in_flight", [1, 2, 5])
def test_idempotent_producer_max_in_flight(kafka_producer_factory, kafka_consumer_factory, max_in_flight):
    """Test idempotent producer with max_in_flight_requests_per_connection 1-5."""
    producer = kafka_producer_factory(
        enable_idempotence=True,
        max_in_flight_requests_per_connection=max_in_flight,
    )
    topic = random_string(5)
    messages = 100
    futures = []
    for i in range(messages):
        futures.append(producer.send(topic, value=('msg %d' % i).encode()))
    ret = [f.get(timeout=30) for f in futures]
    assert len(ret) == messages

    # Verify ordering: offsets should be monotonically increasing per partition
    partition_offsets = {}
    for metadata in ret:
        offsets = partition_offsets.setdefault(metadata.partition, [])
        offsets.append(metadata.offset)
    for offsets in partition_offsets.values():
        assert offsets == sorted(offsets), "Offsets should be monotonically increasing"

    # Verify all messages are readable
    consumer = kafka_consumer_factory(
        topics=(),
        group_id=None,
        consumer_timeout_ms=30000,
        auto_offset_reset='earliest',
        value_deserializer=bytes.decode,
    )
    consumer.subscribe([topic])
    received = set()
    for _ in range(messages):
        try:
            received.add(next(consumer).value)
        except StopIteration:
            break
    assert received == set('msg %d' % i for i in range(messages))


@pytest.mark.skipif(env_kafka_version() < (0, 11), reason="Idempotent producer requires broker >=0.11")
def test_idempotent_producer_high_throughput(kafka_producer_factory):
    """Test idempotent producer with max_in_flight=5 handles many concurrent batches."""
    producer = kafka_producer_factory(
        enable_idempotence=True,
        max_in_flight_requests_per_connection=5,
        batch_size=1024,  # Small batches to create more in-flight requests
        linger_ms=5,
    )
    topic = random_string(5)
    messages = 500
    futures = []
    for i in range(messages):
        futures.append(producer.send(topic, value=('msg %d' % i).encode(), partition=0))
    ret = [f.get(timeout=30) for f in futures]
    assert len(ret) == messages

    # All offsets should be unique and sequential for partition 0
    offsets = [r.offset for r in ret]
    assert offsets == list(range(messages))


@pytest.mark.skipif(env_kafka_version() < (0, 11), reason="Idempotent producer requires broker >=0.11")
def test_transactional_producer_offsets(kafka_producer_factory, kafka_admin_client_factory):
    # Setting leader_epoch only supported in 2.1+
    if env_kafka_version() >= (2, 1):
        leader_epoch = 0
    else:
        leader_epoch = -1
    topic = 'transactional_test_topic'
    offsets = {TopicPartition(topic, 0): OffsetAndMetadata(0, 'metadata', leader_epoch)}
    producer = kafka_producer_factory(transactional_id='testing')
    producer.init_transactions()
    producer.begin_transaction()
    producer.send(topic, partition=0, value=b'msg1').get()
    producer.send_offsets_to_transaction(offsets, 'txn-test-group')
    producer.commit_transaction()

    producer.begin_transaction()
    producer.send_offsets_to_transaction({TopicPartition(topic, 1): OffsetAndMetadata(1, 'bad', 1)}, 'txn-test-group')
    producer.abort_transaction()

    admin = kafka_admin_client_factory()
    result = {
        topic: {
            0: {
                'committed_offset': 0,
                'error_code': 0,
                'metadata': 'metadata',
            },
        }
    }
    if env_kafka_version() >= (2, 1):
        result[topic][0]['committed_leader_epoch'] = leader_epoch
    assert admin.list_group_offsets('txn-test-group') == result
