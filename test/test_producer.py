import gc
import platform
import sys
import threading

import pytest

from kafka import KafkaConsumer, KafkaProducer
from kafka.producer.buffer import SimpleBufferPool
from test.conftest import version
from test.testutil import random_string


def test_buffer_pool():
    pool = SimpleBufferPool(1000, 1000)

    buf1 = pool.allocate(1000, 1000)
    message = ''.join(map(str, range(100)))
    buf1.write(message.encode('utf-8'))
    pool.deallocate(buf1)

    buf2 = pool.allocate(1000, 1000)
    assert buf2.read() == b''


@pytest.mark.skipif(not version(), reason="No KAFKA_VERSION set")
@pytest.mark.parametrize("compression", [None, 'gzip', 'snappy', 'lz4'])
def test_end_to_end(kafka_broker, compression):

    if compression == 'lz4':
        # LZ4 requires 0.8.2
        if version() < (0, 8, 2):
            return
        # LZ4 python libs don't work on python2.6
        elif sys.version_info < (2, 7):
            return

    connect_str = 'localhost:' + str(kafka_broker.port)
    producer = KafkaProducer(bootstrap_servers=connect_str,
                             retries=5,
                             max_block_ms=10000,
                             compression_type=compression,
                             value_serializer=str.encode)
    consumer = KafkaConsumer(bootstrap_servers=connect_str,
                             group_id=None,
                             consumer_timeout_ms=10000,
                             auto_offset_reset='earliest',
                             value_deserializer=bytes.decode)

    topic = random_string(5)

    messages = 100
    futures = []
    for i in range(messages):
        futures.append(producer.send(topic, 'msg %d' % i))
    ret = [f.get(timeout=30) for f in futures]
    assert len(ret) == messages

    producer.close()

    consumer.subscribe([topic])
    msgs = set()
    for i in range(messages):
        try:
            msgs.add(next(consumer).value)
        except StopIteration:
            break

    assert msgs == set(['msg %d' % i for i in range(messages)])


@pytest.mark.skipif(platform.python_implementation() != 'CPython',
                    reason='Test relies on CPython-specific gc policies')
def test_kafka_producer_gc_cleanup():
    threads = threading.active_count()
    producer = KafkaProducer(api_version='0.9') # set api_version explicitly to avoid auto-detection
    assert threading.active_count() == threads + 1
    del(producer)
    gc.collect()
    assert threading.active_count() == threads
