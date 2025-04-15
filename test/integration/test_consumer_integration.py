import logging
import time

try:
    from unittest.mock import patch, ANY
except ImportError:
    from mock import patch, ANY
import pytest
from kafka.vendor.six.moves import range

import kafka.codec
from kafka.errors import UnsupportedCodecError, UnsupportedVersionError
from kafka.structs import TopicPartition, OffsetAndTimestamp

from test.testutil import Timer, assert_message_count, env_kafka_version, random_string


@pytest.mark.skipif(not env_kafka_version(), reason="No KAFKA_VERSION set")
@pytest.mark.skipif(env_kafka_version()[:2] > (2, 6, 0), reason="KAFKA_VERSION newer than max inferred version")
def test_kafka_version_infer(kafka_consumer_factory):
    consumer = kafka_consumer_factory()
    actual_ver_major_minor = env_kafka_version()[:2]
    client = consumer._client
    conn = list(client._conns.values())[0]
    inferred_ver_major_minor = conn.check_version()[:2]
    assert actual_ver_major_minor == inferred_ver_major_minor, \
        "Was expecting inferred broker version to be %s but was %s" % (actual_ver_major_minor, inferred_ver_major_minor)


@pytest.mark.skipif(not env_kafka_version(), reason="No KAFKA_VERSION set")
def test_kafka_consumer(kafka_consumer_factory, send_messages):
    """Test KafkaConsumer"""
    consumer = kafka_consumer_factory(auto_offset_reset='earliest', consumer_timeout_ms=2000)
    send_messages(range(0, 100), partition=0)
    send_messages(range(0, 100), partition=1)
    cnt = 0
    messages = {0: [], 1: []}
    for message in consumer:
        logging.debug("Consumed message %s", repr(message))
        cnt += 1
        messages[message.partition].append(message)
        if cnt >= 200:
            break

    assert_message_count(messages[0], 100)
    assert_message_count(messages[1], 100)


@pytest.mark.skipif(not env_kafka_version(), reason="No KAFKA_VERSION set")
def test_kafka_consumer_unsupported_encoding(
        topic, kafka_producer_factory, kafka_consumer_factory):
    # Send a compressed message
    producer = kafka_producer_factory(compression_type="gzip")
    fut = producer.send(topic, b"simple message" * 200)
    fut.get(timeout=5)
    producer.close()

    # Consume, but with the related compression codec not available
    with patch.object(kafka.codec, "has_gzip") as mocked:
        mocked.return_value = False
        consumer = kafka_consumer_factory(auto_offset_reset='earliest')
        error_msg = "Libraries for gzip compression codec not found"
        with pytest.raises(UnsupportedCodecError, match=error_msg):
            consumer.poll(timeout_ms=2000)


@pytest.mark.skipif(not env_kafka_version(), reason="No KAFKA_VERSION set")
def test_kafka_consumer__blocking(kafka_consumer_factory, topic, send_messages):
    TIMEOUT_MS = 500
    consumer = kafka_consumer_factory(auto_offset_reset='earliest',
                                      enable_auto_commit=False,
                                      consumer_timeout_ms=TIMEOUT_MS)

    # Manual assignment avoids overhead of consumer group mgmt
    consumer.unsubscribe()
    consumer.assign([TopicPartition(topic, 0)])

    # Ask for 5 messages, nothing in queue, block 500ms
    with Timer() as t:
        with pytest.raises(StopIteration):
            msg = next(consumer)
    assert t.interval >= (TIMEOUT_MS / 1000.0)

    send_messages(range(0, 10))

    # Ask for 5 messages, 10 in queue. Get 5 back, no blocking
    messages = []
    with Timer() as t:
        for i in range(5):
            msg = next(consumer)
            messages.append(msg)
    assert_message_count(messages, 5)
    assert t.interval < (TIMEOUT_MS / 1000.0)

    # Ask for 10 messages, get 5 back, block 500ms
    messages = []
    with Timer() as t:
        with pytest.raises(StopIteration):
            for i in range(10):
                msg = next(consumer)
                messages.append(msg)
    assert_message_count(messages, 5)
    assert t.interval >= (TIMEOUT_MS / 1000.0)


@pytest.mark.skipif(env_kafka_version() < (0, 8, 1), reason="Requires KAFKA_VERSION >= 0.8.1")
def test_kafka_consumer__offset_commit_resume(kafka_consumer_factory, send_messages):
    GROUP_ID = random_string(10)

    send_messages(range(0, 100), partition=0)
    send_messages(range(100, 200), partition=1)

    # Start a consumer and grab the first 180 messages
    consumer1 = kafka_consumer_factory(
        group_id=GROUP_ID,
        enable_auto_commit=True,
        auto_commit_interval_ms=100,
        auto_offset_reset='earliest',
    )
    output_msgs1 = []
    for _ in range(180):
        m = next(consumer1)
        output_msgs1.append(m)
    assert_message_count(output_msgs1, 180)

    # Normally we let the pytest fixture `kafka_consumer_factory` handle
    # closing as part of its teardown. Here we manually call close() to force
    # auto-commit to occur before the second consumer starts. That way the
    # second consumer only consumes previously unconsumed messages.
    consumer1.close()

    # Start a second consumer to grab 181-200
    consumer2 = kafka_consumer_factory(
        group_id=GROUP_ID,
        enable_auto_commit=True,
        auto_commit_interval_ms=100,
        auto_offset_reset='earliest',
    )
    output_msgs2 = []
    for _ in range(20):
        m = next(consumer2)
        output_msgs2.append(m)
    assert_message_count(output_msgs2, 20)

    # Verify the second consumer wasn't reconsuming messages that the first
    # consumer already saw
    assert_message_count(output_msgs1 + output_msgs2, 200)


@pytest.mark.skipif(env_kafka_version() < (0, 10, 1), reason="Requires KAFKA_VERSION >= 0.10.1")
def test_kafka_consumer_max_bytes_simple(kafka_consumer_factory, topic, send_messages):
    send_messages(range(100, 200), partition=0)
    send_messages(range(200, 300), partition=1)

    # Start a consumer
    consumer = kafka_consumer_factory(
        auto_offset_reset='earliest', fetch_max_bytes=300)
    seen_partitions = set()
    for i in range(90):
        poll_res = consumer.poll(timeout_ms=100)
        for partition, msgs in poll_res.items():
            for msg in msgs:
                seen_partitions.add(partition)

    # Check that we fetched at least 1 message from both partitions
    assert seen_partitions == {TopicPartition(topic, 0), TopicPartition(topic, 1)}


@pytest.mark.skipif(env_kafka_version() < (0, 10, 1), reason="Requires KAFKA_VERSION >= 0.10.1")
def test_kafka_consumer_max_bytes_one_msg(kafka_consumer_factory, send_messages):
    # We send to only 1 partition so we don't have parallel requests to 2
    # nodes for data.
    send_messages(range(100, 200))

    # Start a consumer. FetchResponse_v3 should always include at least 1
    # full msg, so by setting fetch_max_bytes=1 we should get 1 msg at a time
    # But 0.11.0.0 returns 1 MessageSet at a time when the messages are
    # stored in the new v2 format by the broker.
    #
    # DP Note: This is a strange test. The consumer shouldn't care
    # how many messages are included in a FetchResponse, as long as it is
    # non-zero. I would not mind if we deleted this test. It caused
    # a minor headache when testing 0.11.0.0.
    group = 'test-kafka-consumer-max-bytes-one-msg-' + random_string(5)
    consumer = kafka_consumer_factory(
        group_id=group,
        auto_offset_reset='earliest',
        consumer_timeout_ms=5000,
        fetch_max_bytes=1)

    fetched_msgs = [next(consumer) for i in range(10)]
    assert_message_count(fetched_msgs, 10)


@pytest.mark.skipif(env_kafka_version() < (0, 10, 1), reason="Requires KAFKA_VERSION >= 0.10.1")
def test_kafka_consumer_offsets_for_time(topic, kafka_consumer, kafka_producer):
    late_time = int(time.time()) * 1000
    middle_time = late_time - 1000
    early_time = late_time - 2000
    tp = TopicPartition(topic, 0)

    timeout = 10
    early_msg = kafka_producer.send(
        topic, partition=0, value=b"first",
        timestamp_ms=early_time).get(timeout)
    late_msg = kafka_producer.send(
        topic, partition=0, value=b"last",
        timestamp_ms=late_time).get(timeout)

    consumer = kafka_consumer
    offsets = consumer.offsets_for_times({tp: early_time})
    assert len(offsets) == 1
    assert offsets[tp].offset == early_msg.offset
    assert offsets[tp].timestamp == early_time

    offsets = consumer.offsets_for_times({tp: middle_time})
    assert offsets[tp].offset == late_msg.offset
    assert offsets[tp].timestamp == late_time

    offsets = consumer.offsets_for_times({tp: late_time})
    assert offsets[tp].offset == late_msg.offset
    assert offsets[tp].timestamp == late_time

    offsets = consumer.offsets_for_times({})
    assert offsets == {}

    # Out of bound timestamps check

    offsets = consumer.offsets_for_times({tp: 0})
    assert offsets[tp].offset == early_msg.offset
    assert offsets[tp].timestamp == early_time

    offsets = consumer.offsets_for_times({tp: 9999999999999})
    assert offsets[tp] is None

    # Beginning/End offsets

    offsets = consumer.beginning_offsets([tp])
    assert offsets == {tp: early_msg.offset}
    offsets = consumer.end_offsets([tp])
    assert offsets == {tp: late_msg.offset + 1}


@pytest.mark.skipif(env_kafka_version() < (0, 10, 1), reason="Requires KAFKA_VERSION >= 0.10.1")
def test_kafka_consumer_offsets_search_many_partitions(kafka_consumer, kafka_producer, topic):
    tp0 = TopicPartition(topic, 0)
    tp1 = TopicPartition(topic, 1)

    send_time = int(time.time() * 1000)
    timeout = 10
    p0msg = kafka_producer.send(
        topic, partition=0, value=b"XXX",
        timestamp_ms=send_time).get(timeout)
    p1msg = kafka_producer.send(
        topic, partition=1, value=b"XXX",
        timestamp_ms=send_time).get(timeout)

    consumer = kafka_consumer
    offsets = consumer.offsets_for_times({
        tp0: send_time,
        tp1: send_time
    })

    leader_epoch = ANY if env_kafka_version() >= (2, 1) else -1
    assert offsets == {
        tp0: OffsetAndTimestamp(p0msg.offset, send_time, leader_epoch),
        tp1: OffsetAndTimestamp(p1msg.offset, send_time, leader_epoch)
    }

    offsets = consumer.beginning_offsets([tp0, tp1])
    assert offsets == {
        tp0: p0msg.offset,
        tp1: p1msg.offset
    }

    offsets = consumer.end_offsets([tp0, tp1])
    assert offsets == {
        tp0: p0msg.offset + 1,
        tp1: p1msg.offset + 1
    }


@pytest.mark.skipif(not env_kafka_version(), reason="No KAFKA_VERSION set")
@pytest.mark.skipif(env_kafka_version() >= (0, 10, 1), reason="Requires KAFKA_VERSION < 0.10.1")
def test_kafka_consumer_offsets_for_time_old(kafka_consumer, topic):
    consumer = kafka_consumer
    tp = TopicPartition(topic, 0)

    with pytest.raises(UnsupportedVersionError):
        consumer.offsets_for_times({tp: int(time.time())})


@pytest.mark.skipif(env_kafka_version() < (0, 10, 1), reason="Requires KAFKA_VERSION >= 0.10.1")
def test_kafka_consumer_offsets_for_times_errors(kafka_consumer_factory, topic):
    consumer = kafka_consumer_factory(fetch_max_wait_ms=200,
                                      request_timeout_ms=500)
    tp = TopicPartition(topic, 0)
    bad_tp = TopicPartition(topic, 100)

    with pytest.raises(ValueError):
        consumer.offsets_for_times({tp: -1})

    assert consumer.offsets_for_times({bad_tp: 0}) == {bad_tp: None}
