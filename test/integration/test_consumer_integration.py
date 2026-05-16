import collections
import logging
import threading
import time
from unittest.mock import patch, ANY

import pytest

import kafka.codec
from kafka.coordinator.base import MemberState
from kafka.errors import KafkaTimeoutError, UnsupportedCodecError, UnsupportedVersionError
from kafka.protocol.broker_version_data import BrokerVersionData
from kafka.structs import TopicPartition, OffsetAndTimestamp

from test.testutil import Timer, assert_message_count, env_kafka_version, random_string


def test_consumer(consumer):
    consumer.bootstrap(timeout_ms=500)
    assert consumer._client.cluster.brokers()


def test_consumer_topics(consumer, topic):
    # The `topic` fixture waits for the topic to be visible in broker
    # metadata before returning, so a single poll + fetch is sufficient here.
    consumer.bootstrap(timeout_ms=500)
    assert topic in consumer.topics()
    assert len(consumer.partitions_for_topic(topic)) > 0


def test_paused(kafka_consumer_factory, topic):
    with kafka_consumer_factory(topics=()) as consumer:
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


@pytest.mark.skipif(env_kafka_version() < (0, 10), reason="Requires KAFKA_VERSION >= 0.10")
def test_kafka_version_infer(kafka_consumer_factory):
    with kafka_consumer_factory(api_version=None) as consumer:
        actual = BrokerVersionData(env_kafka_version())
        expected = min((4, 2), actual.broker_version)
        assert consumer.config['api_version'] == expected, \
            "Was expecting inferred broker version to be %s but was %s" % (expected, consumer.config['api_version'])


def test_kafka_consumer(kafka_consumer_factory, send_messages):
    """Test KafkaConsumer"""
    # consumer_timeout_ms must exceed worst-case broker+CI latency for 200
    # records; 30s gives plenty of margin without masking real hangs.
    with kafka_consumer_factory(auto_offset_reset='earliest', consumer_timeout_ms=30000) as consumer:
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


def test_kafka_consumer_unsupported_encoding(
        topic, kafka_producer_factory, kafka_consumer_factory):
    # Send a compressed message
    with kafka_producer_factory(compression_type="gzip") as producer:
        fut = producer.send(topic, b"simple message" * 200)
        fut.get(timeout=5)

    # Consume, but with the related compression codec not available
    with patch.object(kafka.codec, "has_gzip") as mocked:
        mocked.return_value = False
        with kafka_consumer_factory(auto_offset_reset='earliest') as consumer:
            error_msg = "Libraries for gzip compression codec not found"
            with pytest.raises(UnsupportedCodecError, match=error_msg):
                consumer.poll(timeout_ms=2000)


def test_kafka_consumer__blocking(kafka_consumer_factory, topic, send_messages):
    TIMEOUT_MS = 500
    with kafka_consumer_factory(auto_offset_reset='earliest',
                                enable_auto_commit=False,
                                consumer_timeout_ms=TIMEOUT_MS) as consumer:

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

    # Start a consumer and grab the first 180 messages. Use `with` so that
    # close() (and the implicit auto-commit) runs before consumer2 starts.
    with kafka_consumer_factory(
        group_id=GROUP_ID,
        enable_auto_commit=True,
        auto_commit_interval_ms=100,
        auto_offset_reset='earliest',
    ) as consumer1:
        output_msgs1 = []
        for _ in range(180):
            m = next(consumer1)
            output_msgs1.append(m)
        assert_message_count(output_msgs1, 180)

    # Start a second consumer to grab 181-200
    with kafka_consumer_factory(
        group_id=GROUP_ID,
        enable_auto_commit=True,
        auto_commit_interval_ms=100,
        auto_offset_reset='earliest',
    ) as consumer2:
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
    with kafka_consumer_factory(
        auto_offset_reset='earliest', fetch_max_bytes=300) as consumer:
        seen_partitions = set()
        for i in range(90):
            poll_res = consumer.poll(timeout_ms=100)
            for partition, msgs in poll_res.items():
                for msg in msgs:
                    seen_partitions.add(partition)
            if seen_partitions == {TopicPartition(topic, 0), TopicPartition(topic, 1)}:
                break

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
    with kafka_consumer_factory(
        group_id=group,
        auto_offset_reset='earliest',
        consumer_timeout_ms=5000,
        fetch_max_bytes=1) as consumer:

        fetched_msgs = [next(consumer) for i in range(10)]
        assert_message_count(fetched_msgs, 10)


@pytest.mark.skipif(env_kafka_version() < (0, 10, 1), reason="Requires KAFKA_VERSION >= 0.10.1")
def test_kafka_consumer_offsets_for_time(topic, consumer, producer):
    late_time = int(time.time()) * 1000
    middle_time = late_time - 1000
    early_time = late_time - 2000
    tp = TopicPartition(topic, 0)

    timeout = 10
    early_msg = producer.send(
        topic, partition=0, value=b"first",
        timestamp_ms=early_time).get(timeout)
    late_msg = producer.send(
        topic, partition=0, value=b"last",
        timestamp_ms=late_time).get(timeout)

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
def test_kafka_consumer_offsets_search_many_partitions(consumer, producer, topic):
    tp0 = TopicPartition(topic, 0)
    tp1 = TopicPartition(topic, 1)

    send_time = int(time.time() * 1000)
    timeout = 10
    p0msg = producer.send(
        topic, partition=0, value=b"XXX",
        timestamp_ms=send_time).get(timeout)
    p1msg = producer.send(
        topic, partition=1, value=b"XXX",
        timestamp_ms=send_time).get(timeout)

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


@pytest.mark.skipif(env_kafka_version() >= (0, 10, 1), reason="Requires KAFKA_VERSION < 0.10.1")
def test_kafka_consumer_offsets_for_time_old(consumer, topic):
    tp = TopicPartition(topic, 0)

    with pytest.raises(UnsupportedVersionError):
        consumer.offsets_for_times({tp: int(time.time())})


@pytest.mark.skipif(env_kafka_version() < (0, 10, 1), reason="Requires KAFKA_VERSION >= 0.10.1")
def test_kafka_consumer_offsets_for_times_errors(kafka_consumer_factory, topic):
    with kafka_consumer_factory(fetch_max_wait_ms=200,
                                request_timeout_ms=500) as consumer:
        tp = TopicPartition(topic, 0)
        bad_tp = TopicPartition(topic, 100)

        with pytest.raises(ValueError):
            consumer.offsets_for_times({tp: -1})

        with pytest.raises(KafkaTimeoutError):
            consumer.offsets_for_times({bad_tp: 0})


def test_kafka_consumer_position_after_seek_to_end(kafka_consumer_factory, topic, send_messages):
    send_messages(range(0, 10), partition=0)

    # Start a consumer with manual partition assignment.
    with kafka_consumer_factory(
        topics=(),
        group_id=None,
        enable_auto_commit=False,
    ) as consumer:
        tp = TopicPartition(topic, 0)
        consumer.assign([tp])

        # Seek to the end of the partition, and call position() to synchronize
        # the partition's offset without calling poll().
        consumer.seek_to_end(tp)
        position = consumer.position(tp, timeout_ms=1000)

        # Verify we got the expected position
        assert position == 10, "Expected position 10, got {}".format(position)


# Consumer group tests (use group_id, exercise the coordinator/join path)


@pytest.mark.skipif(env_kafka_version() < (0, 9), reason='Unsupported Kafka Version')
def test_group(kafka_consumer_factory, topic):
    num_partitions = 4
    consumers = {}
    stop = {}
    threads = {}
    messages = collections.defaultdict(lambda: collections.defaultdict(list))
    group_id = 'test-group-' + random_string(6)
    def consumer_thread(i):
        assert i not in consumers
        assert i not in stop
        stop[i] = threading.Event()
        with kafka_consumer_factory(group_id=group_id,
                                    client_id="consumer_thread-%s" % i,
                                    api_version_auto_timeout_ms=5000) as c:
            consumers[i] = c
            while not stop[i].is_set():
                for tp, records in consumers[i].poll(timeout_ms=200).items():
                    messages[i][tp].extend(records)
        consumers[i] = None
        stop[i] = None

    num_consumers = 4
    for i in range(num_consumers):
        t = threading.Thread(target=consumer_thread, args=(i,))
        t.daemon = True
        t.start()
        threads[i] = t

    try:
        timeout = time.monotonic() + 15
        while True:
            assert time.monotonic() < timeout, "timeout waiting for assignments"
            # Verify all consumers have been created
            missing_consumers =  set(range(num_consumers)) - set(consumers.keys())
            if missing_consumers:
                logging.info('Waiting on consumer threads: %s', missing_consumers)
                time.sleep(1)
                continue

            unassigned_consumers = {c for c, consumer in consumers.items() if not consumer.assignment()}
            if unassigned_consumers:
                logging.info('Waiting for consumer assignments: %s', unassigned_consumers)
                time.sleep(1)
                continue

            # If all consumers exist and have an assignment
            logging.info('All consumers have assignment... checking for stable group')
            # Verify all consumers are in the same generation
            # then log state and break while loop
            generations = set([consumer._coordinator._generation.generation_id
                               for consumer in consumers.values()])

            # New generation assignment is not complete until
            # coordinator.rejoining = False
            rejoining = set([c for c, consumer in consumers.items() if consumer._coordinator.rejoining])

            if not rejoining and len(generations) == 1:
                for c, consumer in consumers.items():
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

        logging.info('Verifying heartbeats')
        while True:
            for c in range(num_consumers):
                heartbeat = consumers[c]._coordinator.heartbeat
                last_hb = time.monotonic() - 0.5
                if (heartbeat.heartbeat_failed or
                    heartbeat.last_receive < last_hb or
                    heartbeat.last_reset > last_hb):
                    time.sleep(0.1)
                    continue
            else:
                break
        logging.info('Heartbeats look good')

    finally:
        logging.info('Shutting down %s consumers', num_consumers)
        for c in range(num_consumers):
            logging.info('Stopping consumer %s', c)
            stop[c].set()
            threads[c].join(timeout=5)
            assert not threads[c].is_alive()
            threads[c] = None


@pytest.mark.skipif(env_kafka_version() < (0, 9), reason='Unsupported Kafka Version')
def test_heartbeat_thread(kafka_consumer_factory):
    group_id = 'test-group-' + random_string(6)
    with kafka_consumer_factory(group_id=group_id) as consumer:

        # poll until we have joined group / have assignment
        start = time.monotonic()
        while not consumer.assignment():
            consumer.poll(timeout_ms=100)

        assert consumer._coordinator.state is MemberState.STABLE
        last_poll = consumer._coordinator.heartbeat.last_poll

        # wait until we receive first heartbeat
        while consumer._coordinator.heartbeat.last_receive < start:
            time.sleep(0.1)

        last_send = consumer._coordinator.heartbeat.last_send
        last_recv = consumer._coordinator.heartbeat.last_receive
        assert last_poll > start
        assert last_send > start
        assert last_recv > start

        timeout = time.monotonic() + 30
        while True:
            if time.monotonic() > timeout:
                raise RuntimeError('timeout waiting for heartbeat')
            if consumer._coordinator.heartbeat.last_receive > last_recv:
                break
            time.sleep(0.5)

        assert consumer._coordinator.heartbeat.last_poll == last_poll
        consumer.poll(timeout_ms=100)
        assert consumer._coordinator.heartbeat.last_poll > last_poll
