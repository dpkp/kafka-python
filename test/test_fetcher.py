# pylint: skip-file
from __future__ import absolute_import

import pytest

from collections import OrderedDict
import itertools
import time

from kafka.client_async import KafkaClient
from kafka.consumer.fetcher import (
    CompletedFetch, ConsumerRecord, Fetcher, NoOffsetForPartitionError
)
from kafka.consumer.subscription_state import SubscriptionState
from kafka.future import Future
from kafka.metrics import Metrics
from kafka.protocol.fetch import FetchRequest, FetchResponse
from kafka.protocol.offset import OffsetResponse
from kafka.errors import (
    StaleMetadata, LeaderNotAvailableError, NotLeaderForPartitionError,
    UnknownTopicOrPartitionError, OffsetOutOfRangeError
)
from kafka.record.memory_records import MemoryRecordsBuilder, MemoryRecords
from kafka.structs import OffsetAndMetadata, TopicPartition


@pytest.fixture
def client(mocker):
    return mocker.Mock(spec=KafkaClient(bootstrap_servers=(), api_version=(0, 9)))


@pytest.fixture
def subscription_state():
    return SubscriptionState()


@pytest.fixture
def topic():
    return 'foobar'


@pytest.fixture
def fetcher(client, subscription_state, topic):
    subscription_state.subscribe(topics=[topic])
    assignment = [TopicPartition(topic, i) for i in range(3)]
    subscription_state.assign_from_subscribed(assignment)
    for tp in assignment:
        subscription_state.seek(tp, 0)
    return Fetcher(client, subscription_state, Metrics())


def _build_record_batch(msgs, compression=0):
    builder = MemoryRecordsBuilder(
        magic=1, compression_type=0, batch_size=9999999)
    for msg in msgs:
        key, value, timestamp = msg
        builder.append(key=key, value=value, timestamp=timestamp, headers=[])
    builder.close()
    return builder.buffer()


def test_send_fetches(fetcher, topic, mocker):
    fetch_requests = [
        FetchRequest[0](
            -1, fetcher.config['fetch_max_wait_ms'],
            fetcher.config['fetch_min_bytes'],
            [(topic, [
                (0, 0, fetcher.config['max_partition_fetch_bytes']),
                (1, 0, fetcher.config['max_partition_fetch_bytes']),
            ])]),
        FetchRequest[0](
            -1, fetcher.config['fetch_max_wait_ms'],
            fetcher.config['fetch_min_bytes'],
            [(topic, [
                (2, 0, fetcher.config['max_partition_fetch_bytes']),
            ])])
    ]

    mocker.patch.object(fetcher, '_create_fetch_requests',
                        return_value=dict(enumerate(fetch_requests)))

    ret = fetcher.send_fetches()
    for node, request in enumerate(fetch_requests):
        fetcher._client.send.assert_any_call(node, request, wakeup=False)
    assert len(ret) == len(fetch_requests)


@pytest.mark.parametrize(("api_version", "fetch_version"), [
    ((0, 10, 1), 3),
    ((0, 10, 0), 2),
    ((0, 9), 1),
    ((0, 8), 0)
])
def test_create_fetch_requests(fetcher, mocker, api_version, fetch_version):
    fetcher._client.in_flight_request_count.return_value = 0
    fetcher.config['api_version'] = api_version
    by_node = fetcher._create_fetch_requests()
    requests = by_node.values()
    assert all([isinstance(r, FetchRequest[fetch_version]) for r in requests])


def test_update_fetch_positions(fetcher, topic, mocker):
    mocker.patch.object(fetcher, '_reset_offset')
    partition = TopicPartition(topic, 0)

    # unassigned partition
    fetcher.update_fetch_positions([TopicPartition('fizzbuzz', 0)])
    assert fetcher._reset_offset.call_count == 0

    # fetchable partition (has offset, not paused)
    fetcher.update_fetch_positions([partition])
    assert fetcher._reset_offset.call_count == 0

    # partition needs reset, no committed offset
    fetcher._subscriptions.need_offset_reset(partition)
    fetcher._subscriptions.assignment[partition].awaiting_reset = False
    fetcher.update_fetch_positions([partition])
    fetcher._reset_offset.assert_called_with(partition)
    assert fetcher._subscriptions.assignment[partition].awaiting_reset is True
    fetcher.update_fetch_positions([partition])
    fetcher._reset_offset.assert_called_with(partition)

    # partition needs reset, has committed offset
    fetcher._reset_offset.reset_mock()
    fetcher._subscriptions.need_offset_reset(partition)
    fetcher._subscriptions.assignment[partition].awaiting_reset = False
    fetcher._subscriptions.assignment[partition].committed = OffsetAndMetadata(123, b'')
    mocker.patch.object(fetcher._subscriptions, 'seek')
    fetcher.update_fetch_positions([partition])
    assert fetcher._reset_offset.call_count == 0
    fetcher._subscriptions.seek.assert_called_with(partition, 123)


def test__reset_offset(fetcher, mocker):
    tp = TopicPartition("topic", 0)
    fetcher._subscriptions.subscribe(topics="topic")
    fetcher._subscriptions.assign_from_subscribed([tp])
    fetcher._subscriptions.need_offset_reset(tp)
    mocked = mocker.patch.object(fetcher, '_retrieve_offsets')

    mocked.return_value = {tp: (1001, None)}
    fetcher._reset_offset(tp)
    assert not fetcher._subscriptions.assignment[tp].awaiting_reset
    assert fetcher._subscriptions.assignment[tp].position == 1001


def test__send_offset_requests(fetcher, mocker):
    tp = TopicPartition("topic_send_offset", 1)
    mocked_send = mocker.patch.object(fetcher, "_send_offset_request")
    send_futures = []

    def send_side_effect(*args, **kw):
        f = Future()
        send_futures.append(f)
        return f
    mocked_send.side_effect = send_side_effect

    mocked_leader = mocker.patch.object(
        fetcher._client.cluster, "leader_for_partition")
    # First we report unavailable leader 2 times different ways and later
    # always as available
    mocked_leader.side_effect = itertools.chain(
        [None, -1], itertools.cycle([0]))

    # Leader == None
    fut = fetcher._send_offset_requests({tp: 0})
    assert fut.failed()
    assert isinstance(fut.exception, StaleMetadata)
    assert not mocked_send.called

    # Leader == -1
    fut = fetcher._send_offset_requests({tp: 0})
    assert fut.failed()
    assert isinstance(fut.exception, LeaderNotAvailableError)
    assert not mocked_send.called

    # Leader == 0, send failed
    fut = fetcher._send_offset_requests({tp: 0})
    assert not fut.is_done
    assert mocked_send.called
    # Check that we bound the futures correctly to chain failure
    send_futures.pop().failure(NotLeaderForPartitionError(tp))
    assert fut.failed()
    assert isinstance(fut.exception, NotLeaderForPartitionError)

    # Leader == 0, send success
    fut = fetcher._send_offset_requests({tp: 0})
    assert not fut.is_done
    assert mocked_send.called
    # Check that we bound the futures correctly to chain success
    send_futures.pop().success({tp: (10, 10000)})
    assert fut.succeeded()
    assert fut.value == {tp: (10, 10000)}


def test__send_offset_requests_multiple_nodes(fetcher, mocker):
    tp1 = TopicPartition("topic_send_offset", 1)
    tp2 = TopicPartition("topic_send_offset", 2)
    tp3 = TopicPartition("topic_send_offset", 3)
    tp4 = TopicPartition("topic_send_offset", 4)
    mocked_send = mocker.patch.object(fetcher, "_send_offset_request")
    send_futures = []

    def send_side_effect(node_id, timestamps):
        f = Future()
        send_futures.append((node_id, timestamps, f))
        return f
    mocked_send.side_effect = send_side_effect

    mocked_leader = mocker.patch.object(
        fetcher._client.cluster, "leader_for_partition")
    mocked_leader.side_effect = itertools.cycle([0, 1])

    # -- All node succeeded case
    tss = OrderedDict([(tp1, 0), (tp2, 0), (tp3, 0), (tp4, 0)])
    fut = fetcher._send_offset_requests(tss)
    assert not fut.is_done
    assert mocked_send.call_count == 2

    req_by_node = {}
    second_future = None
    for node, timestamps, f in send_futures:
        req_by_node[node] = timestamps
        if node == 0:
            # Say tp3 does not have any messages so it's missing
            f.success({tp1: (11, 1001)})
        else:
            second_future = f
    assert req_by_node == {
        0: {tp1: 0, tp3: 0},
        1: {tp2: 0, tp4: 0}
    }

    # We only resolved 1 future so far, so result future is not yet ready
    assert not fut.is_done
    second_future.success({tp2: (12, 1002), tp4: (14, 1004)})
    assert fut.succeeded()
    assert fut.value == {tp1: (11, 1001), tp2: (12, 1002), tp4: (14, 1004)}

    # -- First succeeded second not
    del send_futures[:]
    fut = fetcher._send_offset_requests(tss)
    assert len(send_futures) == 2
    send_futures[0][2].success({tp1: (11, 1001)})
    send_futures[1][2].failure(UnknownTopicOrPartitionError(tp1))
    assert fut.failed()
    assert isinstance(fut.exception, UnknownTopicOrPartitionError)

    # -- First fails second succeeded
    del send_futures[:]
    fut = fetcher._send_offset_requests(tss)
    assert len(send_futures) == 2
    send_futures[0][2].failure(UnknownTopicOrPartitionError(tp1))
    send_futures[1][2].success({tp1: (11, 1001)})
    assert fut.failed()
    assert isinstance(fut.exception, UnknownTopicOrPartitionError)


def test__handle_offset_response(fetcher, mocker):
    # Broker returns UnsupportedForMessageFormatError, will omit partition
    fut = Future()
    res = OffsetResponse[1]([
        ("topic", [(0, 43, -1, -1)]),
        ("topic", [(1, 0, 1000, 9999)])
    ])
    fetcher._handle_offset_response(fut, res)
    assert fut.succeeded()
    assert fut.value == {TopicPartition("topic", 1): (9999, 1000)}

    # Broker returns NotLeaderForPartitionError
    fut = Future()
    res = OffsetResponse[1]([
        ("topic", [(0, 6, -1, -1)]),
    ])
    fetcher._handle_offset_response(fut, res)
    assert fut.failed()
    assert isinstance(fut.exception, NotLeaderForPartitionError)

    # Broker returns UnknownTopicOrPartitionError
    fut = Future()
    res = OffsetResponse[1]([
        ("topic", [(0, 3, -1, -1)]),
    ])
    fetcher._handle_offset_response(fut, res)
    assert fut.failed()
    assert isinstance(fut.exception, UnknownTopicOrPartitionError)

    # Broker returns many errors and 1 result
    # Will fail on 1st error and return
    fut = Future()
    res = OffsetResponse[1]([
        ("topic", [(0, 43, -1, -1)]),
        ("topic", [(1, 6, -1, -1)]),
        ("topic", [(2, 3, -1, -1)]),
        ("topic", [(3, 0, 1000, 9999)])
    ])
    fetcher._handle_offset_response(fut, res)
    assert fut.failed()
    assert isinstance(fut.exception, NotLeaderForPartitionError)


def test_fetched_records(fetcher, topic, mocker):
    fetcher.config['check_crcs'] = False
    tp = TopicPartition(topic, 0)

    msgs = []
    for i in range(10):
        msgs.append((None, b"foo", None))
    completed_fetch = CompletedFetch(
        tp, 0, 0, [0, 100, _build_record_batch(msgs)],
        mocker.MagicMock()
    )
    fetcher._completed_fetches.append(completed_fetch)
    records, partial = fetcher.fetched_records()
    assert tp in records
    assert len(records[tp]) == len(msgs)
    assert all(map(lambda x: isinstance(x, ConsumerRecord), records[tp]))
    assert partial is False


@pytest.mark.parametrize(("fetch_request", "fetch_response", "num_partitions"), [
    (
        FetchRequest[0](
            -1, 100, 100,
            [('foo', [(0, 0, 1000),])]),
        FetchResponse[0](
            [("foo", [(0, 0, 1000, [(0, b'xxx'),])]),]),
        1,
    ),
    (
        FetchRequest[1](
            -1, 100, 100,
            [('foo', [(0, 0, 1000), (1, 0, 1000),])]),
        FetchResponse[1](
            0,
            [("foo", [
                (0, 0, 1000, [(0, b'xxx'),]),
                (1, 0, 1000, [(0, b'xxx'),]),
            ]),]),
        2,
    ),
    (
        FetchRequest[2](
            -1, 100, 100,
            [('foo', [(0, 0, 1000),])]),
        FetchResponse[2](
            0, [("foo", [(0, 0, 1000, [(0, b'xxx'),])]),]),
        1,
    ),
    (
        FetchRequest[3](
            -1, 100, 100, 10000,
            [('foo', [(0, 0, 1000),])]),
        FetchResponse[3](
            0, [("foo", [(0, 0, 1000, [(0, b'xxx'),])]),]),
        1,
    ),
    (
        FetchRequest[4](
            -1, 100, 100, 10000, 0,
            [('foo', [(0, 0, 1000),])]),
        FetchResponse[4](
            0, [("foo", [(0, 0, 1000, 0, [], [(0, b'xxx'),])]),]),
        1,
    ),
    (
        # This may only be used in broker-broker api calls
        FetchRequest[5](
            -1, 100, 100, 10000, 0,
            [('foo', [(0, 0, 1000),])]),
        FetchResponse[5](
            0, [("foo", [(0, 0, 1000, 0, 0, [], [(0, b'xxx'),])]),]),
        1,
    ),
])
def test__handle_fetch_response(fetcher, fetch_request, fetch_response, num_partitions):
    fetcher._handle_fetch_response(fetch_request, time.time(), fetch_response)
    assert len(fetcher._completed_fetches) == num_partitions


def test__unpack_message_set(fetcher):
    fetcher.config['check_crcs'] = False
    tp = TopicPartition('foo', 0)
    messages = [
        (None, b"a", None),
        (None, b"b", None),
        (None, b"c", None),
    ]
    memory_records = MemoryRecords(_build_record_batch(messages))
    records = list(fetcher._unpack_message_set(tp, memory_records))
    assert len(records) == 3
    assert all(map(lambda x: isinstance(x, ConsumerRecord), records))
    assert records[0].value == b'a'
    assert records[1].value == b'b'
    assert records[2].value == b'c'
    assert records[0].offset == 0
    assert records[1].offset == 1
    assert records[2].offset == 2


def test__message_generator(fetcher, topic, mocker):
    fetcher.config['check_crcs'] = False
    tp = TopicPartition(topic, 0)
    msgs = []
    for i in range(10):
        msgs.append((None, b"foo", None))
    completed_fetch = CompletedFetch(
        tp, 0, 0, [0, 100, _build_record_batch(msgs)],
        mocker.MagicMock()
    )
    fetcher._completed_fetches.append(completed_fetch)
    for i in range(10):
        msg = next(fetcher)
        assert isinstance(msg, ConsumerRecord)
        assert msg.offset == i
        assert msg.value == b'foo'


def test__parse_fetched_data(fetcher, topic, mocker):
    fetcher.config['check_crcs'] = False
    tp = TopicPartition(topic, 0)
    msgs = []
    for i in range(10):
        msgs.append((None, b"foo", None))
    completed_fetch = CompletedFetch(
        tp, 0, 0, [0, 100, _build_record_batch(msgs)],
        mocker.MagicMock()
    )
    partition_record = fetcher._parse_fetched_data(completed_fetch)
    assert isinstance(partition_record, fetcher.PartitionRecords)
    assert len(partition_record) == 10


def test__parse_fetched_data__paused(fetcher, topic, mocker):
    fetcher.config['check_crcs'] = False
    tp = TopicPartition(topic, 0)
    msgs = []
    for i in range(10):
        msgs.append((None, b"foo", None))
    completed_fetch = CompletedFetch(
        tp, 0, 0, [0, 100, _build_record_batch(msgs)],
        mocker.MagicMock()
    )
    fetcher._subscriptions.pause(tp)
    partition_record = fetcher._parse_fetched_data(completed_fetch)
    assert partition_record is None


def test__parse_fetched_data__stale_offset(fetcher, topic, mocker):
    fetcher.config['check_crcs'] = False
    tp = TopicPartition(topic, 0)
    msgs = []
    for i in range(10):
        msgs.append((None, b"foo", None))
    completed_fetch = CompletedFetch(
        tp, 10, 0, [0, 100, _build_record_batch(msgs)],
        mocker.MagicMock()
    )
    partition_record = fetcher._parse_fetched_data(completed_fetch)
    assert partition_record is None


def test__parse_fetched_data__not_leader(fetcher, topic, mocker):
    fetcher.config['check_crcs'] = False
    tp = TopicPartition(topic, 0)
    completed_fetch = CompletedFetch(
        tp, 0, 0, [NotLeaderForPartitionError.errno, -1, None],
        mocker.MagicMock()
    )
    partition_record = fetcher._parse_fetched_data(completed_fetch)
    assert partition_record is None
    fetcher._client.cluster.request_update.assert_called_with()


def test__parse_fetched_data__unknown_tp(fetcher, topic, mocker):
    fetcher.config['check_crcs'] = False
    tp = TopicPartition(topic, 0)
    completed_fetch = CompletedFetch(
        tp, 0, 0, [UnknownTopicOrPartitionError.errno, -1, None],
        mocker.MagicMock()
    )
    partition_record = fetcher._parse_fetched_data(completed_fetch)
    assert partition_record is None
    fetcher._client.cluster.request_update.assert_called_with()


def test__parse_fetched_data__out_of_range(fetcher, topic, mocker):
    fetcher.config['check_crcs'] = False
    tp = TopicPartition(topic, 0)
    completed_fetch = CompletedFetch(
        tp, 0, 0, [OffsetOutOfRangeError.errno, -1, None],
        mocker.MagicMock()
    )
    partition_record = fetcher._parse_fetched_data(completed_fetch)
    assert partition_record is None
    assert fetcher._subscriptions.assignment[tp].awaiting_reset is True


def test_partition_records_offset():
    """Test that compressed messagesets are handle correctly
    when fetch offset is in the middle of the message list
    """
    batch_start = 120
    batch_end = 130
    fetch_offset = 123
    tp = TopicPartition('foo', 0)
    messages = [ConsumerRecord(tp.topic, tp.partition, i,
                               None, None, 'key', 'value', [], 'checksum', 0, 0, -1)
                for i in range(batch_start, batch_end)]
    records = Fetcher.PartitionRecords(fetch_offset, None, messages)
    assert len(records) > 0
    msgs = records.take(1)
    assert msgs[0].offset == fetch_offset
    assert records.fetch_offset == fetch_offset + 1
    msgs = records.take(2)
    assert len(msgs) == 2
    assert len(records) > 0
    records.discard()
    assert len(records) == 0


def test_partition_records_empty():
    records = Fetcher.PartitionRecords(0, None, [])
    assert len(records) == 0


def test_partition_records_no_fetch_offset():
    batch_start = 0
    batch_end = 100
    fetch_offset = 123
    tp = TopicPartition('foo', 0)
    messages = [ConsumerRecord(tp.topic, tp.partition, i,
                               None, None, 'key', 'value', None, 'checksum', 0, 0, -1)
                for i in range(batch_start, batch_end)]
    records = Fetcher.PartitionRecords(fetch_offset, None, messages)
    assert len(records) == 0


def test_partition_records_compacted_offset():
    """Test that messagesets are handle correctly
    when the fetch offset points to a message that has been compacted
    """
    batch_start = 0
    batch_end = 100
    fetch_offset = 42
    tp = TopicPartition('foo', 0)
    messages = [ConsumerRecord(tp.topic, tp.partition, i,
                               None, None, 'key', 'value', None, 'checksum', 0, 0, -1)
                for i in range(batch_start, batch_end) if i != fetch_offset]
    records = Fetcher.PartitionRecords(fetch_offset, None, messages)
    assert len(records) == batch_end - fetch_offset - 1
    msgs = records.take(1)
    assert msgs[0].offset == fetch_offset + 1
