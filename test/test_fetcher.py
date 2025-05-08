# pylint: skip-file
from __future__ import absolute_import
import logging

import pytest

from collections import OrderedDict
import itertools
import time

from kafka.consumer.fetcher import (
    CompletedFetch, ConsumerRecord, Fetcher
)
from kafka.consumer.subscription_state import SubscriptionState
import kafka.errors as Errors
from kafka.future import Future
from kafka.protocol.broker_api_versions import BROKER_API_VERSIONS
from kafka.protocol.fetch import FetchRequest, FetchResponse
from kafka.protocol.list_offsets import ListOffsetsResponse, OffsetResetStrategy
from kafka.errors import (
    StaleMetadata, NotLeaderForPartitionError,
    UnknownTopicOrPartitionError, OffsetOutOfRangeError
)
from kafka.future import Future
from kafka.record.memory_records import MemoryRecordsBuilder, MemoryRecords
from kafka.structs import OffsetAndMetadata, OffsetAndTimestamp, TopicPartition


@pytest.fixture
def subscription_state():
    return SubscriptionState()


@pytest.fixture
def topic():
    return 'foobar'


@pytest.fixture
def fetcher(client, metrics, subscription_state, topic):
    subscription_state.subscribe(topics=[topic])
    assignment = [TopicPartition(topic, i) for i in range(3)]
    subscription_state.assign_from_subscribed(assignment)
    for tp in assignment:
        subscription_state.seek(tp, 0)
    return Fetcher(client, subscription_state, metrics=metrics)


def _build_record_batch(msgs, compression=0, offset=0, magic=2):
    builder = MemoryRecordsBuilder(
        magic=magic, compression_type=0, batch_size=9999999, offset=offset)
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

    def build_fetch_offsets(request):
        fetch_offsets = {}
        for topic, partitions in request.topics:
            for partition_data in partitions:
                partition, offset = partition_data[:2]
                fetch_offsets[TopicPartition(topic, partition)] = offset
        return fetch_offsets

    mocker.patch.object(
        fetcher, '_create_fetch_requests',
        return_value=(dict(enumerate(map(lambda r: (r, build_fetch_offsets(r)), fetch_requests)))))

    mocker.patch.object(fetcher._client, 'ready', return_value=True)
    mocker.patch.object(fetcher._client, 'send')
    ret = fetcher.send_fetches()
    for node, request in enumerate(fetch_requests):
        fetcher._client.send.assert_any_call(node, request, wakeup=False)
    assert len(ret) == len(fetch_requests)


@pytest.mark.parametrize(("api_version", "fetch_version"), [
    ((0, 10, 1), 3),
    ((0, 10, 0), 2),
    ((0, 9), 1),
    ((0, 8, 2), 0)
])
def test_create_fetch_requests(fetcher, mocker, api_version, fetch_version):
    fetcher._client._api_versions = BROKER_API_VERSIONS[api_version]
    mocker.patch.object(fetcher._client.cluster, "leader_for_partition", return_value=0)
    mocker.patch.object(fetcher._client.cluster, "leader_epoch_for_partition", return_value=0)
    mocker.patch.object(fetcher._client, "ready", return_value=True)
    by_node = fetcher._create_fetch_requests()
    requests_and_offsets = by_node.values()
    assert set([r.API_VERSION for (r, _offsets) in requests_and_offsets]) == set([fetch_version])


def test_reset_offsets_if_needed(fetcher, topic, mocker):
    mocker.patch.object(fetcher, '_reset_offsets_async')
    partition = TopicPartition(topic, 0)

    # fetchable partition (has offset, not paused)
    fetcher.reset_offsets_if_needed()
    assert fetcher._reset_offsets_async.call_count == 0

    # partition needs reset, no valid position
    fetcher._subscriptions.request_offset_reset(partition)
    fetcher.reset_offsets_if_needed()
    fetcher._reset_offsets_async.assert_called_with({partition: OffsetResetStrategy.EARLIEST})
    assert fetcher._subscriptions.assignment[partition].awaiting_reset is True
    fetcher.reset_offsets_if_needed()
    fetcher._reset_offsets_async.assert_called_with({partition: OffsetResetStrategy.EARLIEST})

    # partition needs reset, has valid position
    fetcher._reset_offsets_async.reset_mock()
    fetcher._subscriptions.request_offset_reset(partition)
    fetcher._subscriptions.seek(partition, 123)
    fetcher.reset_offsets_if_needed()
    assert fetcher._reset_offsets_async.call_count == 0


def test__reset_offsets_async(fetcher, mocker):
    tp0 = TopicPartition("topic", 0)
    tp1 = TopicPartition("topic", 1)
    fetcher._subscriptions.subscribe(topics=["topic"])
    fetcher._subscriptions.assign_from_subscribed([tp0, tp1])
    fetcher._subscriptions.request_offset_reset(tp0)
    fetcher._subscriptions.request_offset_reset(tp1)
    leaders = {tp0: 0, tp1: 1}
    mocker.patch.object(fetcher._client.cluster, "leader_for_partition", side_effect=lambda tp: leaders[tp])
    mocker.patch.object(fetcher._client, 'ready', return_value=True)
    future1 = Future()
    future2 = Future()
    mocker.patch.object(fetcher, '_send_list_offsets_request', side_effect=[future1, future2])
    fetcher._reset_offsets_async({
        tp0: OffsetResetStrategy.EARLIEST,
        tp1: OffsetResetStrategy.EARLIEST,
    })
    future1.success(({tp0: OffsetAndTimestamp(1001, None, -1)}, set())),
    future2.success(({tp1: OffsetAndTimestamp(1002, None, -1)}, set())),
    assert not fetcher._subscriptions.assignment[tp0].awaiting_reset
    assert not fetcher._subscriptions.assignment[tp1].awaiting_reset
    assert fetcher._subscriptions.assignment[tp0].position.offset == 1001
    assert fetcher._subscriptions.assignment[tp1].position.offset == 1002


def test__send_list_offsets_requests(fetcher, mocker):
    tp = TopicPartition("topic_send_list_offsets", 1)
    mocked_send = mocker.patch.object(fetcher, "_send_list_offsets_request")
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
    mocker.patch.object(fetcher._client.cluster, "leader_epoch_for_partition", return_value=0)

    # Leader == None
    fut = fetcher._send_list_offsets_requests({tp: 0})
    assert fut.failed()
    assert isinstance(fut.exception, StaleMetadata)
    assert not mocked_send.called

    # Leader == -1
    fut = fetcher._send_list_offsets_requests({tp: 0})
    assert fut.failed()
    assert isinstance(fut.exception, StaleMetadata)
    assert not mocked_send.called

    # Leader == 0, send failed
    fut = fetcher._send_list_offsets_requests({tp: 0})
    assert not fut.is_done
    assert mocked_send.called
    # Check that we bound the futures correctly to chain failure
    send_futures.pop().failure(NotLeaderForPartitionError(tp))
    assert fut.failed()
    assert isinstance(fut.exception, NotLeaderForPartitionError)

    # Leader == 0, send success
    fut = fetcher._send_list_offsets_requests({tp: 0})
    assert not fut.is_done
    assert mocked_send.called
    # Check that we bound the futures correctly to chain success
    send_futures.pop().success(({tp: (10, 10000)}, set()))
    assert fut.succeeded()
    assert fut.value == ({tp: (10, 10000)}, set())


def test__send_list_offsets_requests_multiple_nodes(fetcher, mocker):
    tp1 = TopicPartition("topic_send_list_offsets", 1)
    tp2 = TopicPartition("topic_send_list_offsets", 2)
    tp3 = TopicPartition("topic_send_list_offsets", 3)
    tp4 = TopicPartition("topic_send_list_offsets", 4)
    mocked_send = mocker.patch.object(fetcher, "_send_list_offsets_request")
    send_futures = []

    def send_side_effect(node_id, timestamps):
        f = Future()
        send_futures.append((node_id, timestamps, f))
        return f
    mocked_send.side_effect = send_side_effect

    mocked_leader = mocker.patch.object(
        fetcher._client.cluster, "leader_for_partition")
    mocked_leader.side_effect = itertools.cycle([0, 1])
    mocker.patch.object(fetcher._client.cluster, "leader_epoch_for_partition", return_value=0)

    # -- All node succeeded case
    tss = OrderedDict([(tp1, 0), (tp2, 0), (tp3, 0), (tp4, 0)])
    fut = fetcher._send_list_offsets_requests(tss)
    assert not fut.is_done
    assert mocked_send.call_count == 2

    req_by_node = {}
    second_future = None
    for node, timestamps, f in send_futures:
        req_by_node[node] = timestamps
        if node == 0:
            # Say tp3 does not have any messages so it's missing
            f.success(({tp1: (11, 1001)}, set()))
        else:
            second_future = f
    assert req_by_node == {
        0: {tp1: (0, -1), tp3: (0, -1)},
        1: {tp2: (0, -1), tp4: (0, -1)}
    }

    # We only resolved 1 future so far, so result future is not yet ready
    assert not fut.is_done
    second_future.success(({tp2: (12, 1002), tp4: (14, 1004)}, set()))
    assert fut.succeeded()
    assert fut.value == ({tp1: (11, 1001), tp2: (12, 1002), tp4: (14, 1004)}, set())

    # -- First succeeded second not
    del send_futures[:]
    fut = fetcher._send_list_offsets_requests(tss)
    assert len(send_futures) == 2
    send_futures[0][2].success(({tp1: (11, 1001)}, set()))
    send_futures[1][2].failure(UnknownTopicOrPartitionError(tp1))
    assert fut.failed()
    assert isinstance(fut.exception, UnknownTopicOrPartitionError)

    # -- First fails second succeeded
    del send_futures[:]
    fut = fetcher._send_list_offsets_requests(tss)
    assert len(send_futures) == 2
    send_futures[0][2].failure(UnknownTopicOrPartitionError(tp1))
    send_futures[1][2].success(({tp1: (11, 1001)}, set()))
    assert fut.failed()
    assert isinstance(fut.exception, UnknownTopicOrPartitionError)


def test__handle_list_offsets_response_v1(fetcher, mocker):
    # Broker returns UnsupportedForMessageFormatError, will omit partition
    fut = Future()
    res = ListOffsetsResponse[1]([
        ("topic", [(0, 43, -1, -1)]),
        ("topic", [(1, 0, 1000, 9999)])
    ])
    fetcher._handle_list_offsets_response(fut, res)
    assert fut.succeeded()
    assert fut.value == ({TopicPartition("topic", 1): OffsetAndTimestamp(9999, 1000, -1)}, set())

    # Broker returns NotLeaderForPartitionError
    fut = Future()
    res = ListOffsetsResponse[1]([
        ("topic", [(0, 6, -1, -1)]),
    ])
    fetcher._handle_list_offsets_response(fut, res)
    assert fut.succeeded()
    assert fut.value == ({}, set([TopicPartition("topic", 0)]))

    # Broker returns UnknownTopicOrPartitionError
    fut = Future()
    res = ListOffsetsResponse[1]([
        ("topic", [(0, 3, -1, -1)]),
    ])
    fetcher._handle_list_offsets_response(fut, res)
    assert fut.succeeded()
    assert fut.value == ({}, set([TopicPartition("topic", 0)]))

    # Broker returns many errors and 1 result
    fut = Future()
    res = ListOffsetsResponse[1]([
        ("topic", [(0, 43, -1, -1)]), # not retriable
        ("topic", [(1, 6, -1, -1)]),  # retriable
        ("topic", [(2, 3, -1, -1)]),  # retriable
        ("topic", [(3, 0, 1000, 9999)])
    ])
    fetcher._handle_list_offsets_response(fut, res)
    assert fut.succeeded()
    assert fut.value == ({TopicPartition("topic", 3): OffsetAndTimestamp(9999, 1000, -1)},
                         set([TopicPartition("topic", 1), TopicPartition("topic", 2)]))


def test__handle_list_offsets_response_v2_v3(fetcher, mocker):
    # including a throttle_time shouldnt cause issues
    fut = Future()
    res = ListOffsetsResponse[2](
        123, # throttle_time_ms
        [("topic", [(0, 0, 1000, 9999)])
    ])
    fetcher._handle_list_offsets_response(fut, res)
    assert fut.succeeded()
    assert fut.value == ({TopicPartition("topic", 0): OffsetAndTimestamp(9999, 1000, -1)}, set())

    # v3 response is the same format
    fut = Future()
    res = ListOffsetsResponse[3](
        123, # throttle_time_ms
        [("topic", [(0, 0, 1000, 9999)])
    ])
    fetcher._handle_list_offsets_response(fut, res)
    assert fut.succeeded()
    assert fut.value == ({TopicPartition("topic", 0): OffsetAndTimestamp(9999, 1000, -1)}, set())


def test__handle_list_offsets_response_v4_v5(fetcher, mocker):
    # includes leader_epoch
    fut = Future()
    res = ListOffsetsResponse[4](
        123, # throttle_time_ms
        [("topic", [(0, 0, 1000, 9999, 1234)])
    ])
    fetcher._handle_list_offsets_response(fut, res)
    assert fut.succeeded()
    assert fut.value == ({TopicPartition("topic", 0): OffsetAndTimestamp(9999, 1000, 1234)}, set())

    # v5 response is the same format
    fut = Future()
    res = ListOffsetsResponse[5](
        123, # throttle_time_ms
        [("topic", [(0, 0, 1000, 9999, 1234)])
    ])
    fetcher._handle_list_offsets_response(fut, res)
    assert fut.succeeded()
    assert fut.value == ({TopicPartition("topic", 0): OffsetAndTimestamp(9999, 1000, 1234)}, set())


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


@pytest.mark.parametrize(("fetch_offsets", "fetch_response", "num_partitions"), [
    (
        {TopicPartition('foo', 0): 0},
        FetchResponse[0](
            [("foo", [(0, 0, 1000, [(0, b'xxx'),])]),]),
        1,
    ),
    (
        {TopicPartition('foo', 0): 0, TopicPartition('foo', 1): 0},
        FetchResponse[1](
            0,
            [("foo", [
                (0, 0, 1000, [(0, b'xxx'),]),
                (1, 0, 1000, [(0, b'xxx'),]),
            ]),]),
        2,
    ),
    (
        {TopicPartition('foo', 0): 0},
        FetchResponse[2](
            0, [("foo", [(0, 0, 1000, [(0, b'xxx'),])]),]),
        1,
    ),
    (
        {TopicPartition('foo', 0): 0},
        FetchResponse[3](
            0, [("foo", [(0, 0, 1000, [(0, b'xxx'),])]),]),
        1,
    ),
    (
        {TopicPartition('foo', 0): 0},
        FetchResponse[4](
            0, [("foo", [(0, 0, 1000, 0, [], [(0, b'xxx'),])]),]),
        1,
    ),
    (
        # This may only be used in broker-broker api calls
        {TopicPartition('foo', 0): 0},
        FetchResponse[5](
            0, [("foo", [(0, 0, 1000, 0, 0, [], [(0, b'xxx'),])]),]),
        1,
    ),
])
def test__handle_fetch_response(fetcher, fetch_offsets, fetch_response, num_partitions):
    fetcher._nodes_with_pending_fetch_requests.add(0)
    fetcher._handle_fetch_response(0, fetch_offsets, time.time(), fetch_response)
    assert len(fetcher._completed_fetches) == num_partitions


@pytest.mark.parametrize(("exception", "log_level"), [
(
    Errors.Cancelled(),
    logging.INFO
),
(
    Errors.KafkaError(),
    logging.ERROR
)
])
def test__handle_fetch_error(fetcher, caplog, exception, log_level):
    fetcher._nodes_with_pending_fetch_requests.add(3)
    fetcher._handle_fetch_error(3, exception)
    assert len(caplog.records) == 1
    assert caplog.records[0].levelname == logging.getLevelName(log_level)


def test__unpack_records(mocker):
    tp = TopicPartition('foo', 0)
    messages = [
        (None, b"a", None),
        (None, b"b", None),
        (None, b"c", None),
    ]
    memory_records = MemoryRecords(_build_record_batch(messages))
    part_records = Fetcher.PartitionRecords(0, tp, memory_records)
    records = list(part_records.record_iterator)
    assert len(records) == 3
    assert all(map(lambda x: isinstance(x, ConsumerRecord), records))
    assert records[0].value == b'a'
    assert records[1].value == b'b'
    assert records[2].value == b'c'
    assert records[0].offset == 0
    assert records[1].offset == 1
    assert records[2].offset == 2


def test__unpack_records_corrupted(mocker):
    tp = TopicPartition('foo', 0)
    messages = [
        (None, b"a", None),
        (None, b"b", None),
        (None, b"c", None),
    ]
    memory_records = MemoryRecords(_build_record_batch(messages))
    from kafka.record.default_records import DefaultRecord
    mocker.patch.object(DefaultRecord, 'validate_crc', side_effect=[True, True, False])
    part_records = Fetcher.PartitionRecords(0, tp, memory_records)
    records = part_records.take(10)
    assert len(records) == 2
    with pytest.raises(Errors.CorruptRecordError):
        part_records.take(10)


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
    assert partition_record
    assert len(partition_record.take()) == 10


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
    mocker.patch.object(fetcher._client.cluster, 'request_update')
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
    mocker.patch.object(fetcher._client.cluster, 'request_update')
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


def test_partition_records_offset(mocker):
    """Test that compressed messagesets are handle correctly
    when fetch offset is in the middle of the message list
    """
    batch_start = 120
    batch_end = 130
    fetch_offset = 123
    tp = TopicPartition('foo', 0)
    messages = [(None, b'msg', None) for i in range(batch_start, batch_end)]
    memory_records = MemoryRecords(_build_record_batch(messages, offset=batch_start))
    records = Fetcher.PartitionRecords(fetch_offset, tp, memory_records)
    assert records
    assert records.next_fetch_offset == fetch_offset
    msgs = records.take(1)
    assert msgs[0].offset == fetch_offset
    assert records.next_fetch_offset == fetch_offset + 1
    msgs = records.take(2)
    assert len(msgs) == 2
    assert records
    assert records.next_fetch_offset == fetch_offset + 3
    records.drain()
    assert not records


def test_partition_records_empty(mocker):
    tp = TopicPartition('foo', 0)
    memory_records = MemoryRecords(_build_record_batch([]))
    records = Fetcher.PartitionRecords(0, tp, memory_records)
    msgs = records.take()
    assert len(msgs) == 0
    assert not records


def test_partition_records_no_fetch_offset(mocker):
    batch_start = 0
    batch_end = 100
    fetch_offset = 123
    tp = TopicPartition('foo', 0)
    messages = [(None, b'msg', None) for i in range(batch_start, batch_end)]
    memory_records = MemoryRecords(_build_record_batch(messages, offset=batch_start))
    records = Fetcher.PartitionRecords(fetch_offset, tp, memory_records)
    msgs = records.take()
    assert len(msgs) == 0
    assert not records


def test_partition_records_compacted_offset(mocker):
    """Test that messagesets are handle correctly
    when the fetch offset points to a message that has been compacted
    """
    batch_start = 0
    batch_end = 100
    fetch_offset = 42
    tp = TopicPartition('foo', 0)
    builder = MemoryRecordsBuilder(
        magic=2, compression_type=0, batch_size=9999999)

    for i in range(batch_start, batch_end):
        if i == fetch_offset:
            builder.skip(1)
        else:
            builder.append(key=None, value=b'msg', timestamp=None, headers=[])
    builder.close()
    memory_records = MemoryRecords(builder.buffer())
    records = Fetcher.PartitionRecords(fetch_offset, tp, memory_records)
    msgs = records.take()
    assert len(msgs) == batch_end - fetch_offset - 1
    assert msgs[0].offset == fetch_offset + 1


def test_reset_offsets_paused(subscription_state, client, mocker):
    fetcher = Fetcher(client, subscription_state)
    tp = TopicPartition('foo', 0)
    subscription_state.assign_from_user([tp])
    subscription_state.pause(tp) # paused partition does not have a valid position
    subscription_state.request_offset_reset(tp, OffsetResetStrategy.LATEST)

    fetched_offsets = {tp: OffsetAndTimestamp(10, 1, -1)}
    mocker.patch.object(fetcher._client, 'ready', return_value=True)
    mocker.patch.object(fetcher, '_send_list_offsets_request',
                        return_value=Future().success((fetched_offsets, set())))
    mocker.patch.object(fetcher._client.cluster, "leader_for_partition", return_value=0)
    fetcher.reset_offsets_if_needed()

    assert not subscription_state.is_offset_reset_needed(tp)
    assert not subscription_state.is_fetchable(tp) # because tp is paused
    assert subscription_state.has_valid_position(tp)
    assert subscription_state.position(tp) == OffsetAndMetadata(10, '', -1)


def test_reset_offsets_paused_without_valid(subscription_state, client, mocker):
    fetcher = Fetcher(client, subscription_state)
    tp = TopicPartition('foo', 0)
    subscription_state.assign_from_user([tp])
    subscription_state.pause(tp) # paused partition does not have a valid position
    subscription_state.reset_missing_positions()

    fetched_offsets = {tp: OffsetAndTimestamp(0, 1, -1)}
    mocker.patch.object(fetcher._client, 'ready', return_value=True)
    mocker.patch.object(fetcher, '_send_list_offsets_request',
                        return_value=Future().success((fetched_offsets, set())))
    mocker.patch.object(fetcher._client.cluster, "leader_for_partition", return_value=0)
    fetcher.reset_offsets_if_needed()

    assert not subscription_state.is_offset_reset_needed(tp)
    assert not subscription_state.is_fetchable(tp) # because tp is paused
    assert subscription_state.has_valid_position(tp)
    assert subscription_state.position(tp) == OffsetAndMetadata(0, '', -1)


def test_reset_offsets_paused_with_valid(subscription_state, client, mocker):
    fetcher = Fetcher(client, subscription_state)
    tp = TopicPartition('foo', 0)
    subscription_state.assign_from_user([tp])
    subscription_state.seek(tp, 0)
    subscription_state.assignment[tp].position = OffsetAndMetadata(10, '', -1)
    subscription_state.pause(tp) # paused partition already has a valid position

    mocker.patch.object(fetcher, '_fetch_offsets_by_times', return_value={tp: OffsetAndTimestamp(0, 1, -1)})
    fetcher.reset_offsets_if_needed()

    assert not subscription_state.is_offset_reset_needed(tp)
    assert not subscription_state.is_fetchable(tp) # because tp is paused
    assert subscription_state.has_valid_position(tp)
    assert subscription_state.position(tp) == OffsetAndMetadata(10, '', -1)


def test_fetch_position_after_exception(client, mocker):
    subscription_state = SubscriptionState(offset_reset_strategy='NONE')
    fetcher = Fetcher(client, subscription_state)

    tp0 = TopicPartition('foo', 0)
    tp1 = TopicPartition('foo', 1)
    # verify the advancement in the next fetch offset equals to the number of fetched records when
    # some fetched partitions cause Exception. This ensures that consumer won't lose record upon exception
    subscription_state.assign_from_user([tp0, tp1])
    subscription_state.seek(tp0, 1)
    subscription_state.seek(tp1, 1)

    assert len(fetcher._fetchable_partitions()) == 2

    empty_records = _build_record_batch([], offset=1)
    three_records = _build_record_batch([(None, b'msg', None) for _ in range(3)], offset=1)
    fetcher._completed_fetches.append(
        CompletedFetch(tp1, 1, 0, [0, 100, three_records], mocker.MagicMock()))
    fetcher._completed_fetches.append(
        CompletedFetch(tp0, 1, 0, [1, 100, empty_records], mocker.MagicMock()))
    records, partial = fetcher.fetched_records()

    assert len(records) == 1
    assert tp1 in records
    assert tp0 not in records
    assert len(records[tp1]) == 3
    assert subscription_state.position(tp1).offset == 4

    exceptions = []
    try:
        records, partial = fetcher.fetched_records()
    except Errors.OffsetOutOfRangeError as e:
        exceptions.append(e)

    assert len(exceptions) == 1
    assert isinstance(exceptions[0], Errors.OffsetOutOfRangeError)
    assert exceptions[0].args == ({tp0: 1},)


def test_seek_before_exception(client, mocker):
    subscription_state = SubscriptionState(offset_reset_strategy='NONE')
    fetcher = Fetcher(client, subscription_state, max_poll_records=2)

    tp0 = TopicPartition('foo', 0)
    tp1 = TopicPartition('foo', 1)
    subscription_state.assign_from_user([tp0])
    subscription_state.seek(tp0, 1)

    assert len(fetcher._fetchable_partitions()) == 1

    three_records = _build_record_batch([(None, b'msg', None) for _ in range(3)], offset=1)
    fetcher._completed_fetches.append(
        CompletedFetch(tp0, 1, 0, [0, 100, three_records], mocker.MagicMock()))
    records, partial = fetcher.fetched_records()

    assert len(records) == 1
    assert tp0 in records
    assert len(records[tp0]) == 2
    assert subscription_state.position(tp0).offset == 3

    subscription_state.assign_from_user([tp0, tp1])
    subscription_state.seek(tp1, 1)

    assert len(fetcher._fetchable_partitions()) == 1

    empty_records = _build_record_batch([], offset=1)
    fetcher._completed_fetches.append(
        CompletedFetch(tp1, 1, 0, [1, 100, empty_records], mocker.MagicMock()))
    records, partial = fetcher.fetched_records()

    assert len(records) == 1
    assert tp0 in records
    assert len(records[tp0]) == 1
    assert subscription_state.position(tp0).offset == 4

    subscription_state.seek(tp1, 10)
    # Should not throw OffsetOutOfRangeError after the seek
    records, partial = fetcher.fetched_records()
    assert len(records) == 0
