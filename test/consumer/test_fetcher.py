# pylint: skip-file
import logging

import pytest

from collections import OrderedDict
import itertools
import time
from unittest.mock import MagicMock

from kafka.consumer.fetcher import (
    CompletedFetch, ConsumerRecord, Fetcher
)
from kafka.consumer.subscription_state import SubscriptionState
import kafka.errors as Errors
from kafka.future import Future
from kafka.protocol.broker_version_data import BrokerVersionData
from kafka.protocol.consumer import (
    FetchRequest, FetchResponse,
    ListOffsetsResponse, OffsetForLeaderEpochResponse,
    OffsetResetStrategy,
)
from kafka.errors import (
    StaleMetadata, NotLeaderForPartitionError,
    UnknownTopicOrPartitionError, OffsetOutOfRangeError
)
from kafka.future import Future
from kafka.record.memory_records import MemoryRecordsBuilder, MemoryRecords
from kafka.structs import OffsetAndMetadata, OffsetAndTimestamp, TopicPartition


_ResponseTopic = FetchResponse.FetchableTopicResponse
_ResponsePartition = _ResponseTopic.PartitionData
_ListResponseTopic = ListOffsetsResponse.ListOffsetsTopicResponse
_ListResponsePartition = _ListResponseTopic.ListOffsetsPartitionResponse


@pytest.fixture
def subscription_state():
    return SubscriptionState()


@pytest.fixture
def topic():
    return 'foobar'


@pytest.fixture
def assignment(topic):
    return [TopicPartition(topic, i) for i in range(3)]


@pytest.fixture
def fetcher(client, metrics, subscription_state, topic, assignment):
    subscription_state.subscribe(topics=[topic])
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


def _build_completed_fetch(tp, msgs, error=None, offset=0):
    if error is not None:
        partition_data = _ResponsePartition(
            error_code=error.errno,
            high_watermark=-1,
            records=None)
    else:
        partition_data = _ResponsePartition(
                error_code=0,
                high_watermark=100,
                records=_build_record_batch(msgs, offset=offset))
    return CompletedFetch(
        tp, offset, 0, partition_data, MagicMock())


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
    mocker.patch.object(fetcher._manager, 'send')
    ret = fetcher.send_fetches()
    for node, request in enumerate(fetch_requests):
        fetcher._manager.send.assert_any_call(request, node_id=node)
    assert len(ret) == len(fetch_requests)


def test_create_fetch_requests(fetcher, mocker, assignment):
    mocker.patch.object(fetcher._manager.cluster, "leader_for_partition", return_value=0)
    mocker.patch.object(fetcher._manager.cluster, "leader_epoch_for_partition", return_value=0)
    mocker.patch.object(fetcher._client, "ready", return_value=True)
    by_node = fetcher._create_fetch_requests()
    assert len(by_node) == 1
    assert 0 in by_node
    request, offsets = by_node[0]
    assert isinstance(request, FetchRequest)
    requested = set([
        TopicPartition(topic.topic, partition.partition)
        for topic in request.topics
        for partition in topic.partitions
    ])
    assert requested == set(assignment)


def test_reset_offsets_if_needed(fetcher, topic, mocker):
    # Stub call_soon so we can count invocations without actually scheduling.
    # Return a not-yet-done Future so reset_offsets_if_needed's cached-task
    # check (`is_done`) sees it as in-flight.
    call_soon = mocker.patch.object(
        fetcher._client._manager, 'call_soon', side_effect=lambda *a, **kw: Future())
    partition = TopicPartition(topic, 0)

    # fetchable partition (has offset, not paused) -> no reset needed, returns None
    assert fetcher.reset_offsets_if_needed() is None
    assert call_soon.call_count == 0

    # partition needs reset, no valid position -> schedules reset task
    fetcher._subscriptions.request_offset_reset(partition)
    fetcher.reset_offsets_if_needed()
    call_soon.assert_called_with(fetcher._reset_offsets_async, None)
    assert fetcher._subscriptions.assignment[partition].awaiting_reset is True

    # Second call with task still in-flight: returns the cached task; no new schedule.
    call_soon.reset_mock()
    fetcher.reset_offsets_if_needed()
    assert call_soon.call_count == 0

    # partition needs reset, has valid position
    fetcher._subscriptions.request_offset_reset(partition)
    fetcher._subscriptions.seek(partition, 123)
    # Clear the cache to test the has-valid-position fast-path (otherwise
    # the cached in-flight task would short-circuit the check).
    fetcher._reset_task = None
    call_soon.reset_mock()
    assert fetcher.reset_offsets_if_needed() is None
    assert call_soon.call_count == 0


def test__reset_offsets_async_waits_for_metadata_when_leader_unknown(
        fetcher, manager, mocker):
    """If the leader is unknown, _reset_offsets_async should wait for a
    metadata refresh and retry within the timer budget. Regression for the
    test_kafka_consumer_position_after_seek_to_end integration test where
    a manually-assigned partition + seek_to_end + position() call hits
    _reset_offsets_async before metadata has resolved the leader.
    """
    tp = TopicPartition("topic", 0)
    fetcher._subscriptions.subscribe(topics=["topic"])
    fetcher._subscriptions.assign_from_subscribed([tp])
    fetcher._subscriptions.request_offset_reset(tp, OffsetResetStrategy.LATEST)

    # leader_for_partition returns None on first call (leader unknown), then
    # node 0 on subsequent calls (metadata refresh "completed" between).
    leader_call_count = [0]
    def fake_leader(tp_arg):
        leader_call_count[0] += 1
        return None if leader_call_count[0] == 1 else 0
    mocker.patch.object(fetcher._client.cluster, "leader_for_partition",
                        side_effect=fake_leader)

    # request_update returns a future that's already-done (simulates a quick
    # metadata refresh).
    metadata_future = Future().success(fetcher._client.cluster)
    mocker.patch.object(fetcher._client.cluster, "request_update",
                        return_value=metadata_future)
    mocker.patch.object(fetcher._client, 'ready', return_value=True)

    async def fake_send(node_id, timestamps_and_epochs):
        return ({tp: OffsetAndTimestamp(42, None, -1)}, set())
    mocker.patch.object(fetcher, '_send_list_offsets_request', side_effect=fake_send)

    manager.run(fetcher._reset_offsets_async, 1000)

    assert not fetcher._subscriptions.assignment[tp].awaiting_reset
    assert fetcher._subscriptions.assignment[tp].position.offset == 42
    # First call returned None (unknown), second returned 0 (known).
    assert leader_call_count[0] == 2


def test__reset_offsets_async_bails_when_leader_permanently_unknown(
        fetcher, manager, mocker):
    """If metadata refresh doesn't resolve the leader within the timer
    budget, _reset_offsets_async should bail rather than spin forever.
    """
    tp = TopicPartition("topic", 0)
    fetcher._subscriptions.subscribe(topics=["topic"])
    fetcher._subscriptions.assign_from_subscribed([tp])
    fetcher._subscriptions.request_offset_reset(tp, OffsetResetStrategy.LATEST)

    mocker.patch.object(fetcher._client.cluster, "leader_for_partition",
                        return_value=None)
    metadata_future = Future().success(fetcher._client.cluster)
    mocker.patch.object(fetcher._client.cluster, "request_update",
                        return_value=metadata_future)

    # 50ms timer caps the spin even though leader stays unknown forever.
    start = time.monotonic()
    manager.run(fetcher._reset_offsets_async, 50)
    elapsed = time.monotonic() - start
    assert elapsed < 1.0, (
        '_reset_offsets_async did not respect timer budget; took %.2fs' % elapsed)

    # Partition still awaiting reset (we couldn't resolve the leader).
    assert fetcher._subscriptions.assignment[tp].awaiting_reset


def test__reset_offsets_async(fetcher, manager, mocker):
    tp0 = TopicPartition("topic", 0)
    tp1 = TopicPartition("topic", 1)
    fetcher._subscriptions.subscribe(topics=["topic"])
    fetcher._subscriptions.assign_from_subscribed([tp0, tp1])
    fetcher._subscriptions.request_offset_reset(tp0)
    fetcher._subscriptions.request_offset_reset(tp1)
    leaders = {tp0: 0, tp1: 1}
    mocker.patch.object(fetcher._client.cluster, "leader_for_partition", side_effect=lambda tp: leaders[tp])
    mocker.patch.object(fetcher._client, 'ready', return_value=True)

    results = {
        0: ({tp0: OffsetAndTimestamp(1001, None, -1)}, set()),
        1: ({tp1: OffsetAndTimestamp(1002, None, -1)}, set()),
    }
    async def fake_send(node_id, timestamps_and_epochs):
        return results[node_id]
    mocker.patch.object(fetcher, '_send_list_offsets_request', side_effect=fake_send)

    # _reset_offsets_async loops until partitions_needing_reset is empty;
    # manager.run waits for the whole driver to complete.
    manager.run(fetcher._reset_offsets_async, 1000)

    assert not fetcher._subscriptions.assignment[tp0].awaiting_reset
    assert not fetcher._subscriptions.assignment[tp1].awaiting_reset
    assert fetcher._subscriptions.assignment[tp0].position.offset == 1001
    assert fetcher._subscriptions.assignment[tp1].position.offset == 1002


def test__reset_offsets_async_retries_after_retriable_failure(
        fetcher, manager, mocker):
    """A retriable per-partition error (NotLeader, etc.) sets the partition
    into retry_backoff_ms backoff. _reset_offsets_async must sleep for that
    backoff and retry, rather than exit and rely on an outer caller to
    redrive.
    """
    tp = TopicPartition("topic", 0)
    fetcher._subscriptions.subscribe(topics=["topic"])
    fetcher._subscriptions.assign_from_subscribed([tp])
    fetcher._subscriptions.request_offset_reset(tp)
    mocker.patch.object(fetcher._client.cluster, "leader_for_partition",
                        return_value=0)
    mocker.patch.object(fetcher._client, 'ready', return_value=True)

    # Use a small backoff to keep the test fast.
    fetcher.config['retry_backoff_ms'] = 50

    call_count = [0]
    async def fake_send(node_id, timestamps_and_epochs):
        call_count[0] += 1
        if call_count[0] == 1:
            # Simulate NotLeader: empty offsets, partition in retry set.
            return ({}, {tp})
        return ({tp: OffsetAndTimestamp(42, None, -1)}, set())
    mocker.patch.object(fetcher, '_send_list_offsets_request',
                        side_effect=fake_send)

    start = time.monotonic()
    manager.run(fetcher._reset_offsets_async, 1000)
    elapsed = time.monotonic() - start

    assert call_count[0] == 2, (
        'expected two ListOffsets attempts after retriable error, got %d'
        % call_count[0])
    assert not fetcher._subscriptions.assignment[tp].awaiting_reset
    assert fetcher._subscriptions.assignment[tp].position.offset == 42
    assert elapsed >= 0.05, (
        '_reset_offsets_async did not sleep for retry_backoff_ms; %.3fs'
        % elapsed)


def test__send_list_offsets_requests(fetcher, manager, net, mocker):
    tp = TopicPartition("topic_send_list_offsets", 1)

    pending = []
    async def fake_send(node_id, timestamps):
        f = Future()
        pending.append(f)
        return await f
    mocked_send = mocker.patch.object(fetcher, "_send_list_offsets_request", side_effect=fake_send)

    mocked_leader = mocker.patch.object(
        fetcher._client.cluster, "leader_for_partition")
    # First we report unavailable leader 2 times different ways and later
    # always as available
    mocked_leader.side_effect = itertools.chain(
        [None, -1], itertools.cycle([0]))
    mocker.patch.object(fetcher._client.cluster, "leader_epoch_for_partition", return_value=0)


    # Leader == None
    with pytest.raises(StaleMetadata):
        manager.run(fetcher._send_list_offsets_requests, {tp: 0})
    assert not mocked_send.called

    # Leader == -1
    with pytest.raises(StaleMetadata):
        manager.run(fetcher._send_list_offsets_requests, {tp: 0})
    assert not mocked_send.called

    # Leader == 0, send failed
    fut = manager.call_soon(fetcher._send_list_offsets_requests, {tp: 0})
    while not pending:
        net.poll(timeout_ms=10)
    assert not fut.is_done
    assert mocked_send.called
    pending.pop().failure(NotLeaderForPartitionError(tp))
    net.poll(future=fut)
    assert fut.failed()
    assert isinstance(fut.exception, NotLeaderForPartitionError)

    # Leader == 0, send success
    fut = manager.call_soon(fetcher._send_list_offsets_requests, {tp: 0})
    while not pending:
        net.poll(timeout_ms=10)
    assert not fut.is_done
    pending.pop().success(({tp: (10, 10000)}, set()))
    net.poll(future=fut)
    assert fut.succeeded()
    assert fut.value == ({tp: (10, 10000)}, set())


def test__send_list_offsets_requests_multiple_nodes(fetcher, manager, net, mocker):
    tp1 = TopicPartition("topic_send_list_offsets", 1)
    tp2 = TopicPartition("topic_send_list_offsets", 2)
    tp3 = TopicPartition("topic_send_list_offsets", 3)
    tp4 = TopicPartition("topic_send_list_offsets", 4)

    send_futures = []
    async def fake_send(node_id, timestamps):
        f = Future()
        send_futures.append((node_id, timestamps, f))
        return await f
    mocked_send = mocker.patch.object(fetcher, "_send_list_offsets_request", side_effect=fake_send)

    mocked_leader = mocker.patch.object(
        fetcher._client.cluster, "leader_for_partition")
    mocked_leader.side_effect = itertools.cycle([0, 1])
    mocker.patch.object(fetcher._client.cluster, "leader_epoch_for_partition", return_value=0)

    def wait_for_send_futures(n):
        while len(send_futures) < n:
            net.poll(timeout_ms=10)

    # -- All node succeeded case
    tss = OrderedDict([(tp1, 0), (tp2, 0), (tp3, 0), (tp4, 0)])
    fut = manager.call_soon(fetcher._send_list_offsets_requests, tss)
    wait_for_send_futures(2)
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
    net.poll(timeout_ms=10)
    assert not fut.is_done
    second_future.success(({tp2: (12, 1002), tp4: (14, 1004)}, set()))
    net.poll(future=fut)
    assert fut.succeeded()
    assert fut.value == ({tp1: (11, 1001), tp2: (12, 1002), tp4: (14, 1004)}, set())

    # -- First succeeded second not
    del send_futures[:]
    fut = manager.call_soon(fetcher._send_list_offsets_requests, tss)
    wait_for_send_futures(2)
    send_futures[0][2].success(({tp1: (11, 1001)}, set()))
    send_futures[1][2].failure(UnknownTopicOrPartitionError(tp1))
    net.poll(future=fut)
    assert fut.failed()
    assert isinstance(fut.exception, UnknownTopicOrPartitionError)

    # -- First fails second succeeded
    del send_futures[:]
    fut = manager.call_soon(fetcher._send_list_offsets_requests, tss)
    wait_for_send_futures(2)
    send_futures[0][2].failure(UnknownTopicOrPartitionError(tp1))
    send_futures[1][2].success(({tp1: (11, 1001)}, set()))
    net.poll(future=fut)
    assert fut.failed()
    assert isinstance(fut.exception, UnknownTopicOrPartitionError)


def test__handle_list_offsets_response_v1(fetcher, mocker):
    # Broker returns UnsupportedForMessageFormatError, will omit partition
    res = ListOffsetsResponse[1]([
        _ListResponseTopic("topic", [_ListResponsePartition(0, 43, -1, -1, version=1)], version=1),
        _ListResponseTopic("topic", [_ListResponsePartition(1, 0, 1000, 9999, version=1)], version=1)
    ])
    assert fetcher._handle_list_offsets_response(res) == (
        {TopicPartition("topic", 1): OffsetAndTimestamp(9999, 1000, -1)}, set())

    # Broker returns NotLeaderForPartitionError
    res = ListOffsetsResponse[1]([
        _ListResponseTopic("topic", [_ListResponsePartition(0, 6, -1, -1, version=1)], version=1),
    ])
    assert fetcher._handle_list_offsets_response(res) == (
        {}, set([TopicPartition("topic", 0)]))

    # Broker returns UnknownTopicOrPartitionError
    res = ListOffsetsResponse[1]([
        _ListResponseTopic("topic", [_ListResponsePartition(0, 3, -1, -1, version=1)], version=1),
    ])
    assert fetcher._handle_list_offsets_response(res) == (
        {}, set([TopicPartition("topic", 0)]))

    # Broker returns many errors and 1 result
    res = ListOffsetsResponse[1]([
        _ListResponseTopic("topic", [_ListResponsePartition(0, 43, -1, -1, version=1)], version=1), # not retriable
        _ListResponseTopic("topic", [_ListResponsePartition(1, 6, -1, -1, version=1)], version=1),  # retriable
        _ListResponseTopic("topic", [_ListResponsePartition(2, 3, -1, -1, version=1)], version=1),  # retriable
        _ListResponseTopic("topic", [_ListResponsePartition(3, 0, 1000, 9999, version=1)], version=1)
    ])
    assert fetcher._handle_list_offsets_response(res) == (
        {TopicPartition("topic", 3): OffsetAndTimestamp(9999, 1000, -1)},
        set([TopicPartition("topic", 1), TopicPartition("topic", 2)]))


def test__handle_list_offsets_response_v2_v3(fetcher, mocker):
    # including a throttle_time shouldnt cause issues
    res = ListOffsetsResponse[2](
        123, # throttle_time_ms
        [_ListResponseTopic("topic", [_ListResponsePartition(0, 0, 1000, 9999, version=2)], version=2)
    ])
    assert fetcher._handle_list_offsets_response(res) == (
        {TopicPartition("topic", 0): OffsetAndTimestamp(9999, 1000, -1)}, set())

    # v3 response is the same format
    res = ListOffsetsResponse[3](
        123, # throttle_time_ms
        [_ListResponseTopic("topic", [_ListResponsePartition(0, 0, 1000, 9999, version=3)], version=3)
    ])
    assert fetcher._handle_list_offsets_response(res) == (
        {TopicPartition("topic", 0): OffsetAndTimestamp(9999, 1000, -1)}, set())


def test__handle_list_offsets_response_v4_v5(fetcher, mocker):
    # includes leader_epoch
    res = ListOffsetsResponse[4](
        123, # throttle_time_ms
        [_ListResponseTopic("topic", [_ListResponsePartition(0, 0, 1000, 9999, 1234, version=4)], version=4)
    ])
    assert fetcher._handle_list_offsets_response(res) == (
        {TopicPartition("topic", 0): OffsetAndTimestamp(9999, 1000, 1234)}, set())

    # v5 response is the same format
    res = ListOffsetsResponse[5](
        123, # throttle_time_ms
        [_ListResponseTopic("topic", [_ListResponsePartition(0, 0, 1000, 9999, 1234, version=5)], version=5)
    ])
    assert fetcher._handle_list_offsets_response(res) == (
        {TopicPartition("topic", 0): OffsetAndTimestamp(9999, 1000, 1234)}, set())


def test_fetched_records(fetcher, topic, mocker):
    fetcher.config['check_crcs'] = False
    tp = TopicPartition(topic, 0)

    msgs = []
    for i in range(10):
        msgs.append((None, b"foo", None))
    completed_fetch = _build_completed_fetch(tp, msgs)
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
            [_ResponseTopic("foo", [
                _ResponsePartition(0, 0, 1000, _build_record_batch([(None, b'xxx', None)]), version=0)], version=0)]),
        1,
    ),
    (
        {TopicPartition('foo', 0): 0, TopicPartition('foo', 1): 0},
        FetchResponse[1](
            0,
            [_ResponseTopic("foo", [
                _ResponsePartition(0, 0, 1000, _build_record_batch([(None, b'xxx', None)]), version=1),
                _ResponsePartition(1, 0, 1000, _build_record_batch([(None, b'xxx', None)]), version=1)], version=1)]),
        2,
    ),
    (
        {TopicPartition('foo', 0): 0},
        FetchResponse[2](
            0,
            [_ResponseTopic("foo", [
                _ResponsePartition(0, 0, 1000, _build_record_batch([(None, b'xxx', None)]), version=2)], version=2)]),
        1,
    ),
    (
        {TopicPartition('foo', 0): 0},
        FetchResponse[3](
            0,
            [_ResponseTopic("foo", [
                _ResponsePartition(0, 0, 1000, _build_record_batch([(None, b'xxx', None)]), version=3)], version=3)]),
        1,
    ),
    (
        {TopicPartition('foo', 0): 0},
        FetchResponse[4](
            0,
            [_ResponseTopic("foo", [
                _ResponsePartition(0, 0, 1000, 0, [], _build_record_batch([(None, b'xxx', None)]), version=4)], version=4)]),
        1,
    ),
    (
        # This may only be used in broker-broker api calls
        {TopicPartition('foo', 0): 0},
        FetchResponse[5](
            0,
            [_ResponseTopic("foo", [
                _ResponsePartition(0, 0, 1000, 0, 0, [], _build_record_batch([(None, b'xxx', None)]), version=5)], version=5)]),
        1,
    ),
])
def test__handle_fetch_response(fetcher, fetch_offsets, fetch_response, num_partitions):
    fetcher._nodes_with_pending_fetch_requests.add(0)
    fetcher._handle_fetch_response(0, fetch_offsets, time.monotonic(), fetch_response)
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
    completed_fetch = _build_completed_fetch(tp, msgs)
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
    completed_fetch = _build_completed_fetch(tp, msgs)
    fetcher._subscriptions.pause(tp)
    partition_record = fetcher._parse_fetched_data(completed_fetch)
    assert partition_record is None


def test__parse_fetched_data__stale_offset(fetcher, topic, mocker):
    fetcher.config['check_crcs'] = False
    tp = TopicPartition(topic, 0)
    msgs = []
    for i in range(10):
        msgs.append((None, b"foo", None))
    completed_fetch = _build_completed_fetch(tp, msgs)
    completed_fetch = completed_fetch._replace(fetched_offset=10)
    partition_record = fetcher._parse_fetched_data(completed_fetch)
    assert partition_record is None


def test__parse_fetched_data__not_leader(fetcher, topic, mocker):
    fetcher.config['check_crcs'] = False
    tp = TopicPartition(topic, 0)
    completed_fetch = _build_completed_fetch(tp, [], error=NotLeaderForPartitionError)
    mocker.patch.object(fetcher._client.cluster, 'request_update')
    partition_record = fetcher._parse_fetched_data(completed_fetch)
    assert partition_record is None
    fetcher._client.cluster.request_update.assert_called_with()


def test__parse_fetched_data__unknown_tp(fetcher, topic, mocker):
    fetcher.config['check_crcs'] = False
    tp = TopicPartition(topic, 0)
    completed_fetch = _build_completed_fetch(tp, [], error=UnknownTopicOrPartitionError)
    mocker.patch.object(fetcher._client.cluster, 'request_update')
    partition_record = fetcher._parse_fetched_data(completed_fetch)
    assert partition_record is None
    fetcher._client.cluster.request_update.assert_called_with()


def test__parse_fetched_data__out_of_range(fetcher, topic, mocker):
    fetcher.config['check_crcs'] = False
    tp = TopicPartition(topic, 0)
    completed_fetch = _build_completed_fetch(tp, [], error=OffsetOutOfRangeError)
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


def test_reset_offsets_paused(subscription_state, client, manager, net, mocker):
    fetcher = Fetcher(client, subscription_state)
    tp = TopicPartition('foo', 0)
    subscription_state.assign_from_user([tp])
    subscription_state.pause(tp) # paused partition does not have a valid position
    subscription_state.request_offset_reset(tp, OffsetResetStrategy.LATEST)

    fetched_offsets = {tp: OffsetAndTimestamp(10, 1, -1)}
    async def fake_send(node_id, timestamps_and_epochs):
        return (fetched_offsets, set())
    mocker.patch.object(fetcher._client, 'ready', return_value=True)
    mocker.patch.object(fetcher, '_send_list_offsets_request', side_effect=fake_send)
    mocker.patch.object(fetcher._client.cluster, "leader_for_partition", return_value=0)
    manager.run(fetcher._reset_offsets_async, 1000)

    assert not subscription_state.is_offset_reset_needed(tp)
    assert not subscription_state.is_fetchable(tp) # because tp is paused
    assert subscription_state.has_valid_position(tp)
    assert subscription_state.position(tp) == OffsetAndMetadata(10, '', -1)


def test_reset_offsets_paused_without_valid(subscription_state, client, manager, net, mocker):
    fetcher = Fetcher(client, subscription_state)
    tp = TopicPartition('foo', 0)
    subscription_state.assign_from_user([tp])
    subscription_state.pause(tp) # paused partition does not have a valid position
    subscription_state.reset_missing_positions()

    fetched_offsets = {tp: OffsetAndTimestamp(0, 1, -1)}
    async def fake_send(node_id, timestamps_and_epochs):
        return (fetched_offsets, set())
    mocker.patch.object(fetcher._client, 'ready', return_value=True)
    mocker.patch.object(fetcher, '_send_list_offsets_request', side_effect=fake_send)
    mocker.patch.object(fetcher._client.cluster, "leader_for_partition", return_value=0)
    manager.run(fetcher._reset_offsets_async, 1000)

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

    mocker.patch.object(fetcher, '_reset_offsets_async')
    fetcher.reset_offsets_if_needed()

    assert not subscription_state.is_offset_reset_needed(tp)
    assert not subscription_state.is_fetchable(tp) # because tp is paused
    assert subscription_state.has_valid_position(tp)
    assert subscription_state.position(tp) == OffsetAndMetadata(10, '', -1)


def test_fetch_position_after_exception(client):
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

    msgs = [(None, b'msg', None) for _ in range(3)]
    fetcher._completed_fetches.append(
        _build_completed_fetch(tp1, msgs, offset=1))
    fetcher._completed_fetches.append(
        _build_completed_fetch(tp0, [], error=OffsetOutOfRangeError, offset=1))
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


def test_seek_before_exception(client):
    subscription_state = SubscriptionState(offset_reset_strategy='NONE')
    fetcher = Fetcher(client, subscription_state, max_poll_records=2)

    tp0 = TopicPartition('foo', 0)
    tp1 = TopicPartition('foo', 1)
    subscription_state.assign_from_user([tp0])
    subscription_state.seek(tp0, 1)

    assert len(fetcher._fetchable_partitions()) == 1

    msgs = [(None, b'msg', None) for _ in range(3)]
    fetcher._completed_fetches.append(
        _build_completed_fetch(tp0, msgs, offset=1))
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
        _build_completed_fetch(tp1, [], error=OffsetOutOfRangeError, offset=1))
    records, partial = fetcher.fetched_records()

    assert len(records) == 1
    assert tp0 in records
    assert len(records[tp0]) == 1
    assert subscription_state.position(tp0).offset == 4

    subscription_state.seek(tp1, 10)
    # Should not throw OffsetOutOfRangeError after the seek
    records, partial = fetcher.fetched_records()
    assert len(records) == 0


class TestFetchOffsetsByTimes:
    @pytest.fixture
    def fetcher(self, client):
        subscription_state = SubscriptionState()
        subscription_state.subscribe(topics=['test'])
        tp = TopicPartition('test', 0)
        subscription_state.assign_from_subscribed([tp])
        subscription_state.seek(tp, 0)
        return Fetcher(client, subscription_state)

    def test_empty_timestamps(self, fetcher):
        assert fetcher.offsets_by_times({}) == {}

    def test_success_no_retry(self, fetcher, mocker):
        tp = TopicPartition('test', 0)
        timestamps = {tp: 1000}
        expected_offset = OffsetAndTimestamp(10, 1000, -1)

        async def fake_send(ts):
            return ({tp: expected_offset}, set())
        mocker.patch.object(fetcher, '_send_list_offsets_requests', side_effect=fake_send)

        result = fetcher.offsets_by_times(timestamps, timeout_ms=10000)
        assert result == {tp: expected_offset}

    def test_success_with_retry(self, fetcher, mocker):
        tp0 = TopicPartition('test', 0)
        tp1 = TopicPartition('test', 1)
        timestamps = {tp0: 1000, tp1: 2000}
        offset0 = OffsetAndTimestamp(10, 1000, -1)
        offset1 = OffsetAndTimestamp(20, 2000, -1)

        results = iter([({tp0: offset0}, {tp1}), ({tp1: offset1}, set())])
        async def fake_send(ts):
            return next(results)
        mocker.patch.object(fetcher, '_send_list_offsets_requests', side_effect=fake_send)

        result = fetcher.offsets_by_times(timestamps, timeout_ms=10000)
        assert result == {tp0: offset0, tp1: offset1}

    def test_timeout_raises(self, fetcher, mocker):
        tp = TopicPartition('test', 0)
        timestamps = {tp: 1000}

        # Awaits a future that never completes
        async def fake_send(ts):
            await Future()
        mocker.patch.object(fetcher, '_send_list_offsets_requests', side_effect=fake_send)

        with pytest.raises(Errors.KafkaTimeoutError):
            fetcher.offsets_by_times(timestamps, timeout_ms=50)

    def test_non_retriable_error_raises(self, fetcher, mocker):
        tp = TopicPartition('test', 0)
        timestamps = {tp: 1000}

        # AuthorizationError is not retriable
        async def fake_send(ts):
            raise Errors.TopicAuthorizationFailedError()
        mocker.patch.object(fetcher, '_send_list_offsets_requests', side_effect=fake_send)

        with pytest.raises(Errors.TopicAuthorizationFailedError):
            fetcher.offsets_by_times(timestamps, timeout_ms=10000)

    def test_retriable_invalid_metadata_triggers_refresh(self, fetcher, mocker):
        tp = TopicPartition('test', 0)
        timestamps = {tp: 1000}
        expected_offset = OffsetAndTimestamp(10, 1000, -1)

        # First call fails with invalid_metadata error, second succeeds
        results = iter([NotLeaderForPartitionError(), ({tp: expected_offset}, set())])
        async def fake_send(ts):
            r = next(results)
            if isinstance(r, BaseException):
                raise r
            return r
        mocker.patch.object(fetcher, '_send_list_offsets_requests', side_effect=fake_send)

        refresh_future = Future().success(None)
        update_metadata_mock = mocker.patch.object(
            fetcher._client.cluster, 'request_update', return_value=refresh_future)

        result = fetcher.offsets_by_times(timestamps, timeout_ms=10000)
        assert result == {tp: expected_offset}
        update_metadata_mock.assert_called_once()

    def test_retriable_non_metadata_error_sleeps(self, fetcher, mocker):
        tp = TopicPartition('test', 0)
        timestamps = {tp: 1000}
        expected_offset = OffsetAndTimestamp(10, 1000, -1)

        # RequestTimedOutError is retriable but not invalid_metadata
        results = iter([Errors.RequestTimedOutError(), ({tp: expected_offset}, set())])
        async def fake_send(ts):
            r = next(results)
            if isinstance(r, BaseException):
                raise r
            return r
        mocker.patch.object(fetcher, '_send_list_offsets_requests', side_effect=fake_send)

        # Ensure cluster does not need update
        mocker.patch.object(type(fetcher._client._manager.cluster), 'need_update',
                            new_callable=mocker.PropertyMock, return_value=False)

        # Spy on call_later to verify a backoff timer was scheduled
        call_later_spy = mocker.spy(fetcher._client._manager._net, 'call_later')

        result = fetcher.offsets_by_times(timestamps, timeout_ms=10000)
        assert result == {tp: expected_offset}
        # At least one call_later was for the retry_backoff sleep
        assert call_later_spy.call_count >= 1

    def test_success_does_not_check_exception(self, fetcher, mocker):
        """Regression: successful future should not fall through to check future.exception."""
        tp0 = TopicPartition('test', 0)
        tp1 = TopicPartition('test', 1)
        timestamps = {tp0: 1000, tp1: 2000}
        offset0 = OffsetAndTimestamp(10, 1000, -1)
        offset1 = OffsetAndTimestamp(20, 2000, -1)

        # Succeeds but has retry partitions -- the bug was that code
        # would fall through to check future.exception (which is None),
        # causing an AttributeError
        results = iter([({tp0: offset0}, {tp1}), ({tp1: offset1}, set())])
        async def fake_send(ts):
            return next(results)
        mocker.patch.object(fetcher, '_send_list_offsets_requests', side_effect=fake_send)

        # Should not raise AttributeError
        result = fetcher.offsets_by_times(timestamps, timeout_ms=10000)
        assert result == {tp0: offset0, tp1: offset1}

    def test_no_timeout_passes_none(self, fetcher, mocker):
        tp = TopicPartition('test', 0)
        timestamps = {tp: 1000}
        expected_offset = OffsetAndTimestamp(10, 1000, -1)

        async def fake_send(ts):
            return ({tp: expected_offset}, set())
        mocker.patch.object(fetcher, '_send_list_offsets_requests', side_effect=fake_send)

        result = fetcher.offsets_by_times(timestamps, timeout_ms=None)
        assert result == {tp: expected_offset}


# ----------------------------------------------------------------------
# KIP-320: offset validation via OffsetForLeaderEpoch
# ----------------------------------------------------------------------

_OffsetForLeaderTopicResult = OffsetForLeaderEpochResponse.OffsetForLeaderTopicResult
_EpochEndOffset = _OffsetForLeaderTopicResult.EpochEndOffset


def _build_offset_for_leader_epoch_response(entries):
    """entries: list of (topic, partition, error_code, leader_epoch, end_offset)."""
    by_topic = {}
    for topic, partition, error_code, leader_epoch, end_offset in entries:
        by_topic.setdefault(topic, []).append(_EpochEndOffset(
            error_code=error_code, partition=partition,
            leader_epoch=leader_epoch, end_offset=end_offset))
    topics = [_OffsetForLeaderTopicResult(topic=t, partitions=ps)
              for t, ps in by_topic.items()]
    return OffsetForLeaderEpochResponse(throttle_time_ms=0, topics=topics)


def test_maybe_validate_positions_marks_stale_epoch(fetcher, mocker):
    """When cluster's leader_epoch advances beyond a position's epoch,
    maybe_validate_positions should mark the partition for validation."""
    tp = TopicPartition('foobar', 0)
    fetcher._subscriptions.assignment[tp].seek(OffsetAndMetadata(100, '', 3))
    mocker.patch.object(fetcher._manager.cluster, 'leader_epoch_for_partition',
                        return_value=5)

    fetcher.maybe_validate_positions()

    assert fetcher._subscriptions.assignment[tp].awaiting_validation
    assert not fetcher._subscriptions.is_fetchable(tp)


def test_maybe_validate_positions_skips_position_without_epoch(fetcher, mocker):
    """Positions seeded without a leader_epoch (-1, legacy/post-seek) can't
    be validated and should be left alone."""
    tp = TopicPartition('foobar', 0)
    fetcher._subscriptions.assignment[tp].seek(OffsetAndMetadata(100, '', -1))
    mocker.patch.object(fetcher._manager.cluster, 'leader_epoch_for_partition',
                        return_value=5)

    fetcher.maybe_validate_positions()

    assert not fetcher._subscriptions.assignment[tp].awaiting_validation
    assert fetcher._subscriptions.is_fetchable(tp)


def test_maybe_validate_positions_no_op_when_epoch_current(fetcher, mocker):
    """Position's epoch matches cluster's epoch -> no validation needed."""
    tp = TopicPartition('foobar', 0)
    fetcher._subscriptions.assignment[tp].seek(OffsetAndMetadata(100, '', 5))
    mocker.patch.object(fetcher._manager.cluster, 'leader_epoch_for_partition',
                        return_value=5)

    fetcher.maybe_validate_positions()

    assert not fetcher._subscriptions.assignment[tp].awaiting_validation


def test_validate_offsets_async_clears_validation_on_matching_epoch(
        fetcher, manager, mocker):
    """OffsetForLeaderEpoch response with matching epoch and end_offset >=
    position.offset clears awaiting_validation."""
    tp = TopicPartition('foobar', 0)
    fetcher._subscriptions.assignment[tp].seek(OffsetAndMetadata(50, '', 3))
    fetcher._subscriptions.assignment[tp].maybe_validate_position(5)
    assert fetcher._subscriptions.assignment[tp].awaiting_validation

    mocker.patch.object(fetcher._manager.cluster, 'leader_for_partition',
                        return_value=0)
    mocker.patch.object(fetcher._manager.cluster, 'leader_epoch_for_partition',
                        return_value=5)
    mocker.patch.object(fetcher._client, 'ready', return_value=True)

    async def fake_send(node_id, partitions_to_positions):
        response = _build_offset_for_leader_epoch_response(
            [('foobar', 0, 0, 5, 100)])
        return fetcher._handle_offset_for_leader_epoch_response(
            response, partitions_to_positions)
    mocker.patch.object(fetcher, '_send_offset_for_leader_epoch_request',
                        side_effect=fake_send)

    manager.run(fetcher._validate_offsets_async, 1000)

    assert not fetcher._subscriptions.assignment[tp].awaiting_validation
    assert fetcher._subscriptions.assignment[tp].position.leader_epoch == 5
    assert fetcher._subscriptions.assignment[tp].position.offset == 50


def test_validate_offsets_async_truncation_auto_resets_with_policy(
        fetcher, manager, mocker):
    """Java behavior: with a default reset policy, truncation triggers an
    automatic offset reset rather than surfacing LogTruncationError."""
    tp = TopicPartition('foobar', 0)
    fetcher._subscriptions.assignment[tp].seek(OffsetAndMetadata(100, '', 3))
    fetcher._subscriptions.assignment[tp].maybe_validate_position(5)

    mocker.patch.object(fetcher._manager.cluster, 'leader_for_partition',
                        return_value=0)
    mocker.patch.object(fetcher._manager.cluster, 'leader_epoch_for_partition',
                        return_value=5)
    mocker.patch.object(fetcher._client, 'ready', return_value=True)

    async def fake_send(node_id, partitions_to_positions):
        response = _build_offset_for_leader_epoch_response(
            [('foobar', 0, 0, 3, 80)])
        return fetcher._handle_offset_for_leader_epoch_response(
            response, partitions_to_positions)
    mocker.patch.object(fetcher, '_send_offset_for_leader_epoch_request',
                        side_effect=fake_send)

    manager.run(fetcher._validate_offsets_async, 1000)

    # No truncation surfaced: auto-reset was applied instead
    assert fetcher._cached_log_truncation is None
    assert fetcher._subscriptions.assignment[tp].awaiting_reset
    # validate_offsets_if_needed should return None (no pending exception)
    assert fetcher.validate_offsets_if_needed() is None


def test_validate_offsets_async_detects_truncation_when_no_reset_policy(
        client, metrics, mocker, topic):
    """With offset_reset_strategy=NONE the user must catch
    LogTruncationError - we don't silently move the position."""
    # Build fetcher with explicit NONE policy
    subs = SubscriptionState(offset_reset_strategy='none')
    subs.subscribe(topics=[topic])
    tp = TopicPartition(topic, 0)
    subs.assign_from_subscribed([tp])
    subs.seek(tp, OffsetAndMetadata(100, '', 3))
    fetcher = Fetcher(client, subs, metrics=metrics)
    fetcher._subscriptions.assignment[tp].maybe_validate_position(5)

    mocker.patch.object(fetcher._manager.cluster, 'leader_for_partition',
                        return_value=0)
    mocker.patch.object(fetcher._manager.cluster, 'leader_epoch_for_partition',
                        return_value=5)
    mocker.patch.object(fetcher._client, 'ready', return_value=True)

    async def fake_send(node_id, partitions_to_positions):
        response = _build_offset_for_leader_epoch_response(
            [(topic, 0, 0, 3, 80)])
        return fetcher._handle_offset_for_leader_epoch_response(
            response, partitions_to_positions)
    mocker.patch.object(fetcher, '_send_offset_for_leader_epoch_request',
                        side_effect=fake_send)

    fetcher._manager._net.run(fetcher._validate_offsets_async, 1000)

    assert fetcher._cached_log_truncation is not None
    with pytest.raises(Errors.LogTruncationError) as exc_info:
        fetcher.validate_offsets_if_needed()
    assert tp in exc_info.value.divergent_offsets
    divergent = exc_info.value.divergent_offsets[tp]
    assert divergent.offset == 80
    assert divergent.leader_epoch == 3
    # awaiting_validation cleared so caller can act
    assert not fetcher._subscriptions.assignment[tp].awaiting_validation
    # And no auto-reset
    assert not fetcher._subscriptions.assignment[tp].awaiting_reset


def test_validate_offsets_async_retries_on_fenced_epoch(
        fetcher, manager, mocker):
    """FENCED_LEADER_EPOCH on the validation RPC sleeps retry_backoff_ms
    and retries within a single _validate_offsets_async invocation."""
    tp = TopicPartition('foobar', 0)
    fetcher._subscriptions.assignment[tp].seek(OffsetAndMetadata(50, '', 3))
    fetcher._subscriptions.assignment[tp].maybe_validate_position(5)

    mocker.patch.object(fetcher._manager.cluster, 'leader_for_partition',
                        return_value=0)
    mocker.patch.object(fetcher._manager.cluster, 'leader_epoch_for_partition',
                        return_value=5)
    mocker.patch.object(fetcher._client, 'ready', return_value=True)
    fetcher.config['retry_backoff_ms'] = 50

    call_count = [0]
    async def fake_send(node_id, partitions_to_positions):
        call_count[0] += 1
        if call_count[0] == 1:
            # First response: FENCED_LEADER_EPOCH (errno 74)
            response = _build_offset_for_leader_epoch_response(
                [('foobar', 0, 74, -1, -1)])
        else:
            response = _build_offset_for_leader_epoch_response(
                [('foobar', 0, 0, 5, 100)])
        return fetcher._handle_offset_for_leader_epoch_response(
            response, partitions_to_positions)
    mocker.patch.object(fetcher, '_send_offset_for_leader_epoch_request',
                        side_effect=fake_send)

    start = time.monotonic()
    manager.run(fetcher._validate_offsets_async, 1000)
    elapsed = time.monotonic() - start

    assert call_count[0] == 2, (
        'expected second OffsetForLeaderEpoch attempt after FENCED, got %d'
        % call_count[0])
    assert not fetcher._subscriptions.assignment[tp].awaiting_validation
    assert elapsed >= 0.05, (
        '_validate_offsets_async did not sleep for retry_backoff_ms; %.3fs'
        % elapsed)


def test_fetch_response_fenced_epoch_marks_validation(fetcher, mocker):
    """A FENCED_LEADER_EPOCH error in a Fetch response should mark the
    partition for validation, not silently drop the records."""
    tp = TopicPartition('foobar', 0)
    fetcher._subscriptions.assignment[tp].seek(OffsetAndMetadata(50, '', 3))
    mocker.patch.object(fetcher._manager.cluster, 'request_update')

    # Build a CompletedFetch with FENCED_LEADER_EPOCH (errno 74)
    completion = _build_completed_fetch(
        tp, [], error=Errors.FencedLeaderEpochError, offset=50)
    fetcher._parse_fetched_data(completion)

    assert fetcher._subscriptions.assignment[tp].awaiting_validation
    fetcher._manager.cluster.request_update.assert_called()


def test_validate_offsets_if_needed_no_op_when_no_partitions(fetcher):
    """No partitions awaiting validation -> validate_offsets_if_needed
    returns None without scheduling a task."""
    assert fetcher.validate_offsets_if_needed() is None
    assert fetcher._validation_task is None
