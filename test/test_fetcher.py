# pylint: skip-file
from __future__ import absolute_import

import pytest

import itertools
from collections import OrderedDict

from kafka.client_async import KafkaClient
from kafka.consumer.fetcher import Fetcher, NoOffsetForPartitionError
from kafka.consumer.subscription_state import SubscriptionState
from kafka.metrics import Metrics
from kafka.protocol.fetch import FetchRequest
from kafka.protocol.offset import OffsetResponse
from kafka.structs import TopicPartition
from kafka.future import Future
from kafka.errors import (
    StaleMetadata, LeaderNotAvailableError, NotLeaderForPartitionError,
    UnknownTopicOrPartitionError
)


@pytest.fixture
def client(mocker):
    return mocker.Mock(spec=KafkaClient(bootstrap_servers=[], api_version=(0, 9)))


@pytest.fixture
def subscription_state():
    return SubscriptionState()


@pytest.fixture
def fetcher(client, subscription_state):
    subscription_state.subscribe(topics=['foobar'])
    assignment = [TopicPartition('foobar', i) for i in range(3)]
    subscription_state.assign_from_subscribed(assignment)
    for tp in assignment:
        subscription_state.seek(tp, 0)
    return Fetcher(client, subscription_state, Metrics())


def test_send_fetches(fetcher, mocker):
    fetch_requests = [
        FetchRequest[0](
            -1, fetcher.config['fetch_max_wait_ms'],
            fetcher.config['fetch_min_bytes'],
            [('foobar', [
                (0, 0, fetcher.config['max_partition_fetch_bytes']),
                (1, 0, fetcher.config['max_partition_fetch_bytes']),
            ])]),
        FetchRequest[0](
            -1, fetcher.config['fetch_max_wait_ms'],
            fetcher.config['fetch_min_bytes'],
            [('foobar', [
                (2, 0, fetcher.config['max_partition_fetch_bytes']),
            ])])
    ]

    mocker.patch.object(fetcher, '_create_fetch_requests',
                        return_value=dict(enumerate(fetch_requests)))

    ret = fetcher.send_fetches()
    for node, request in enumerate(fetch_requests):
        fetcher._client.send.assert_any_call(node, request)
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


def test_update_fetch_positions(fetcher, mocker):
    mocker.patch.object(fetcher, '_reset_offset')
    partition = TopicPartition('foobar', 0)

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
    fetcher._subscriptions.assignment[partition].committed = 123
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

    mocked.return_value = {}
    with pytest.raises(NoOffsetForPartitionError):
        fetcher._reset_offset(tp)

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
