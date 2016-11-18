# pylint: skip-file
from __future__ import absolute_import

import pytest

from kafka.client_async import KafkaClient
from kafka.consumer.fetcher import Fetcher
from kafka.consumer.subscription_state import SubscriptionState
import kafka.errors as Errors
from kafka.future import Future
from kafka.metrics import Metrics
from kafka.protocol.fetch import FetchRequest
from kafka.structs import TopicPartition, OffsetAndMetadata


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
                        return_value = dict(enumerate(fetch_requests)))

    ret = fetcher.send_fetches()
    for node, request in enumerate(fetch_requests):
        fetcher._client.send.assert_any_call(node, request)
    assert len(ret) == len(fetch_requests)


@pytest.mark.parametrize(("api_version", "fetch_version"), [
    ((0, 10), 2),
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
