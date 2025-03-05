# pylint: skip-file
from __future__ import absolute_import
import time

import pytest

from kafka.client_async import KafkaClient
from kafka.consumer.subscription_state import (
    SubscriptionState, ConsumerRebalanceListener)
from kafka.coordinator.assignors.range import RangePartitionAssignor
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from kafka.coordinator.assignors.sticky.sticky_assignor import StickyPartitionAssignor
from kafka.coordinator.base import Generation, MemberState, HeartbeatThread
from kafka.coordinator.consumer import ConsumerCoordinator
from kafka.coordinator.protocol import (
    ConsumerProtocolMemberMetadata, ConsumerProtocolMemberAssignment)
import kafka.errors as Errors
from kafka.future import Future
from kafka.metrics import Metrics
from kafka.protocol.broker_api_versions import BROKER_API_VERSIONS
from kafka.protocol.commit import (
    OffsetCommitRequest, OffsetCommitResponse,
    OffsetFetchRequest, OffsetFetchResponse)
from kafka.protocol.metadata import MetadataResponse
from kafka.structs import OffsetAndMetadata, TopicPartition
from kafka.util import WeakMethod


@pytest.fixture
def client(conn):
    return KafkaClient(api_version=(0, 9))

@pytest.fixture
def coordinator(client):
    return ConsumerCoordinator(client, SubscriptionState(), Metrics())


def test_init(client, coordinator):
    # metadata update on init
    assert client.cluster._need_update is True
    assert WeakMethod(coordinator._handle_metadata_update) in client.cluster._listeners


@pytest.mark.parametrize("api_version", [(0, 8, 0), (0, 8, 1), (0, 8, 2), (0, 9)])
def test_autocommit_enable_api_version(conn, api_version):
    coordinator = ConsumerCoordinator(KafkaClient(api_version=api_version),
                                      SubscriptionState(),
                                      Metrics(),
                                      enable_auto_commit=True,
                                      session_timeout_ms=30000,   # session_timeout_ms and max_poll_interval_ms
                                      max_poll_interval_ms=30000, # should be the same to avoid KafkaConfigurationError
                                      group_id='foobar',
                                      api_version=api_version)
    if api_version < (0, 8, 1):
        assert coordinator.config['enable_auto_commit'] is False
    else:
        assert coordinator.config['enable_auto_commit'] is True


def test_protocol_type(coordinator):
    assert coordinator.protocol_type() == 'consumer'


def test_group_protocols(coordinator):
    # Requires a subscription
    try:
        coordinator.group_protocols()
    except Errors.IllegalStateError:
        pass
    else:
        assert False, 'Exception not raised when expected'

    coordinator._subscription.subscribe(topics=['foobar'])
    assert coordinator.group_protocols() == [
        ('range', ConsumerProtocolMemberMetadata(
            RangePartitionAssignor.version,
            ['foobar'],
            b'')),
        ('roundrobin', ConsumerProtocolMemberMetadata(
            RoundRobinPartitionAssignor.version,
            ['foobar'],
            b'')),
        ('sticky', ConsumerProtocolMemberMetadata(
            StickyPartitionAssignor.version,
            ['foobar'],
            b'')),
    ]


@pytest.mark.parametrize('api_version', [(0, 8, 0), (0, 8, 1), (0, 8, 2), (0, 9)])
def test_pattern_subscription(conn, api_version):
    coordinator = ConsumerCoordinator(KafkaClient(api_version=api_version),
                                      SubscriptionState(),
                                      Metrics(),
                                      api_version=api_version,
                                      session_timeout_ms=10000,
                                      max_poll_interval_ms=10000)
    coordinator._subscription.subscribe(pattern='foo')
    assert coordinator._subscription.subscription == set([])
    assert coordinator._metadata_snapshot == coordinator._build_metadata_snapshot(coordinator._subscription, {})

    cluster = coordinator._client.cluster
    cluster.update_metadata(MetadataResponse[0](
        # brokers
        [(0, 'foo', 12), (1, 'bar', 34)],
        # topics
        [(0, 'fizz', []),
         (0, 'foo1', [(0, 0, 0, [], [])]),
         (0, 'foo2', [(0, 0, 1, [], [])])]))
    assert coordinator._subscription.subscription == {'foo1', 'foo2'}

    # 0.9 consumers should trigger dynamic partition assignment
    if api_version >= (0, 9):
        assert coordinator._subscription.assignment == {}

    # earlier consumers get all partitions assigned locally
    else:
        assert set(coordinator._subscription.assignment.keys()) == {TopicPartition('foo1', 0),
                                                                    TopicPartition('foo2', 0)}


def test_lookup_assignor(coordinator):
    assert coordinator._lookup_assignor('roundrobin') is RoundRobinPartitionAssignor
    assert coordinator._lookup_assignor('range') is RangePartitionAssignor
    assert coordinator._lookup_assignor('sticky') is StickyPartitionAssignor
    assert coordinator._lookup_assignor('foobar') is None


def test_join_complete(mocker, coordinator):
    coordinator._subscription.subscribe(topics=['foobar'])
    assignor = RoundRobinPartitionAssignor()
    coordinator.config['assignors'] = (assignor,)
    mocker.spy(assignor, 'on_assignment')
    assert assignor.on_assignment.call_count == 0
    assignment = ConsumerProtocolMemberAssignment(0, [('foobar', [0, 1])], b'')
    coordinator._on_join_complete(0, 'member-foo', 'roundrobin', assignment.encode())
    assert assignor.on_assignment.call_count == 1
    assignor.on_assignment.assert_called_with(assignment)


def test_join_complete_with_sticky_assignor(mocker, coordinator):
    coordinator._subscription.subscribe(topics=['foobar'])
    assignor = StickyPartitionAssignor()
    coordinator.config['assignors'] = (assignor,)
    mocker.spy(assignor, 'on_assignment')
    mocker.spy(assignor, 'on_generation_assignment')
    assert assignor.on_assignment.call_count == 0
    assert assignor.on_generation_assignment.call_count == 0
    assignment = ConsumerProtocolMemberAssignment(0, [('foobar', [0, 1])], b'')
    coordinator._on_join_complete(0, 'member-foo', 'sticky', assignment.encode())
    assert assignor.on_assignment.call_count == 1
    assert assignor.on_generation_assignment.call_count == 1
    assignor.on_assignment.assert_called_with(assignment)
    assignor.on_generation_assignment.assert_called_with(0)


def test_subscription_listener(mocker, coordinator):
    listener = mocker.MagicMock(spec=ConsumerRebalanceListener)
    coordinator._subscription.subscribe(
        topics=['foobar'],
        listener=listener)

    coordinator._on_join_prepare(0, 'member-foo')
    assert listener.on_partitions_revoked.call_count == 1
    listener.on_partitions_revoked.assert_called_with(set([]))

    assignment = ConsumerProtocolMemberAssignment(0, [('foobar', [0, 1])], b'')
    coordinator._on_join_complete(
        0, 'member-foo', 'roundrobin', assignment.encode())
    assert listener.on_partitions_assigned.call_count == 1
    listener.on_partitions_assigned.assert_called_with({TopicPartition('foobar', 0), TopicPartition('foobar', 1)})


def test_subscription_listener_failure(mocker, coordinator):
    listener = mocker.MagicMock(spec=ConsumerRebalanceListener)
    coordinator._subscription.subscribe(
        topics=['foobar'],
        listener=listener)

    # exception raised in listener should not be re-raised by coordinator
    listener.on_partitions_revoked.side_effect = Exception('crash')
    coordinator._on_join_prepare(0, 'member-foo')
    assert listener.on_partitions_revoked.call_count == 1

    assignment = ConsumerProtocolMemberAssignment(0, [('foobar', [0, 1])], b'')
    coordinator._on_join_complete(
        0, 'member-foo', 'roundrobin', assignment.encode())
    assert listener.on_partitions_assigned.call_count == 1


def test_perform_assignment(mocker, coordinator):
    member_metadata = {
        'member-foo': ConsumerProtocolMemberMetadata(0, ['foo1'], b''),
        'member-bar': ConsumerProtocolMemberMetadata(0, ['foo1'], b'')
    }
    assignments = {
        'member-foo': ConsumerProtocolMemberAssignment(
            0, [('foo1', [0])], b''),
        'member-bar': ConsumerProtocolMemberAssignment(
            0, [('foo1', [1])], b'')
    }

    mocker.patch.object(RoundRobinPartitionAssignor, 'assign')
    RoundRobinPartitionAssignor.assign.return_value = assignments

    ret = coordinator._perform_assignment(
        'member-foo', 'roundrobin',
        [(member, metadata.encode())
         for member, metadata in member_metadata.items()])

    assert RoundRobinPartitionAssignor.assign.call_count == 1
    RoundRobinPartitionAssignor.assign.assert_called_with(
        coordinator._client.cluster, member_metadata)
    assert ret == assignments


def test_on_join_prepare(coordinator):
    coordinator._subscription.subscribe(topics=['foobar'])
    coordinator._on_join_prepare(0, 'member-foo')


def test_need_rejoin(coordinator):
    # No subscription - no rejoin
    assert coordinator.need_rejoin() is False

    coordinator._subscription.subscribe(topics=['foobar'])
    assert coordinator.need_rejoin() is True


def test_refresh_committed_offsets_if_needed(mocker, coordinator):
    mocker.patch.object(ConsumerCoordinator, 'fetch_committed_offsets',
                        return_value = {
                            TopicPartition('foobar', 0): OffsetAndMetadata(123, '', -1),
                            TopicPartition('foobar', 1): OffsetAndMetadata(234, '', -1)})
    coordinator._subscription.assign_from_user([TopicPartition('foobar', 0)])
    assert coordinator._subscription.needs_fetch_committed_offsets is True
    coordinator.refresh_committed_offsets_if_needed()
    assignment = coordinator._subscription.assignment
    assert assignment[TopicPartition('foobar', 0)].committed == OffsetAndMetadata(123, '', -1)
    assert TopicPartition('foobar', 1) not in assignment
    assert coordinator._subscription.needs_fetch_committed_offsets is False


def test_fetch_committed_offsets(mocker, coordinator):

    # No partitions, no IO polling
    mocker.patch.object(coordinator._client, 'poll')
    assert coordinator.fetch_committed_offsets([]) == {}
    assert coordinator._client.poll.call_count == 0

    # general case -- send offset fetch request, get successful future
    mocker.patch.object(coordinator, 'ensure_coordinator_ready')
    mocker.patch.object(coordinator, '_send_offset_fetch_request',
                        return_value=Future().success('foobar'))
    partitions = [TopicPartition('foobar', 0)]
    ret = coordinator.fetch_committed_offsets(partitions)
    assert ret == 'foobar'
    coordinator._send_offset_fetch_request.assert_called_with(partitions)
    assert coordinator._client.poll.call_count == 1

    # Failed future is raised if not retriable
    coordinator._send_offset_fetch_request.return_value = Future().failure(AssertionError)
    coordinator._client.poll.reset_mock()
    try:
        coordinator.fetch_committed_offsets(partitions)
    except AssertionError:
        pass
    else:
        assert False, 'Exception not raised when expected'
    assert coordinator._client.poll.call_count == 1

    coordinator._client.poll.reset_mock()
    coordinator._send_offset_fetch_request.side_effect = [
        Future().failure(Errors.RequestTimedOutError),
        Future().success('fizzbuzz')]

    ret = coordinator.fetch_committed_offsets(partitions)
    assert ret == 'fizzbuzz'
    assert coordinator._client.poll.call_count == 2 # call + retry


def test_close(mocker, coordinator):
    mocker.patch.object(coordinator, '_maybe_auto_commit_offsets_sync')
    mocker.patch.object(coordinator, '_handle_leave_group_response')
    mocker.patch.object(coordinator, 'coordinator_unknown', return_value=False)
    coordinator.coordinator_id = 0
    coordinator._generation = Generation(1, 'foobar', b'')
    coordinator.state = MemberState.STABLE
    cli = coordinator._client
    mocker.patch.object(cli, 'send', return_value=Future().success('foobar'))
    mocker.patch.object(cli, 'poll')

    coordinator.close()
    assert coordinator._maybe_auto_commit_offsets_sync.call_count == 1
    coordinator._handle_leave_group_response.assert_called_with('foobar')

    assert coordinator.generation() is None
    assert coordinator._generation is Generation.NO_GENERATION
    assert coordinator.state is MemberState.UNJOINED
    assert coordinator.rejoin_needed is True


@pytest.fixture
def offsets():
    return {
        TopicPartition('foobar', 0): OffsetAndMetadata(123, '', -1),
        TopicPartition('foobar', 1): OffsetAndMetadata(234, '', -1),
    }


def test_commit_offsets_async(mocker, coordinator, offsets):
    mocker.patch.object(coordinator._client, 'poll')
    mocker.patch.object(coordinator, 'coordinator_unknown', return_value=False)
    mocker.patch.object(coordinator, 'ensure_coordinator_ready')
    mocker.patch.object(coordinator, '_send_offset_commit_request',
                        return_value=Future().success('fizzbuzz'))
    coordinator.commit_offsets_async(offsets)
    assert coordinator._send_offset_commit_request.call_count == 1


def test_commit_offsets_sync(mocker, coordinator, offsets):
    mocker.patch.object(coordinator, 'ensure_coordinator_ready')
    mocker.patch.object(coordinator, '_send_offset_commit_request',
                        return_value=Future().success('fizzbuzz'))
    cli = coordinator._client
    mocker.patch.object(cli, 'poll')

    # No offsets, no calls
    assert coordinator.commit_offsets_sync({}) is None
    assert coordinator._send_offset_commit_request.call_count == 0
    assert cli.poll.call_count == 0

    ret = coordinator.commit_offsets_sync(offsets)
    assert coordinator._send_offset_commit_request.call_count == 1
    assert cli.poll.call_count == 1
    assert ret == 'fizzbuzz'

    # Failed future is raised if not retriable
    coordinator._send_offset_commit_request.return_value = Future().failure(AssertionError)
    coordinator._client.poll.reset_mock()
    try:
        coordinator.commit_offsets_sync(offsets)
    except AssertionError:
        pass
    else:
        assert False, 'Exception not raised when expected'
    assert coordinator._client.poll.call_count == 1

    coordinator._client.poll.reset_mock()
    coordinator._send_offset_commit_request.side_effect = [
        Future().failure(Errors.RequestTimedOutError),
        Future().success('fizzbuzz')]

    ret = coordinator.commit_offsets_sync(offsets)
    assert ret == 'fizzbuzz'
    assert coordinator._client.poll.call_count == 2 # call + retry


@pytest.mark.parametrize(
    'api_version,group_id,enable,error,has_auto_commit,commit_offsets,warn,exc', [
        ((0, 8, 0), 'foobar', True, None, False, False, True, False),
        ((0, 8, 1), 'foobar', True, None, True, True, False, False),
        ((0, 8, 2), 'foobar', True, None, True, True, False, False),
        ((0, 9), 'foobar', False, None, False, False, False, False),
        ((0, 9), 'foobar', True, Errors.UnknownMemberIdError(), True, True, True, False),
        ((0, 9), 'foobar', True, Errors.IllegalGenerationError(), True, True, True, False),
        ((0, 9), 'foobar', True, Errors.RebalanceInProgressError(), True, True, True, False),
        ((0, 9), 'foobar', True, Exception(), True, True, False, True),
        ((0, 9), 'foobar', True, None, True, True, False, False),
        ((0, 9), None, True, None, False, False, True, False),
    ])
def test_maybe_auto_commit_offsets_sync(mocker, api_version, group_id, enable,
                                        error, has_auto_commit, commit_offsets,
                                        warn, exc):
    mock_warn = mocker.patch('kafka.coordinator.consumer.log.warning')
    mock_exc = mocker.patch('kafka.coordinator.consumer.log.exception')
    client = KafkaClient(api_version=api_version)
    coordinator = ConsumerCoordinator(client, SubscriptionState(),
                                      Metrics(),
                                      api_version=api_version,
                                      session_timeout_ms=30000,
                                      max_poll_interval_ms=30000,
                                      enable_auto_commit=enable,
                                      group_id=group_id)
    commit_sync = mocker.patch.object(coordinator, 'commit_offsets_sync',
                                      side_effect=error)
    if has_auto_commit:
        assert coordinator.next_auto_commit_deadline is not None
    else:
        assert coordinator.next_auto_commit_deadline is None

    assert coordinator._maybe_auto_commit_offsets_sync() is None

    if has_auto_commit:
        assert coordinator.next_auto_commit_deadline is not None

    assert commit_sync.call_count == (1 if commit_offsets else 0)
    assert mock_warn.call_count == (1 if warn else 0)
    assert mock_exc.call_count == (1 if exc else 0)


@pytest.fixture
def patched_coord(mocker, coordinator):
    coordinator._subscription.subscribe(topics=['foobar'])
    mocker.patch.object(coordinator, 'coordinator_unknown', return_value=False)
    coordinator.coordinator_id = 0
    mocker.patch.object(coordinator, 'coordinator', return_value=0)
    coordinator._generation = Generation(0, 'foobar', b'')
    coordinator.state = MemberState.STABLE
    coordinator.rejoin_needed = False
    mocker.patch.object(coordinator, 'need_rejoin', return_value=False)
    mocker.patch.object(coordinator._client, 'least_loaded_node',
                        return_value=1)
    mocker.patch.object(coordinator._client, 'ready', return_value=True)
    mocker.patch.object(coordinator._client, 'send')
    mocker.patch.object(coordinator, '_heartbeat_thread')
    mocker.spy(coordinator, '_failed_request')
    mocker.spy(coordinator, '_handle_offset_commit_response')
    mocker.spy(coordinator, '_handle_offset_fetch_response')
    return coordinator


def test_send_offset_commit_request_fail(mocker, patched_coord, offsets):
    patched_coord.coordinator_unknown.return_value = True
    patched_coord.coordinator_id = None
    patched_coord.coordinator.return_value = None

    # No offsets
    ret = patched_coord._send_offset_commit_request({})
    assert isinstance(ret, Future)
    assert ret.succeeded()

    # No coordinator
    ret = patched_coord._send_offset_commit_request(offsets)
    assert ret.failed()
    assert isinstance(ret.exception, Errors.GroupCoordinatorNotAvailableError)


@pytest.mark.parametrize('api_version,req_type', [
    ((0, 8, 1), OffsetCommitRequest[0]),
    ((0, 8, 2), OffsetCommitRequest[1]),
    ((0, 9), OffsetCommitRequest[2]),
    ((0, 11), OffsetCommitRequest[3]),
    ((2, 0), OffsetCommitRequest[4]),
    ((2, 1), OffsetCommitRequest[6]),
])
def test_send_offset_commit_request_versions(patched_coord, offsets,
                                             api_version, req_type):
    expect_node = 0
    patched_coord._client._api_versions = BROKER_API_VERSIONS[api_version]

    patched_coord._send_offset_commit_request(offsets)
    (node, request), _ = patched_coord._client.send.call_args
    assert node == expect_node, 'Unexpected coordinator node'
    assert isinstance(request, req_type)


def test_send_offset_commit_request_failure(patched_coord, offsets):
    _f = Future()
    patched_coord._client.send.return_value = _f
    future = patched_coord._send_offset_commit_request(offsets)
    (node, request), _ = patched_coord._client.send.call_args
    error = Exception()
    _f.failure(error)
    patched_coord._failed_request.assert_called_with(0, request, future, error)
    assert future.failed()
    assert future.exception is error


def test_send_offset_commit_request_success(mocker, patched_coord, offsets):
    _f = Future()
    patched_coord._client.send.return_value = _f
    future = patched_coord._send_offset_commit_request(offsets)
    (node, request), _ = patched_coord._client.send.call_args
    response = OffsetCommitResponse[0]([('foobar', [(0, 0), (1, 0)])])
    _f.success(response)
    patched_coord._handle_offset_commit_response.assert_called_with(
        offsets, future, mocker.ANY, response)


@pytest.mark.parametrize('response,error,dead', [
    (OffsetCommitResponse[0]([('foobar', [(0, 30), (1, 30)])]),
     Errors.GroupAuthorizationFailedError, False),
    (OffsetCommitResponse[0]([('foobar', [(0, 12), (1, 12)])]),
     Errors.OffsetMetadataTooLargeError, False),
    (OffsetCommitResponse[0]([('foobar', [(0, 28), (1, 28)])]),
     Errors.InvalidCommitOffsetSizeError, False),
    (OffsetCommitResponse[0]([('foobar', [(0, 14), (1, 14)])]),
     Errors.GroupLoadInProgressError, False),
    (OffsetCommitResponse[0]([('foobar', [(0, 15), (1, 15)])]),
     Errors.GroupCoordinatorNotAvailableError, True),
    (OffsetCommitResponse[0]([('foobar', [(0, 16), (1, 16)])]),
     Errors.NotCoordinatorForGroupError, True),
    (OffsetCommitResponse[0]([('foobar', [(0, 7), (1, 7)])]),
     Errors.RequestTimedOutError, True),
    (OffsetCommitResponse[0]([('foobar', [(0, 25), (1, 25)])]),
     Errors.CommitFailedError, False),
    (OffsetCommitResponse[0]([('foobar', [(0, 22), (1, 22)])]),
     Errors.CommitFailedError, False),
    (OffsetCommitResponse[0]([('foobar', [(0, 27), (1, 27)])]),
     Errors.CommitFailedError, False),
    (OffsetCommitResponse[0]([('foobar', [(0, 17), (1, 17)])]),
     Errors.InvalidTopicError, False),
    (OffsetCommitResponse[0]([('foobar', [(0, 29), (1, 29)])]),
     Errors.TopicAuthorizationFailedError, False),
    (OffsetCommitResponse[0]([('foobar', [(0, 0), (1, 0)])]),
     None, False),
    (OffsetCommitResponse[1]([('foobar', [(0, 0), (1, 0)])]),
     None, False),
    (OffsetCommitResponse[2]([('foobar', [(0, 0), (1, 0)])]),
     None, False),
    (OffsetCommitResponse[3](0, [('foobar', [(0, 0), (1, 0)])]),
     None, False),
    (OffsetCommitResponse[4](0, [('foobar', [(0, 0), (1, 0)])]),
     None, False),
    (OffsetCommitResponse[5](0, [('foobar', [(0, 0), (1, 0)])]),
     None, False),
    (OffsetCommitResponse[6](0, [('foobar', [(0, 0), (1, 0)])]),
     None, False),
])
def test_handle_offset_commit_response(mocker, patched_coord, offsets,
                                       response, error, dead):
    future = Future()
    patched_coord._handle_offset_commit_response(offsets, future, time.time(),
                                                 response)
    assert isinstance(future.exception, error) if error else True
    assert patched_coord.coordinator_id is (None if dead else 0)


@pytest.fixture
def partitions():
    return [TopicPartition('foobar', 0), TopicPartition('foobar', 1)]


def test_send_offset_fetch_request_fail(mocker, patched_coord, partitions):
    patched_coord.coordinator_unknown.return_value = True
    patched_coord.coordinator_id = None
    patched_coord.coordinator.return_value = None

    # No partitions
    ret = patched_coord._send_offset_fetch_request([])
    assert isinstance(ret, Future)
    assert ret.succeeded()
    assert ret.value == {}

    # No coordinator
    ret = patched_coord._send_offset_fetch_request(partitions)
    assert ret.failed()
    assert isinstance(ret.exception, Errors.GroupCoordinatorNotAvailableError)


@pytest.mark.parametrize('api_version,req_type', [
    ((0, 8, 1), OffsetFetchRequest[0]),
    ((0, 8, 2), OffsetFetchRequest[1]),
    ((0, 9), OffsetFetchRequest[1]),
    ((0, 10, 2), OffsetFetchRequest[2]),
    ((0, 11), OffsetFetchRequest[3]),
    ((2, 0), OffsetFetchRequest[4]),
    ((2, 1), OffsetFetchRequest[5]),
])
def test_send_offset_fetch_request_versions(patched_coord, partitions,
                                            api_version, req_type):
    # assuming fixture sets coordinator=0, least_loaded_node=1
    expect_node = 0
    patched_coord._client._api_versions = BROKER_API_VERSIONS[api_version]

    patched_coord._send_offset_fetch_request(partitions)
    (node, request), _ = patched_coord._client.send.call_args
    assert node == expect_node, 'Unexpected coordinator node'
    assert isinstance(request, req_type)


def test_send_offset_fetch_request_failure(patched_coord, partitions):
    _f = Future()
    patched_coord._client.send.return_value = _f
    future = patched_coord._send_offset_fetch_request(partitions)
    (node, request), _ = patched_coord._client.send.call_args
    error = Exception()
    _f.failure(error)
    patched_coord._failed_request.assert_called_with(0, request, future, error)
    assert future.failed()
    assert future.exception is error


def test_send_offset_fetch_request_success(patched_coord, partitions):
    _f = Future()
    patched_coord._client.send.return_value = _f
    future = patched_coord._send_offset_fetch_request(partitions)
    (node, request), _ = patched_coord._client.send.call_args
    response = OffsetFetchResponse[0]([('foobar', [(0, 123, b'', 0), (1, 234, b'', 0)])])
    _f.success(response)
    patched_coord._handle_offset_fetch_response.assert_called_with(
        future, response)


@pytest.mark.parametrize('response,error,dead', [
    (OffsetFetchResponse[0]([('foobar', [(0, 123, '', 14), (1, 234, '', 14)])]),
     Errors.GroupLoadInProgressError, False),
    (OffsetFetchResponse[0]([('foobar', [(0, 123, '', 16), (1, 234, '', 16)])]),
     Errors.NotCoordinatorForGroupError, True),
    (OffsetFetchResponse[0]([('foobar', [(0, 123, '', 25), (1, 234, '', 25)])]),
     Errors.UnknownMemberIdError, False),
    (OffsetFetchResponse[0]([('foobar', [(0, 123, '', 22), (1, 234, '', 22)])]),
     Errors.IllegalGenerationError, False),
    (OffsetFetchResponse[0]([('foobar', [(0, 123, '', 29), (1, 234, '', 29)])]),
     Errors.TopicAuthorizationFailedError, False),
    (OffsetFetchResponse[0]([('foobar', [(0, 123, '', 0), (1, 234, '', 0)])]),
     None, False),
    (OffsetFetchResponse[1]([('foobar', [(0, 123, '', 0), (1, 234, '', 0)])]),
     None, False),
    (OffsetFetchResponse[2]([('foobar', [(0, 123, '', 0), (1, 234, '', 0)])], 0),
     None, False),
    (OffsetFetchResponse[3](0, [('foobar', [(0, 123, '', 0), (1, 234, '', 0)])], 0),
     None, False),
    (OffsetFetchResponse[4](0, [('foobar', [(0, 123, '', 0), (1, 234, '', 0)])], 0),
     None, False),
    (OffsetFetchResponse[5](0, [('foobar', [(0, 123, -1, '', 0), (1, 234, -1, '', 0)])], 0),
     None, False),
])
def test_handle_offset_fetch_response(patched_coord, offsets,
                                      response, error, dead):
    future = Future()
    patched_coord._handle_offset_fetch_response(future, response)
    if error is not None:
        assert isinstance(future.exception, error)
    else:
        assert future.succeeded()
        assert future.value == offsets
    assert patched_coord.coordinator_id is (None if dead else 0)


def test_heartbeat(mocker, patched_coord):
    heartbeat = HeartbeatThread(patched_coord)

    assert not heartbeat.enabled and not heartbeat.closed

    heartbeat.enable()
    assert heartbeat.enabled

    heartbeat.disable()
    assert not heartbeat.enabled

    # heartbeat disables when un-joined
    heartbeat.enable()
    patched_coord.state = MemberState.UNJOINED
    heartbeat._run_once()
    assert not heartbeat.enabled

    heartbeat.enable()
    patched_coord.state = MemberState.STABLE
    mocker.spy(patched_coord, '_send_heartbeat_request')
    mocker.patch.object(patched_coord.heartbeat, 'should_heartbeat', return_value=True)
    heartbeat._run_once()
    assert patched_coord._send_heartbeat_request.call_count == 1

    heartbeat.close()
    assert heartbeat.closed


def test_lookup_coordinator_failure(mocker, coordinator):

    mocker.patch.object(coordinator, '_send_group_coordinator_request',
                        return_value=Future().failure(Exception('foobar')))
    future = coordinator.lookup_coordinator()
    assert future.failed()


def test_ensure_active_group(mocker, coordinator):
    coordinator._subscription.subscribe(topics=['foobar'])
    mocker.patch.object(coordinator, 'coordinator_unknown', return_value=False)
    mocker.patch.object(coordinator, '_send_join_group_request', return_value=Future().success(True))
    mocker.patch.object(coordinator, 'need_rejoin', side_effect=[True, False])
    mocker.patch.object(coordinator, '_on_join_complete')
    mocker.patch.object(coordinator, '_heartbeat_thread')

    coordinator.ensure_active_group()

    coordinator._send_join_group_request.assert_called_once_with()
