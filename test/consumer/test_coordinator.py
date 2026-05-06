# pylint: skip-file
import time

import pytest

from kafka.consumer.subscription_state import SubscriptionState, ConsumerRebalanceListener
from kafka.coordinator.assignors.abstract import (
    ConsumerProtocolSubscription, ConsumerProtocolAssignment,
)
from kafka.coordinator.assignors.range import RangePartitionAssignor
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from kafka.coordinator.assignors.sticky.sticky_assignor import StickyPartitionAssignor
from kafka.coordinator.base import Generation, MemberState
from kafka.coordinator.consumer import ConsumerCoordinator
import kafka.errors as Errors
from kafka.future import Future
from kafka.coordinator.base import UnjoinedGroupException
from kafka.protocol.consumer import (
    OffsetCommitRequest, OffsetCommitResponse,
    OffsetFetchRequest, OffsetFetchResponse,
    JoinGroupResponse, SyncGroupResponse,
)
from kafka.protocol.metadata import MetadataResponse
from kafka.structs import OffsetAndMetadata, TopicPartition
from kafka.util import WeakMethod


@pytest.fixture
def coordinator(broker, client, metrics):
    coord = ConsumerCoordinator(client, SubscriptionState(),
                                metrics=metrics,
                                api_version=broker.broker_version,
                                max_poll_interval_ms=300000 if broker.broker_version >= (0, 10, 1) else 10000,
                                session_timeout_ms=10000)
    try:
        yield coord
    finally:
        coord.close(timeout_ms=0)


def test_init(client, coordinator):
    # metadata update on init
    assert client.cluster._need_update is True
    assert WeakMethod(coordinator._handle_metadata_update) in client.cluster._listeners


@pytest.mark.parametrize("broker", [(0, 8, 0), (0, 8, 1), (0, 8, 2), (0, 9)], indirect=True)
def test_autocommit_enable_api_version(broker, coordinator):
    if broker.broker_version < (0, 8, 1):
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
        ('range', ConsumerProtocolSubscription(
            RangePartitionAssignor.version,
            ['foobar'],
            b'')),
        ('roundrobin', ConsumerProtocolSubscription(
            RoundRobinPartitionAssignor.version,
            ['foobar'],
            b'')),
        ('sticky', ConsumerProtocolSubscription(
            StickyPartitionAssignor.version,
            ['foobar'],
            b'')),
    ]


@pytest.mark.parametrize("broker", [(0, 8, 0), (0, 8, 1), (0, 8, 2), (0, 9)], indirect=True)
def test_pattern_subscription(broker, coordinator):
    coordinator._subscription.subscribe(pattern='foo')
    assert coordinator._subscription.subscription == set([])
    assert coordinator._metadata_snapshot == coordinator._build_metadata_snapshot(coordinator._subscription, {})

    cluster = coordinator._client.cluster
    Broker = MetadataResponse.MetadataResponseBroker
    Topic = MetadataResponse.MetadataResponseTopic
    Partition = Topic.MetadataResponsePartition
    cluster.update_metadata(MetadataResponse[0](
        brokers=[Broker(0, 'foo', 12, version=0), Broker(1, 'bar', 34, version=0)],
        topics=[
            Topic(0, 'fizz', [], version=0),
            Topic(0, 'foo1', [Partition(0, 0, 0, [], [], version=0)], version=0),
            Topic(0, 'foo2', [Partition(0, 0, 1, [], [], version=0)], version=0)]))
    assert coordinator._subscription.subscription == {'foo1', 'foo2'}

    # 0.9 consumers should trigger dynamic partition assignment
    if broker.broker_version >= (0, 9):
        assert coordinator._subscription.assignment == {}

    # earlier consumers get all partitions assigned locally
    else:
        assert set(coordinator._subscription.assignment.keys()) == {TopicPartition('foo1', 0),
                                                                    TopicPartition('foo2', 0)}
    coordinator.close()


def test_lookup_assignor(coordinator):
    assert isinstance(coordinator._lookup_assignor('roundrobin'), RoundRobinPartitionAssignor)
    assert isinstance(coordinator._lookup_assignor('range'), RangePartitionAssignor)
    assert isinstance(coordinator._lookup_assignor('sticky'), StickyPartitionAssignor)
    assert coordinator._lookup_assignor('foobar') is None


def test_join_complete(mocker, coordinator):
    coordinator._subscription.subscribe(topics=['foobar'])
    assignor = RoundRobinPartitionAssignor()
    coordinator._assignors = {assignor.name: assignor}
    mocker.spy(assignor, 'on_assignment')
    assert assignor.on_assignment.call_count == 0
    assignment = ConsumerProtocolAssignment(0, [('foobar', [0, 1])], b'')
    generation = 12
    coordinator._on_join_complete(generation, 'member-foo', 'roundrobin', assignment.encode())
    assert assignor.on_assignment.call_count == 1
    assignor.on_assignment.assert_called_with(assignment, generation)


def test_join_complete_with_sticky_assignor(mocker, coordinator):
    coordinator._subscription.subscribe(topics=['foobar'])
    assignor = StickyPartitionAssignor()
    coordinator._assignors = {assignor.name: assignor}
    mocker.spy(assignor, 'on_assignment')
    assert assignor.on_assignment.call_count == 0
    generation = 3
    assignment = ConsumerProtocolAssignment(0, [('foobar', [0, 1])], b'')
    coordinator._on_join_complete(generation, 'member-foo', 'sticky', assignment.encode())
    assert assignor.on_assignment.call_count == 1
    assignor.on_assignment.assert_called_with(assignment, generation)


def test_subscription_listener(mocker, coordinator):
    listener = mocker.MagicMock(spec=ConsumerRebalanceListener)
    coordinator._subscription.subscribe(
        topics=['foobar'],
        listener=listener)

    coordinator._on_join_prepare(0, 'member-foo')
    assert listener.on_partitions_revoked.call_count == 1
    listener.on_partitions_revoked.assert_called_with(set([]))

    assignment = ConsumerProtocolAssignment(0, [('foobar', [0, 1])], b'')
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

    assignment = ConsumerProtocolAssignment(0, [('foobar', [0, 1])], b'')
    coordinator._on_join_complete(
        0, 'member-foo', 'roundrobin', assignment.encode())
    assert listener.on_partitions_assigned.call_count == 1


def test_perform_assignment(mocker, coordinator):
    coordinator._subscription.subscribe(topics=['foo1'])
    members = [
        JoinGroupResponse.JoinGroupResponseMember(
            member_id='member-foo',
            metadata=ConsumerProtocolSubscription(0, ['foo1'], b'').encode(),
        ),
        JoinGroupResponse.JoinGroupResponseMember(
            member_id='member-bar',
            metadata=ConsumerProtocolSubscription(0, ['foo1'], b'').encode(),
        ),
    ]
    assignments = {
        'member-foo': ConsumerProtocolAssignment(
            0, [('foo1', [0])], b''),
        'member-bar': ConsumerProtocolAssignment(
            0, [('foo1', [1])], b'')
    }

    mocker.patch.object(RoundRobinPartitionAssignor, 'assign')
    RoundRobinPartitionAssignor.assign.return_value = assignments

    ret = coordinator._perform_assignment(
        'member-foo', 'roundrobin', members,
    )

    assert RoundRobinPartitionAssignor.assign.call_count == 1
    RoundRobinPartitionAssignor.assign.assert_called_with(
        coordinator._client.cluster, members)
    assert ret == assignments


def test_on_join_prepare(coordinator):
    coordinator._subscription.subscribe(topics=['foobar'])
    coordinator._on_join_prepare(0, 'member-foo')


def test_on_join_prepare_async_invokes_sync_listener(mocker, coordinator):
    coordinator.config['enable_auto_commit'] = False
    listener = mocker.MagicMock(spec=ConsumerRebalanceListener)
    coordinator._subscription.subscribe(topics=['foobar'], listener=listener)

    coordinator._manager.run(coordinator._on_join_prepare_async, 0, 'member-foo')

    assert listener.on_partitions_revoked.call_count == 1
    listener.on_partitions_revoked.assert_called_with(set())


def test_on_join_prepare_async_awaits_async_listener(mocker, coordinator):
    coordinator.config['enable_auto_commit'] = False
    listener = mocker.MagicMock(spec=ConsumerRebalanceListener)
    listener.on_partitions_revoked = mocker.AsyncMock()
    coordinator._subscription.subscribe(topics=['foobar'], listener=listener)

    coordinator._manager.run(coordinator._on_join_prepare_async, 0, 'member-foo')

    assert listener.on_partitions_revoked.call_count == 1
    listener.on_partitions_revoked.assert_awaited_with(set())


def test_on_join_prepare_async_listener_exception_is_caught(mocker, coordinator):
    coordinator.config['enable_auto_commit'] = False
    listener = mocker.MagicMock(spec=ConsumerRebalanceListener)
    listener.on_partitions_revoked.side_effect = RuntimeError('listener crash')
    coordinator._subscription.subscribe(topics=['foobar'], listener=listener)

    # Should not raise; should still complete the post-listener cleanup.
    coordinator._manager.run(coordinator._on_join_prepare_async, 0, 'member-foo')
    assert coordinator._is_leader is False


def test_on_join_prepare_async_skips_auto_commit_when_disabled(mocker, coordinator):
    coordinator.config['enable_auto_commit'] = False
    spy = mocker.spy(coordinator, '_commit_offsets_sync_async')
    coordinator._subscription.subscribe(topics=['foobar'])

    coordinator._manager.run(coordinator._on_join_prepare_async, 0, 'member-foo')

    assert spy.call_count == 0


def test_on_join_prepare_async_runs_auto_commit_when_enabled(mocker, coordinator):
    coordinator.config['enable_auto_commit'] = True
    async def _noop(*args, **kwargs):
        return None
    spy = mocker.patch.object(coordinator, '_commit_offsets_sync_async',
                              side_effect=_noop)
    coordinator._subscription.subscribe(topics=['foobar'])

    coordinator._manager.run(coordinator._on_join_prepare_async, 0, 'member-foo')

    assert spy.call_count == 1


def test_on_join_complete_async_invokes_sync_listener(mocker, coordinator):
    listener = mocker.MagicMock(spec=ConsumerRebalanceListener)
    coordinator._subscription.subscribe(topics=['foobar'], listener=listener)
    assignor = RoundRobinPartitionAssignor()
    coordinator._assignors = {assignor.name: assignor}
    assignment = ConsumerProtocolAssignment(0, [('foobar', [0, 1])], b'')

    coordinator._manager.run(
        coordinator._on_join_complete_async,
        12, 'member-foo', 'roundrobin', assignment.encode())

    assert listener.on_partitions_assigned.call_count == 1
    listener.on_partitions_assigned.assert_called_with(
        {TopicPartition('foobar', 0), TopicPartition('foobar', 1)})


def test_on_join_complete_async_awaits_async_listener(mocker, coordinator):
    listener = mocker.MagicMock(spec=ConsumerRebalanceListener)
    listener.on_partitions_assigned = mocker.AsyncMock()
    coordinator._subscription.subscribe(topics=['foobar'], listener=listener)
    assignor = RoundRobinPartitionAssignor()
    coordinator._assignors = {assignor.name: assignor}
    assignment = ConsumerProtocolAssignment(0, [('foobar', [0, 1])], b'')

    coordinator._manager.run(
        coordinator._on_join_complete_async,
        12, 'member-foo', 'roundrobin', assignment.encode())

    assert listener.on_partitions_assigned.call_count == 1
    listener.on_partitions_assigned.assert_awaited_with(
        {TopicPartition('foobar', 0), TopicPartition('foobar', 1)})


def test_on_join_complete_async_listener_exception_is_caught(mocker, coordinator):
    listener = mocker.MagicMock(spec=ConsumerRebalanceListener)
    listener.on_partitions_assigned.side_effect = RuntimeError('listener crash')
    coordinator._subscription.subscribe(topics=['foobar'], listener=listener)
    assignor = RoundRobinPartitionAssignor()
    coordinator._assignors = {assignor.name: assignor}
    assignment = ConsumerProtocolAssignment(0, [('foobar', [0, 1])], b'')

    # Should not raise.
    coordinator._manager.run(
        coordinator._on_join_complete_async,
        12, 'member-foo', 'roundrobin', assignment.encode())


def test_need_rejoin(coordinator):
    # No subscription - no rejoin
    assert coordinator.need_rejoin() is False

    coordinator._subscription.subscribe(topics=['foobar'])
    assert coordinator.need_rejoin() is True


def test_refresh_committed_offsets_if_needed(mocker, coordinator):
    tp0 = TopicPartition('foobar', 0)
    tp1 = TopicPartition('foobar', 1)
    async def _fake_fetch(self, partitions, timeout_ms=None):
        return {
            tp0: OffsetAndMetadata(123, '', -1),
            tp1: OffsetAndMetadata(234, '', -1),
        }
    mocker.patch.object(ConsumerCoordinator, 'fetch_committed_offsets_async', _fake_fetch)
    coordinator._subscription.assign_from_user([tp0, tp1])
    coordinator._subscription.request_offset_reset(tp0)
    coordinator._subscription.request_offset_reset(tp1)
    assert coordinator._subscription.is_offset_reset_needed(tp0)
    assert coordinator._subscription.is_offset_reset_needed(tp1)
    coordinator.refresh_committed_offsets_if_needed()
    assignment = coordinator._subscription.assignment
    assert assignment[tp0].position == OffsetAndMetadata(123, '', -1)
    assert assignment[tp1].position == OffsetAndMetadata(234, '', -1)
    assert not coordinator._subscription.is_offset_reset_needed(tp0)
    assert not coordinator._subscription.is_offset_reset_needed(tp1)


def test_fetch_committed_offsets(mocker, coordinator):

    # No partitions, no IO polling
    assert coordinator.fetch_committed_offsets([]) == {}

    # general case -- send offset fetch request, get successful future
    async def _ready(*args, **kwargs):
        return True
    mocker.patch.object(coordinator, 'ensure_coordinator_ready_async', side_effect=_ready)
    mocker.patch.object(coordinator, '_send_offset_fetch_request',
                        return_value=Future().success('foobar'))
    partitions = [TopicPartition('foobar', 0)]
    ret = coordinator.fetch_committed_offsets(partitions)
    assert ret == 'foobar'
    coordinator._send_offset_fetch_request.assert_called_with(partitions)

    # Failed future is raised if not retriable
    coordinator._send_offset_fetch_request.return_value = Future().failure(AssertionError)
    try:
        coordinator.fetch_committed_offsets(partitions)
    except AssertionError:
        pass
    else:
        assert False, 'Exception not raised when expected'

    coordinator._send_offset_fetch_request.side_effect = [
        Future().failure(Errors.RequestTimedOutError),
        Future().success('fizzbuzz')]

    ret = coordinator.fetch_committed_offsets(partitions)
    assert ret == 'fizzbuzz'
    assert coordinator._send_offset_fetch_request.call_count == 4  # successful, failed, retried+success


def test_close(mocker, coordinator):
    mocker.patch.object(coordinator, '_maybe_auto_commit_offsets_sync')
    mocker.patch.object(coordinator, '_handle_leave_group_response')
    mocker.patch.object(coordinator, 'coordinator_unknown', return_value=False)
    coordinator.coordinator_id = 0
    coordinator._generation = Generation(1, 'foobar', b'')
    coordinator.state = MemberState.STABLE
    cli = coordinator._client
    mocker.patch.object(cli._manager, 'send', return_value=Future().success('foobar'))
    mocker.patch.object(cli, 'poll')

    coordinator.close()
    assert coordinator._maybe_auto_commit_offsets_sync.call_count == 1
    coordinator._handle_leave_group_response.assert_called_with('foobar')

    assert coordinator.generation_if_stable() is None
    assert coordinator._generation == Generation.NO_GENERATION
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
    async def _ready(*args, **kwargs):
        return True
    mocker.patch.object(coordinator, 'ensure_coordinator_ready_async', side_effect=_ready)
    mocker.patch.object(coordinator, '_send_offset_commit_request',
                        return_value=Future().success('fizzbuzz'))

    # No offsets, no calls
    assert coordinator.commit_offsets_sync({}) is None
    assert coordinator._send_offset_commit_request.call_count == 0

    ret = coordinator.commit_offsets_sync(offsets)
    assert coordinator._send_offset_commit_request.call_count == 1
    assert ret == 'fizzbuzz'

    # Failed future is raised if not retriable
    coordinator._send_offset_commit_request.return_value = Future().failure(AssertionError)
    try:
        coordinator.commit_offsets_sync(offsets)
    except AssertionError:
        pass
    else:
        assert False, 'Exception not raised when expected'

    coordinator._send_offset_commit_request.side_effect = [
        Future().failure(Errors.RequestTimedOutError),
        Future().success('fizzbuzz')]

    ret = coordinator.commit_offsets_sync(offsets)
    assert ret == 'fizzbuzz'


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
def test_maybe_auto_commit_offsets_sync(mocker, client, api_version, group_id, enable,
                                        error, has_auto_commit, commit_offsets,
                                        warn, exc):
    mock_warn = mocker.patch('kafka.coordinator.consumer.log.warning')
    mock_exc = mocker.patch('kafka.coordinator.consumer.log.exception')
    coordinator = ConsumerCoordinator(client, SubscriptionState(),
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
    coordinator.close()


@pytest.fixture
def seeded_coord(broker, coordinator):
    """A coordinator wired to a bootstrapped MockBroker with state seeded
    so _send_offset_*_request can dispatch a real wire request to node 0.
    """
    coordinator._client._manager.bootstrap(timeout_ms=5000)
    coordinator._subscription.subscribe(topics=['foobar'])
    coordinator.coordinator_id = 0
    coordinator._generation = Generation(0, 'foobar', b'')
    coordinator.state = MemberState.STABLE
    coordinator.rejoin_needed = False
    return coordinator


def test_send_offset_commit_request_fail(coordinator, offsets):
    # Default coordinator state has coordinator_id=None, so coordinator()
    # returns None and the early-return paths fire without any patching.

    # No offsets
    ret = coordinator._send_offset_commit_request({})
    assert isinstance(ret, Future)
    assert ret.succeeded()

    # No coordinator
    ret = coordinator._send_offset_commit_request(offsets)
    assert ret.failed()
    assert isinstance(ret.exception, Errors.CoordinatorNotAvailableError)


@pytest.mark.parametrize('broker,version', [
    ((0, 8, 1), 0),
    ((0, 8, 2), 1),
    ((0, 9), 2),
    ((0, 11), 3),
    ((2, 0), 4),
    ((2, 1), 6),
], indirect=['broker'])
def test_send_offset_commit_request_versions(broker, seeded_coord, offsets, version):
    captured = {}
    _Topic = OffsetCommitResponse.OffsetCommitResponseTopic
    _Partition = _Topic.OffsetCommitResponsePartition

    def handler(api_key, api_version, correlation_id, request_bytes):
        captured['api_version'] = api_version
        return OffsetCommitResponse(
            throttle_time_ms=0,
            topics=[_Topic(name='foobar', partitions=[
                _Partition(partition_index=0, error_code=0),
                _Partition(partition_index=1, error_code=0),
            ])])

    broker.respond_fn(OffsetCommitRequest, handler)
    future = seeded_coord._send_offset_commit_request(offsets)
    seeded_coord._client.poll(future=future, timeout_ms=5000)
    assert future.succeeded()
    assert captured['api_version'] == version


def test_send_offset_commit_request_failure(mocker, broker, seeded_coord, offsets):
    spy = mocker.spy(seeded_coord, '_failed_request')
    error = Errors.KafkaConnectionError('simulated transport failure')
    broker.fail_next(OffsetCommitRequest, error=error)

    future = seeded_coord._send_offset_commit_request(offsets)
    seeded_coord._client.poll(future=future, timeout_ms=5000)

    assert future.failed()
    assert future.exception is error
    assert spy.call_count == 1
    node_id, request, call_future, call_error = spy.call_args[0]
    assert node_id == 0
    assert isinstance(request, OffsetCommitRequest)
    assert call_future is future
    assert call_error is error


def test_send_offset_commit_request_success(mocker, broker, seeded_coord, offsets):
    _Topic = OffsetCommitResponse.OffsetCommitResponseTopic
    _Partition = _Topic.OffsetCommitResponsePartition
    broker.respond(OffsetCommitRequest, OffsetCommitResponse(
        throttle_time_ms=0,
        topics=[_Topic(name='foobar', partitions=[
            _Partition(partition_index=0, error_code=0),
            _Partition(partition_index=1, error_code=0),
        ])]))
    spy = mocker.spy(seeded_coord, '_handle_offset_commit_response')

    future = seeded_coord._send_offset_commit_request(offsets)
    seeded_coord._client.poll(future=future, timeout_ms=5000)

    assert future.succeeded()
    assert spy.call_count == 1
    call_offsets, call_future, _send_time, response = spy.call_args[0]
    assert call_offsets == offsets
    assert call_future is future
    assert isinstance(response, OffsetCommitResponse)


@pytest.mark.parametrize('response,error,dead', [
    (OffsetCommitResponse[0]([('foobar', [(0, 30), (1, 30)])]),
     Errors.GroupAuthorizationFailedError, False),
    (OffsetCommitResponse[0]([('foobar', [(0, 12), (1, 12)])]),
     Errors.OffsetMetadataTooLargeError, False),
    (OffsetCommitResponse[0]([('foobar', [(0, 28), (1, 28)])]),
     Errors.InvalidCommitOffsetSizeError, False),
    (OffsetCommitResponse[0]([('foobar', [(0, 14), (1, 14)])]),
     Errors.CoordinatorLoadInProgressError, False),
    (OffsetCommitResponse[0]([('foobar', [(0, 15), (1, 15)])]),
     Errors.CoordinatorNotAvailableError, True),
    (OffsetCommitResponse[0]([('foobar', [(0, 16), (1, 16)])]),
     Errors.NotCoordinatorError, True),
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
def test_handle_offset_commit_response(coordinator, offsets, response, error, dead):
    coordinator.coordinator_id = 0
    future = Future()
    coordinator._handle_offset_commit_response(offsets, future, time.monotonic(), response)
    assert isinstance(future.exception, error) if error else True
    assert coordinator.coordinator_id is (None if dead else 0)


@pytest.fixture
def partitions():
    return [TopicPartition('foobar', 0), TopicPartition('foobar', 1)]


def test_send_offset_fetch_request_fail(coordinator, partitions):
    # Default coordinator state has coordinator_id=None.

    # No partitions
    ret = coordinator._send_offset_fetch_request([])
    assert isinstance(ret, Future)
    assert ret.succeeded()
    assert ret.value == {}

    # No coordinator
    ret = coordinator._send_offset_fetch_request(partitions)
    assert ret.failed()
    assert isinstance(ret.exception, Errors.CoordinatorNotAvailableError)


@pytest.mark.parametrize('broker,version', [
    ((0, 8, 1), 0),
    ((0, 8, 2), 1),
    ((0, 9), 1),
    ((0, 10, 2), 2),
    ((0, 11), 3),
    ((2, 0), 4),
    ((2, 1), 5),
], indirect=['broker'])
def test_send_offset_fetch_request_versions(broker, seeded_coord, partitions, version):
    captured = {}
    _Topic = OffsetFetchResponse.OffsetFetchResponseTopic
    _Partition = _Topic.OffsetFetchResponsePartition

    def handler(api_key, api_version, correlation_id, request_bytes):
        captured['api_version'] = api_version
        return OffsetFetchResponse(
            throttle_time_ms=0,
            error_code=0,
            topics=[_Topic(name='foobar', partitions=[
                _Partition(partition_index=0, committed_offset=123,
                           committed_leader_epoch=-1, metadata='', error_code=0),
                _Partition(partition_index=1, committed_offset=234,
                           committed_leader_epoch=-1, metadata='', error_code=0),
            ])])

    broker.respond_fn(OffsetFetchRequest, handler)
    future = seeded_coord._send_offset_fetch_request(partitions)
    seeded_coord._client.poll(future=future, timeout_ms=5000)
    assert future.succeeded()
    assert captured['api_version'] == version


def test_send_offset_fetch_request_failure(mocker, broker, seeded_coord, partitions):
    spy = mocker.spy(seeded_coord, '_failed_request')
    error = Errors.KafkaConnectionError('simulated transport failure')
    broker.fail_next(OffsetFetchRequest, error=error)

    future = seeded_coord._send_offset_fetch_request(partitions)
    seeded_coord._client.poll(future=future, timeout_ms=5000)

    assert future.failed()
    assert future.exception is error
    assert spy.call_count == 1
    node_id, request, call_future, call_error = spy.call_args[0]
    assert node_id == 0
    assert isinstance(request, OffsetFetchRequest)
    assert call_future is future
    assert call_error is error


def test_send_offset_fetch_request_success(mocker, broker, seeded_coord, partitions, offsets):
    _Topic = OffsetFetchResponse.OffsetFetchResponseTopic
    _Partition = _Topic.OffsetFetchResponsePartition
    broker.respond(OffsetFetchRequest, OffsetFetchResponse(
        throttle_time_ms=0,
        error_code=0,
        topics=[_Topic(name='foobar', partitions=[
            _Partition(partition_index=0, committed_offset=123,
                       committed_leader_epoch=-1, metadata='', error_code=0),
            _Partition(partition_index=1, committed_offset=234,
                       committed_leader_epoch=-1, metadata='', error_code=0),
        ])]))
    spy = mocker.spy(seeded_coord, '_handle_offset_fetch_response')

    future = seeded_coord._send_offset_fetch_request(partitions)
    seeded_coord._client.poll(future=future, timeout_ms=5000)

    assert future.succeeded()
    assert future.value == offsets
    assert spy.call_count == 1
    call_future, response = spy.call_args[0]
    assert call_future is future
    assert isinstance(response, OffsetFetchResponse)


@pytest.mark.parametrize('response,error,dead', [
    (OffsetFetchResponse[0]([('foobar', [(0, 123, '', 14), (1, 234, '', 14)])]),
     Errors.CoordinatorLoadInProgressError, False),
    (OffsetFetchResponse[0]([('foobar', [(0, 123, '', 16), (1, 234, '', 16)])]),
     Errors.NotCoordinatorError, True),
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
def test_handle_offset_fetch_response(coordinator, offsets, response, error, dead):
    coordinator.coordinator_id = 0
    future = Future()
    coordinator._handle_offset_fetch_response(future, response)
    if error is not None:
        assert isinstance(future.exception, error)
    else:
        assert future.succeeded()
        assert future.value == offsets
    assert coordinator.coordinator_id is (None if dead else 0)


def _join_response(error_code=0, generation_id=1, member_id='member-1',
                   leader='member-1', protocol_name='range', members=None):
    return JoinGroupResponse(
        throttle_time_ms=0,
        error_code=error_code,
        generation_id=generation_id,
        protocol_name=protocol_name,
        leader=leader,
        member_id=member_id,
        members=members or [])


@pytest.mark.parametrize('error_code,error_type,coordinator_dead,resets_member_id,resets_generation', [
    (0, None, False, False, False),
    (Errors.CoordinatorLoadInProgressError.errno, Errors.CoordinatorLoadInProgressError, False, False, False),
    (Errors.UnknownMemberIdError.errno, Errors.UnknownMemberIdError, False, True, True),
    (Errors.CoordinatorNotAvailableError.errno, Errors.CoordinatorNotAvailableError, True, False, False),
    (Errors.NotCoordinatorError.errno, Errors.NotCoordinatorError, True, False, False),
    (Errors.InconsistentGroupProtocolError.errno, Errors.InconsistentGroupProtocolError, False, False, False),
    (Errors.InvalidSessionTimeoutError.errno, Errors.InvalidSessionTimeoutError, False, False, False),
    (Errors.InvalidGroupIdError.errno, Errors.InvalidGroupIdError, False, False, False),
    (Errors.GroupAuthorizationFailedError.errno, Errors.GroupAuthorizationFailedError, False, False, False),
    (Errors.GroupMaxSizeReachedError.errno, Errors.GroupMaxSizeReachedError, False, False, False),
    (Errors.FencedInstanceIdError.errno, Errors.FencedInstanceIdError, False, False, False),
    (Errors.MemberIdRequiredError.errno, Errors.MemberIdRequiredError, False, True, True),
    (Errors.RebalanceInProgressError.errno, Errors.RebalanceInProgressError, False, False, False),
    # Unmapped error code: should raise the corresponding generic error.
    (Errors.UnknownError.errno, Errors.UnknownError, False, False, False),
])
def test_process_join_group_response(request, coordinator, error_code, error_type,
                                     coordinator_dead, resets_member_id,
                                     resets_generation):
    # Avoid LeaveGroup attempt during teardown (close() only sends LeaveGroup
    # when state is not UNJOINED).
    request.addfinalizer(lambda: setattr(coordinator, 'state', MemberState.UNJOINED))
    coordinator.coordinator_id = 0
    coordinator.state = MemberState.REBALANCING
    coordinator._generation = Generation(7, 'old-member', 'range')

    response = _join_response(
        error_code=error_code,
        generation_id=42,
        member_id='broker-assigned' if error_code == Errors.MemberIdRequiredError.errno else 'member-1')

    if error_type is None:
        ret = coordinator._process_join_group_response(response, send_time=time.monotonic())
        assert ret is response
        # State mutation: generation updated from response.
        assert coordinator._generation.generation_id == 42
        assert coordinator._generation.member_id == 'member-1'
        assert coordinator._generation.protocol == 'range'
        assert coordinator.coordinator_id == 0
    else:
        with pytest.raises(error_type):
            coordinator._process_join_group_response(response, send_time=time.monotonic())
        if coordinator_dead:
            assert coordinator.coordinator_id is None
        else:
            assert coordinator.coordinator_id == 0
        if resets_member_id:
            # MemberIdRequired captures the broker-assigned id; UnknownMemberId
            # clears it back to UNKNOWN_MEMBER_ID.
            if error_code == Errors.MemberIdRequiredError.errno:
                assert coordinator._generation.member_id == 'broker-assigned'
            else:
                assert coordinator._generation.member_id == ''
        if resets_generation:
            assert coordinator._generation.generation_id == -1


def test_process_join_group_response_state_not_rebalancing(coordinator):
    """Defensive: if state changed underneath us, raise UnjoinedGroupException."""
    coordinator.coordinator_id = 0
    coordinator.state = MemberState.UNJOINED
    response = _join_response(error_code=0)
    with pytest.raises(UnjoinedGroupException):
        coordinator._process_join_group_response(response, send_time=time.monotonic())


@pytest.mark.parametrize('error_code,error_type,coordinator_dead,resets_generation,requests_rejoin', [
    (0, None, False, False, False),
    (Errors.GroupAuthorizationFailedError.errno, Errors.GroupAuthorizationFailedError, False, False, True),
    (Errors.RebalanceInProgressError.errno, Errors.RebalanceInProgressError, False, False, True),
    (Errors.FencedInstanceIdError.errno, Errors.FencedInstanceIdError, False, False, True),
    (Errors.UnknownMemberIdError.errno, Errors.UnknownMemberIdError, False, True, True),
    (Errors.IllegalGenerationError.errno, Errors.IllegalGenerationError, False, True, True),
    (Errors.CoordinatorNotAvailableError.errno, Errors.CoordinatorNotAvailableError, True, False, True),
    (Errors.NotCoordinatorError.errno, Errors.NotCoordinatorError, True, False, True),
    (Errors.UnknownError.errno, Errors.UnknownError, False, False, True),
])
def test_process_sync_group_response(request, coordinator, error_code, error_type,
                                     coordinator_dead, resets_generation,
                                     requests_rejoin):
    request.addfinalizer(lambda: setattr(coordinator, 'state', MemberState.UNJOINED))
    coordinator.coordinator_id = 0
    coordinator._generation = Generation(7, 'member-1', 'range')
    coordinator.rejoin_needed = False

    assignment_bytes = b'\x00\x01\x02'
    response = SyncGroupResponse(
        throttle_time_ms=0,
        error_code=error_code,
        assignment=assignment_bytes)

    if error_type is None:
        ret = coordinator._process_sync_group_response(response, send_time=time.monotonic())
        assert ret == assignment_bytes
        assert coordinator.rejoin_needed is False
        assert coordinator.coordinator_id == 0
    else:
        with pytest.raises(error_type):
            coordinator._process_sync_group_response(response, send_time=time.monotonic())
        if requests_rejoin:
            assert coordinator.rejoin_needed is True
        if coordinator_dead:
            assert coordinator.coordinator_id is None
        if resets_generation:
            assert coordinator._generation.generation_id == -1
            assert coordinator._generation.member_id == ''


def test_heartbeat(mocker, coordinator):
    coordinator.coordinator_id = 0
    coordinator.state = MemberState.STABLE
    net = coordinator._manager._net

    assert not coordinator._heartbeat_enabled and not coordinator._heartbeat_closed

    assert coordinator._heartbeat_loop_future is None
    coordinator._maybe_start_heartbeat_loop()
    assert coordinator._heartbeat_loop_future is not None

    coordinator._enable_heartbeat()
    assert coordinator._heartbeat_enabled

    coordinator._disable_heartbeat()
    assert not coordinator._heartbeat_enabled

    # heartbeat disables when un-joined
    coordinator._enable_heartbeat()
    coordinator.state = MemberState.UNJOINED
    net.poll(timeout_ms=50)
    assert not coordinator._heartbeat_enabled

    coordinator._enable_heartbeat()
    coordinator.state = MemberState.STABLE
    # Replace _send_heartbeat_request with an async stub that suspends on a
    # Future we control, so the heartbeat coroutine reaches the dispatch and
    # blocks there. patch.object auto-detects the async def and would return
    # an AsyncMock whose return_value=Future() is never awaited (the Future
    # is just yielded as the coroutine's value, which would let the loop
    # spin); using side_effect with an async function that awaits the Future
    # forces the suspension we want. The Mock's call_count then verifies the
    # loop fired exactly once.
    blocked_send = Future()
    async def _hang(*args, **kwargs):
        await blocked_send
    mocker.patch.object(coordinator, '_send_heartbeat_request', side_effect=_hang)
    mocker.patch.object(coordinator.heartbeat, 'should_heartbeat', return_value=True)
    # Wakeup callback resolves the future on one poll cycle; the heartbeat
    # coroutine resumes and reaches _send_heartbeat_request on the next.
    deadline = time.monotonic() + 0.5
    while time.monotonic() < deadline:
        net.poll(timeout_ms=10)
        if coordinator._send_heartbeat_request.call_count > 0:
            break
    assert coordinator._send_heartbeat_request.call_count == 1

    coordinator._close_heartbeat()
    assert coordinator._heartbeat_closed
    # Unblock the suspended heartbeat coroutine and let it observe
    # _heartbeat_closed=True so it exits cleanly. Otherwise the task lingers
    # in NetworkSelector._pending_tasks and is later GC-closed, raising
    # GeneratorExit through the loop's BaseException handler.
    blocked_send.failure(Errors.KafkaConnectionError())
    net.poll(timeout_ms=50)


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
