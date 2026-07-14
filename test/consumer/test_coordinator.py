# pylint: skip-file
import time

import pytest

from kafka.consumer.subscription_state import (
    AsyncConsumerRebalanceListener,
    ConsumerRebalanceListener,
    SubscriptionState,
)
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
from kafka.net.compat import KafkaNetClient
from kafka.net.manager import KafkaConnectionManager
from kafka.protocol.consumer import (
    OffsetCommitRequest, OffsetCommitResponse,
    OffsetFetchRequest, OffsetFetchResponse,
    JoinGroupRequest, JoinGroupResponse,
    SyncGroupRequest, SyncGroupResponse,
    HeartbeatRequest, HeartbeatResponse,
)
from kafka.protocol.metadata import (
    FindCoordinatorRequest, FindCoordinatorResponse, MetadataResponse,
)
from kafka.structs import OffsetAndMetadata, TopicPartition
from kafka.util import WeakMethod

from test.mock_broker import MockCluster


@pytest.fixture
def coordinator(broker, client, metrics):
    coord = ConsumerCoordinator(client, SubscriptionState(),
                                metrics=metrics,
                                api_version=broker.broker_version)
    try:
        yield coord
    finally:
        # Drop any group generation a test left set so close() doesn't fire a
        # LeaveGroupRequest the MockBroker isn't scripted for (which otherwise
        # surfaces as teardown ERROR-log noise + an unhandled mock-broker task).
        coord.reset_generation()
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


def test_group_metadata_unjoined(coordinator):
    """Before any join, group_metadata() returns the unjoined defaults so
    KafkaProducer can still send v3 TxnOffsetCommit (broker will treat the
    -1 generation as unfenced) without raising."""
    gm = coordinator.group_metadata()
    assert gm.group_id == coordinator.group_id
    # NO_GENERATION constants from coordinator.base
    assert gm.member_id == ''  # UNKNOWN_MEMBER_ID is ''
    assert gm.generation_id == -1  # DEFAULT_GENERATION_ID
    assert gm.group_instance_id is None
    # Live coordination state: unjoined.
    assert gm.state == MemberState.UNJOINED


def test_group_metadata_after_join(coordinator):
    """After joining, group_metadata() reflects the live generation and the
    MemberState."""
    coordinator._generation = Generation(generation_id=42,
                                         member_id='mbr-1',
                                         protocol='range')
    coordinator.state = MemberState.STABLE
    gm = coordinator.group_metadata()
    assert gm.generation_id == 42
    assert gm.member_id == 'mbr-1'
    # group_instance_id comes from config (None by default for this fixture).
    assert gm.group_instance_id is None
    assert gm.state == MemberState.STABLE

    # Still returns the snapshot even while rebalancing - the producer needs
    # *something* to send and the broker handles fencing. The state field
    # tracks the in-progress (re)join.
    coordinator.state = MemberState.REBALANCING
    gm = coordinator.group_metadata()
    assert gm.generation_id == 42
    assert gm.state == MemberState.REBALANCING


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
    coordinator._manager.run(
        coordinator._on_join_complete_async,
        generation, 'member-foo', 'roundrobin', assignment.encode())
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
    coordinator._manager.run(
        coordinator._on_join_complete_async,
        generation, 'member-foo', 'sticky', assignment.encode())
    assert assignor.on_assignment.call_count == 1
    assignor.on_assignment.assert_called_with(assignment, generation)


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


def test_on_join_prepare_async_invokes_sync_listener(mocker, coordinator):
    coordinator.config['enable_auto_commit'] = False
    listener = mocker.MagicMock(spec=ConsumerRebalanceListener)
    coordinator._subscription.subscribe(topics=['foobar'], listener=listener)

    coordinator._manager.run(coordinator._on_join_prepare_async, 0, 'member-foo')

    assert listener.on_partitions_revoked.call_count == 1
    listener.on_partitions_revoked.assert_called_with(set())


def test_on_join_prepare_async_awaits_async_listener(coordinator):
    """An AsyncConsumerRebalanceListener subclass is accepted and awaited."""
    coordinator.config['enable_auto_commit'] = False
    calls = []

    class MyListener(AsyncConsumerRebalanceListener):
        async def on_partitions_revoked(self, revoked):
            calls.append(('revoked', revoked))
        async def on_partitions_assigned(self, assigned):
            calls.append(('assigned', assigned))

    coordinator._subscription.subscribe(topics=['foobar'], listener=MyListener())
    coordinator._manager.run(coordinator._on_join_prepare_async, 0, 'member-foo')

    assert calls == [('revoked', set())]


def test_subscribe_rejects_non_listener(coordinator):
    """Anything that isn't a (Async)ConsumerRebalanceListener is rejected."""
    with pytest.raises(TypeError):
        coordinator._subscription.subscribe(
            topics=['foobar'], listener=lambda revoked: None)


def test_slow_rebalance_listener_logs_warning(mocker, coordinator):
    """A listener call exceeding the threshold logs a named warning."""
    coordinator.config['enable_auto_commit'] = False

    class SlowListener(ConsumerRebalanceListener):
        def on_partitions_revoked(self, revoked):
            time.sleep(0.01)  # well under the threshold; shouldn't warn
        def on_partitions_assigned(self, assigned):
            pass

    coordinator._subscription.subscribe(topics=['foobar'], listener=SlowListener())
    log_warning = mocker.patch('kafka.coordinator.consumer.log.warning')

    # Below threshold: no warning.
    coordinator._manager.run(coordinator._on_join_prepare_async, 0, 'member-foo')
    assert not any(
        'Rebalance listener' in str(call.args[0])
        for call in log_warning.call_args_list)

    # Above threshold: drop the threshold to a tiny value and re-run.
    mocker.patch.object(coordinator, '_REBALANCE_LISTENER_WARN_SECS', 0.001)
    log_warning.reset_mock()
    coordinator._manager.run(coordinator._on_join_prepare_async, 0, 'member-foo')
    matching = [c for c in log_warning.call_args_list
                if 'Rebalance listener' in str(c.args[0])]
    assert len(matching) == 1
    # log.warning(fmt, listener_class, method, group, elapsed)
    _fmt, listener_class, method, _group, _elapsed = matching[0].args
    assert listener_class == 'SlowListener'
    assert method == 'on_partitions_revoked'


def test_on_join_prepare_async_listener_exception_propagates(mocker, coordinator):
    """Java parity: a throwing rebalance listener fails the prepare phase
    with a KafkaError that chains the user exception as __cause__. The
    cleanup (is_leader, reset_group_subscription) still runs before the
    exception is raised."""
    coordinator.config['enable_auto_commit'] = False
    coordinator._generation = Generation(42, 'member-foo', 'range')
    listener = mocker.MagicMock(spec=ConsumerRebalanceListener)
    crash = RuntimeError('listener crash')
    listener.on_partitions_revoked.side_effect = crash
    coordinator._subscription.subscribe(topics=['foobar'], listener=listener)

    with pytest.raises(Errors.KafkaError) as exc_info:
        coordinator._manager.run(coordinator._on_join_prepare_async, 42, 'member-foo')
    assert exc_info.value.__cause__ is crash
    # Cleanup still ran before the exception bubbled out.
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


def test_on_join_complete_async_awaits_async_listener(coordinator):
    """An AsyncConsumerRebalanceListener subclass is accepted and awaited."""
    calls = []

    class MyListener(AsyncConsumerRebalanceListener):
        async def on_partitions_revoked(self, revoked):
            calls.append(('revoked', revoked))
        async def on_partitions_assigned(self, assigned):
            calls.append(('assigned', assigned))

    coordinator._subscription.subscribe(topics=['foobar'], listener=MyListener())
    assignor = RoundRobinPartitionAssignor()
    coordinator._assignors = {assignor.name: assignor}
    assignment = ConsumerProtocolAssignment(0, [('foobar', [0, 1])], b'')

    coordinator._manager.run(
        coordinator._on_join_complete_async,
        12, 'member-foo', 'roundrobin', assignment.encode())

    assert calls == [(
        'assigned',
        {TopicPartition('foobar', 0), TopicPartition('foobar', 1)})]


def test_on_join_complete_async_listener_exception_propagates(mocker, coordinator):
    """Java parity: a throwing on_partitions_assigned fails the complete
    phase with a KafkaError that chains the user exception."""
    listener = mocker.MagicMock(spec=ConsumerRebalanceListener)
    crash = RuntimeError('listener crash')
    listener.on_partitions_assigned.side_effect = crash
    coordinator._subscription.subscribe(topics=['foobar'], listener=listener)
    assignor = RoundRobinPartitionAssignor()
    coordinator._assignors = {assignor.name: assignor}
    assignment = ConsumerProtocolAssignment(0, [('foobar', [0, 1])], b'')

    with pytest.raises(Errors.KafkaError) as exc_info:
        coordinator._manager.run(
            coordinator._on_join_complete_async,
            12, 'member-foo', 'roundrobin', assignment.encode())
    assert exc_info.value.__cause__ is crash


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
    # _send_offset_fetch_request is an async coroutine; scheduled via
    # manager.call_soon so the mock must also be a coroutine function.
    async def fake_send_success(_partitions):
        return 'foobar'
    mocker.patch.object(coordinator, '_send_offset_fetch_request',
                        side_effect=fake_send_success)
    partitions = [TopicPartition('foobar', 0)]
    ret = coordinator.fetch_committed_offsets(partitions)
    assert ret == 'foobar'
    coordinator._send_offset_fetch_request.assert_called_with(partitions)

    # Non-retriable error is raised
    async def fake_send_assertion_error(_partitions):
        raise AssertionError
    coordinator._send_offset_fetch_request.side_effect = fake_send_assertion_error
    try:
        coordinator.fetch_committed_offsets(partitions)
    except AssertionError:
        pass
    else:
        assert False, 'Exception not raised when expected'

    # Retriable error then success
    async def fake_send_retry_then_success(_partitions):
        if not hasattr(fake_send_retry_then_success, 'called'):
            fake_send_retry_then_success.called = True
            raise Errors.RequestTimedOutError
        return 'fizzbuzz'
    coordinator._send_offset_fetch_request.side_effect = fake_send_retry_then_success

    ret = coordinator.fetch_committed_offsets(partitions)
    assert ret == 'fizzbuzz'
    assert coordinator._send_offset_fetch_request.call_count == 4  # successful, failed, retried+success


def test_close(mocker, net, coordinator):
    mocker.patch.object(coordinator, '_maybe_auto_commit_offsets_sync')
    mocker.patch.object(coordinator, '_handle_leave_group_response')
    mocker.patch.object(coordinator, 'coordinator_unknown', return_value=False)
    coordinator.coordinator_id = 0
    coordinator._generation = Generation(1, 'foobar', b'')
    coordinator.state = MemberState.STABLE
    cli = coordinator._client
    mocker.patch.object(cli._manager, 'send',
                        return_value=net.create_future().success('foobar'))
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
    mocker.patch.object(coordinator, 'coordinator_unknown', return_value=False)
    mocker.patch.object(coordinator, 'ensure_coordinator_ready')
    # _send_offset_commit_request is an async coroutine; scheduled via
    # manager.call_soon so we drive the event loop to trigger the mock.
    async def fake_send(_offsets):
        return 'fizzbuzz'
    mocker.patch.object(coordinator, '_send_offset_commit_request',
                        side_effect=fake_send)
    future = coordinator.commit_offsets_async(offsets)
    coordinator._client.poll(future=future, timeout_ms=1000)
    assert coordinator._send_offset_commit_request.call_count == 1


def test_commit_offsets_sync(mocker, coordinator, offsets):
    async def _ready(*args, **kwargs):
        return True
    mocker.patch.object(coordinator, 'ensure_coordinator_ready_async', side_effect=_ready)

    async def fake_send_success(_offsets):
        return 'fizzbuzz'
    mocker.patch.object(coordinator, '_send_offset_commit_request',
                        side_effect=fake_send_success)

    # No offsets, no calls
    assert coordinator.commit_offsets_sync({}) is None
    assert coordinator._send_offset_commit_request.call_count == 0

    ret = coordinator.commit_offsets_sync(offsets)
    assert coordinator._send_offset_commit_request.call_count == 1
    assert ret == 'fizzbuzz'

    # Non-retriable error is raised
    async def fake_send_assertion_error(_offsets):
        raise AssertionError
    coordinator._send_offset_commit_request.side_effect = fake_send_assertion_error
    try:
        coordinator.commit_offsets_sync(offsets)
    except AssertionError:
        pass
    else:
        assert False, 'Exception not raised when expected'

    # Retriable error is retried, then success
    async def fake_send_retry_then_success(_offsets):
        if not hasattr(fake_send_retry_then_success, 'called'):
            fake_send_retry_then_success.called = True
            raise Errors.RequestTimedOutError
        return 'fizzbuzz'
    coordinator._send_offset_commit_request.side_effect = fake_send_retry_then_success

    ret = coordinator.commit_offsets_sync(offsets)
    assert ret == 'fizzbuzz'


def test_commit_offsets_sync_forwards_operation_timeout_to_run(coordinator, offsets, mocker):
    """Facade wiring (#3121): commit_offsets_sync resolves None ->
    default_api_timeout_ms and forwards the operation deadline to net.run()'s
    backstop kwarg, so an explicit long timeout is not cut short by the default
    bridge deadline."""
    run = mocker.patch.object(coordinator._net, 'run', return_value=None)

    # Explicit timeout is forwarded as run()'s backstop.
    coordinator.commit_offsets_sync(offsets, timeout_ms=123000)
    assert run.call_args.kwargs['timeout_ms'] == 123000

    # No timeout -> default_api_timeout_ms (not request_timeout_ms).
    run.reset_mock()
    coordinator.commit_offsets_sync(offsets)
    assert run.call_args.kwargs['timeout_ms'] == coordinator.config['default_api_timeout_ms']


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
    # _send_offset_commit_request is an async coroutine; run it via the
    # selector. Default coordinator state has coordinator_id=None, so
    # coordinator() returns None and the no-coordinator path fires.

    # No offsets - coroutine returns None
    assert coordinator._net.run(coordinator._send_offset_commit_request, {}) is None

    # No coordinator - coroutine raises
    with pytest.raises(Errors.CoordinatorNotAvailableError):
        coordinator._net.run(coordinator._send_offset_commit_request, offsets)


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
    future = seeded_coord._manager.call_soon(
        seeded_coord._send_offset_commit_request, offsets)
    seeded_coord._client.poll(future=future, timeout_ms=5000)
    assert future.succeeded()
    assert captured['api_version'] == version


def test_send_offset_commit_request_failure(mocker, broker, seeded_coord, offsets):
    spy = mocker.spy(seeded_coord, '_failed_request')
    error = Errors.KafkaConnectionError('simulated transport failure')
    broker.fail_next(OffsetCommitRequest, error=error)

    future = seeded_coord._manager.call_soon(
        seeded_coord._send_offset_commit_request, offsets)
    seeded_coord._client.poll(future=future, timeout_ms=5000)

    assert future.failed()
    assert future.exception is error
    # The async coro raises on send failure, so the call_soon Future
    # carries the exception. _failed_request still fires for its side
    # effect (mark coordinator dead); future arg is None since the
    # coroutine itself surfaces the exception.
    assert spy.call_count == 1
    node_id, request, call_error = spy.call_args[0]
    assert node_id == 0
    assert isinstance(request, OffsetCommitRequest)
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

    future = seeded_coord._manager.call_soon(
        seeded_coord._send_offset_commit_request, offsets)
    seeded_coord._client.poll(future=future, timeout_ms=5000)

    assert future.succeeded()
    assert spy.call_count == 1
    call_offsets, _send_time, response = spy.call_args[0]
    assert call_offsets == offsets
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
    if error is None:
        coordinator._handle_offset_commit_response(offsets, time.monotonic(), response)
    else:
        with pytest.raises(error):
            coordinator._handle_offset_commit_response(offsets, time.monotonic(), response)
    assert coordinator.coordinator_id is (None if dead else 0)


@pytest.fixture
def partitions():
    return [TopicPartition('foobar', 0), TopicPartition('foobar', 1)]


def test_send_offset_fetch_request_fail(coordinator, partitions):
    # _send_offset_fetch_request is an async coroutine; run it via the
    # selector. Default coordinator state has coordinator_id=None.

    # No partitions - coroutine returns {}
    assert coordinator._net.run(coordinator._send_offset_fetch_request, []) == {}

    # No coordinator - coroutine raises
    with pytest.raises(Errors.CoordinatorNotAvailableError):
        coordinator._net.run(coordinator._send_offset_fetch_request, partitions)


@pytest.mark.parametrize('broker,version', [
    ((0, 8, 1), 0),
    ((0, 8, 2), 1),
    ((0, 9), 1),
    ((0, 10, 2), 2),
    ((0, 11), 3),
    ((2, 0), 4),
    ((2, 1), 5),
    ((2, 4), 6),
    ((2, 5), 7),
    ((3, 0), 8),
], indirect=['broker'])
def test_send_offset_fetch_request_versions(broker, seeded_coord, partitions, version):
    captured = {}
    _Topic = OffsetFetchResponse.OffsetFetchResponseTopic
    _Partition = _Topic.OffsetFetchResponsePartition
    _Group = OffsetFetchResponse.OffsetFetchResponseGroup
    _GroupTopic = _Group.OffsetFetchResponseTopics
    _GroupPartition = _GroupTopic.OffsetFetchResponsePartitions

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
            ])],
            groups=[_Group(group_id='foobar', error_code=0, topics=[
                _GroupTopic(name='foobar', partitions=[
                    _GroupPartition(partition_index=0, committed_offset=123,
                                    committed_leader_epoch=-1, metadata='', error_code=0),
                    _GroupPartition(partition_index=1, committed_offset=234,
                                    committed_leader_epoch=-1, metadata='', error_code=0),
                ])])])

    broker.respond_fn(OffsetFetchRequest, handler)
    future = seeded_coord._manager.call_soon(
        seeded_coord._send_offset_fetch_request, partitions)
    seeded_coord._client.poll(future=future, timeout_ms=5000)
    assert future.succeeded()
    assert captured['api_version'] == version


def test_send_offset_fetch_request_failure(mocker, broker, seeded_coord, partitions):
    spy = mocker.spy(seeded_coord, '_failed_request')
    error = Errors.KafkaConnectionError('simulated transport failure')
    broker.fail_next(OffsetFetchRequest, error=error)

    future = seeded_coord._manager.call_soon(
        seeded_coord._send_offset_fetch_request, partitions)
    seeded_coord._client.poll(future=future, timeout_ms=5000)

    assert future.failed()
    assert future.exception is error
    # The async coro raises on send failure; the call_soon Future carries
    # the exception. _failed_request still fires for its side effect
    # (mark coordinator dead) with future=None.
    assert spy.call_count == 1
    node_id, request, call_error = spy.call_args[0]
    assert node_id == 0
    assert isinstance(request, OffsetFetchRequest)
    assert call_error is error


def test_send_offset_fetch_request_success(mocker, broker, seeded_coord, partitions, offsets):
    _Topic = OffsetFetchResponse.OffsetFetchResponseTopic
    _Partition = _Topic.OffsetFetchResponsePartition
    _Group = OffsetFetchResponse.OffsetFetchResponseGroup
    _GroupTopic = _Group.OffsetFetchResponseTopics
    _GroupPartition = _GroupTopic.OffsetFetchResponsePartitions
    broker.respond(OffsetFetchRequest, OffsetFetchResponse(
        throttle_time_ms=0,
        error_code=0,
        topics=[_Topic(name='foobar', partitions=[
            _Partition(partition_index=0, committed_offset=123,
                       committed_leader_epoch=-1, metadata='', error_code=0),
            _Partition(partition_index=1, committed_offset=234,
                       committed_leader_epoch=-1, metadata='', error_code=0),
        ])],
        groups=[_Group(group_id='foobar', error_code=0, topics=[
            _GroupTopic(name='foobar', partitions=[
                _GroupPartition(partition_index=0, committed_offset=123,
                                committed_leader_epoch=-1, metadata='', error_code=0),
                _GroupPartition(partition_index=1, committed_offset=234,
                                committed_leader_epoch=-1, metadata='', error_code=0),
            ])])]))
    spy = mocker.spy(seeded_coord, '_handle_offset_fetch_response')

    future = seeded_coord._manager.call_soon(
        seeded_coord._send_offset_fetch_request, partitions)
    seeded_coord._client.poll(future=future, timeout_ms=5000)

    assert future.succeeded()
    assert future.value == offsets
    assert spy.call_count == 1
    (response,) = spy.call_args[0]
    assert isinstance(response, OffsetFetchResponse)


@pytest.mark.parametrize('isolation_level,expected', [
    ('read_uncommitted', False),
    ('read_committed', True),
])
def test_send_offset_fetch_request_sets_require_stable(
        broker, client, metrics, partitions, isolation_level, expected):
    coord = ConsumerCoordinator(client, SubscriptionState(),
                                metrics=metrics,
                                api_version=broker.broker_version,
                                isolation_level=isolation_level)
    try:
        client._manager.bootstrap(timeout_ms=5000)
        coord._subscription.subscribe(topics=['foobar'])
        coord.coordinator_id = 0
        coord._generation = Generation(0, 'foobar', b'')
        coord.state = MemberState.STABLE
        coord.rejoin_needed = False

        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            req = OffsetFetchRequest.decode(
                request_bytes, version=api_version, header=True)
            captured['require_stable'] = req.require_stable
            _Group = OffsetFetchResponse.OffsetFetchResponseGroup
            _GroupTopic = _Group.OffsetFetchResponseTopics
            _GroupPartition = _GroupTopic.OffsetFetchResponsePartitions
            return OffsetFetchResponse(
                throttle_time_ms=0, error_code=0, topics=[],
                groups=[_Group(group_id='foobar', error_code=0, topics=[
                    _GroupTopic(name='foobar', partitions=[
                        _GroupPartition(partition_index=0, committed_offset=1,
                                        committed_leader_epoch=-1, metadata='',
                                        error_code=0),
                        _GroupPartition(partition_index=1, committed_offset=2,
                                        committed_leader_epoch=-1, metadata='',
                                        error_code=0),
                    ])])])

        broker.respond_fn(OffsetFetchRequest, handler)
        future = coord._manager.call_soon(
            coord._send_offset_fetch_request, partitions)
        coord._client.poll(future=future, timeout_ms=5000)
        assert future.succeeded()
        assert captured['require_stable'] is expected
    finally:
        coord.close(timeout_ms=0)


def test_consumer_coordinator_rejects_bad_isolation_level(client, metrics):
    with pytest.raises(Errors.KafkaConfigurationError):
        ConsumerCoordinator(client, SubscriptionState(),
                            metrics=metrics,
                            isolation_level='banana')


OffsetFetchResponseTopic = OffsetFetchResponse.OffsetFetchResponseTopic
OffsetFetchResponsePartition = OffsetFetchResponseTopic.OffsetFetchResponsePartition
@pytest.mark.parametrize('response,error,dead', [
    (OffsetFetchResponse(topics=[
        OffsetFetchResponseTopic(name='foobar', partitions=[
            OffsetFetchResponsePartition(partition_index=0, committed_offset=123, metadata='', error_code=14),
            OffsetFetchResponsePartition(partition_index=1, committed_offset=234, metadata='', error_code=14)])], version=0),
     Errors.CoordinatorLoadInProgressError, False),
    (OffsetFetchResponse(topics=[
        OffsetFetchResponseTopic(name='foobar', partitions=[
            OffsetFetchResponsePartition(partition_index=0, committed_offset=123, metadata='', error_code=16),
            OffsetFetchResponsePartition(partition_index=1, committed_offset=234, metadata='', error_code=16)])], version=0),
     Errors.NotCoordinatorError, True),
    (OffsetFetchResponse(topics=[
        OffsetFetchResponseTopic(name='foobar', partitions=[
            OffsetFetchResponsePartition(partition_index=0, committed_offset=123, metadata='', error_code=25),
            OffsetFetchResponsePartition(partition_index=1, committed_offset=234, metadata='', error_code=25)])], version=0),
     Errors.UnknownMemberIdError, False),
    (OffsetFetchResponse(topics=[
        OffsetFetchResponseTopic(name='foobar', partitions=[
            OffsetFetchResponsePartition(partition_index=0, committed_offset=123, metadata='', error_code=22),
            OffsetFetchResponsePartition(partition_index=1, committed_offset=234, metadata='', error_code=22)])], version=0),
     Errors.IllegalGenerationError, False),
    (OffsetFetchResponse(topics=[
        OffsetFetchResponseTopic(name='foobar', partitions=[
            OffsetFetchResponsePartition(partition_index=0, committed_offset=123, metadata='', error_code=29),
            OffsetFetchResponsePartition(partition_index=1, committed_offset=234, metadata='', error_code=29)])], version=0),
     Errors.TopicAuthorizationFailedError, False),
    (OffsetFetchResponse(topics=[
        OffsetFetchResponseTopic(name='foobar', partitions=[
            OffsetFetchResponsePartition(partition_index=0, committed_offset=123, metadata='', error_code=0),
            OffsetFetchResponsePartition(partition_index=1, committed_offset=234, metadata='', error_code=0)])], version=0),
     None, False),
    (OffsetFetchResponse(topics=[
        OffsetFetchResponseTopic(name='foobar', partitions=[
            OffsetFetchResponsePartition(partition_index=0, committed_offset=123, metadata='', error_code=0),
            OffsetFetchResponsePartition(partition_index=1, committed_offset=234, metadata='', error_code=0)])], version=1),
     None, False),
    (OffsetFetchResponse(topics=[
        OffsetFetchResponseTopic(name='foobar', partitions=[
            OffsetFetchResponsePartition(partition_index=0, committed_offset=123, metadata='', error_code=0),
            OffsetFetchResponsePartition(partition_index=1, committed_offset=234, metadata='', error_code=0)])],
                         error_code=0,
                         version=2),
     None, False),
    (OffsetFetchResponse(throttle_time_ms=0, topics=[
        OffsetFetchResponseTopic(name='foobar', partitions=[
            OffsetFetchResponsePartition(partition_index=0, committed_offset=123, metadata='', error_code=0),
            OffsetFetchResponsePartition(partition_index=1, committed_offset=234, metadata='', error_code=0)])],
                         error_code=0,
                         version=3),
     None, False),
    (OffsetFetchResponse(throttle_time_ms=0, topics=[
        OffsetFetchResponseTopic(name='foobar', partitions=[
            OffsetFetchResponsePartition(partition_index=0, committed_offset=123, metadata='', error_code=0),
            OffsetFetchResponsePartition(partition_index=1, committed_offset=234, metadata='', error_code=0)])],
                         error_code=0,
                         version=4),
     None, False),
    (OffsetFetchResponse(throttle_time_ms=0, topics=[
        OffsetFetchResponseTopic(name='foobar', partitions=[
            OffsetFetchResponsePartition(partition_index=0, committed_offset=123, committed_leader_epoch=-1, metadata='', error_code=0),
            OffsetFetchResponsePartition(partition_index=1, committed_offset=234, committed_leader_epoch=-1, metadata='', error_code=0)])],
                         error_code=0,
                         version=5),
     None, False),
])
def test_handle_offset_fetch_response(coordinator, offsets, response, error, dead):
    coordinator.coordinator_id = 0
    if error is not None:
        with pytest.raises(error):
            coordinator._handle_offset_fetch_response(response)
    else:
        assert coordinator._handle_offset_fetch_response(response) == offsets
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


@pytest.mark.parametrize(
    'error_code,error_type,coordinator_dead,resets_generation_id,resets_member_id,requests_rejoin', [
    (0, None, False, False, False, False),
    (Errors.GroupAuthorizationFailedError.errno, Errors.GroupAuthorizationFailedError, False, False, False, True),
    (Errors.RebalanceInProgressError.errno, Errors.RebalanceInProgressError, False, False, False, True),
    (Errors.FencedInstanceIdError.errno, Errors.FencedInstanceIdError, False, False, False, True),
    (Errors.UnknownMemberIdError.errno, Errors.UnknownMemberIdError, False, True, True, True),
    # KIP-429 / Java parity: IllegalGeneration resets generation_id but
    # preserves member_id so the next JoinGroup can re-use it.
    (Errors.IllegalGenerationError.errno, Errors.IllegalGenerationError, False, True, False, True),
    (Errors.CoordinatorNotAvailableError.errno, Errors.CoordinatorNotAvailableError, True, False, False, True),
    (Errors.NotCoordinatorError.errno, Errors.NotCoordinatorError, True, False, False, True),
    (Errors.UnknownError.errno, Errors.UnknownError, False, False, False, True),
])
def test_process_sync_group_response(request, coordinator, error_code, error_type,
                                     coordinator_dead, resets_generation_id,
                                     resets_member_id, requests_rejoin):
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
        if resets_generation_id:
            assert coordinator._generation.generation_id == -1
        if resets_member_id:
            assert coordinator._generation.member_id == ''
        elif resets_generation_id:
            # IllegalGeneration path: generation cleared but member_id kept.
            assert coordinator._generation.member_id == 'member-1'


def _join_response_object(error_code=0, generation_id=42,
                           member_id='member-1', leader='member-1',
                           protocol_type='consumer',
                           protocol_name='range', members=None):
    return JoinGroupResponse(
        throttle_time_ms=0,
        error_code=error_code,
        generation_id=generation_id,
        protocol_type=protocol_type,
        protocol_name=protocol_name,
        leader=leader,
        member_id=member_id,
        members=members or [])


def _sync_response_object(error_code=0, assignment=b'',
                          protocol_type='consumer', protocol_name='range'):
    return SyncGroupResponse(
        throttle_time_ms=0,
        error_code=error_code,
        protocol_type=protocol_type,
        protocol_name=protocol_name,
        assignment=assignment)


def test_do_join_and_sync_async_follower(request, broker, seeded_coord):
    request.addfinalizer(lambda: setattr(seeded_coord, 'state', MemberState.UNJOINED))
    # Default broker.broker_version=(4,2) -> JoinGroup v9, SyncGroup v5.
    # Follower: leader != our member_id.
    broker.respond(JoinGroupRequest, _join_response_object(
        leader='leader-x', member_id='member-1', members=[]))
    expected_assignment = ConsumerProtocolAssignment(
        0, [('foobar', [0, 1])], b'').encode()
    broker.respond(SyncGroupRequest, _sync_response_object(
        assignment=expected_assignment))

    result = seeded_coord._manager.run(seeded_coord._do_join_and_sync_async)

    assert result == expected_assignment
    assert seeded_coord._generation.generation_id == 42
    assert seeded_coord._generation.member_id == 'member-1'
    assert seeded_coord._generation.protocol == 'range'
    assert seeded_coord.state == MemberState.REBALANCING


def test_do_join_and_sync_async_leader(request, mocker, broker, seeded_coord):
    request.addfinalizer(lambda: setattr(seeded_coord, 'state', MemberState.UNJOINED))
    # Leader: response.leader == response.member_id. Members include the leader.
    member_metadata = ConsumerProtocolSubscription(0, ['foobar'], b'').encode()
    member = JoinGroupResponse.JoinGroupResponseMember(
        member_id='member-1',
        group_instance_id=None,
        metadata=member_metadata)
    broker.respond(JoinGroupRequest, _join_response_object(
        leader='member-1', member_id='member-1', members=[member]))

    # Capture the SyncGroup request to verify the leader sent assignments.
    captured = {}

    def sync_handler(api_key, api_version, correlation_id, request_bytes):
        captured['request'] = SyncGroupRequest.decode(
            request_bytes, version=api_version, header=True)
        return _sync_response_object(
            assignment=ConsumerProtocolAssignment(
                0, [('foobar', [0, 1])], b'').encode())

    broker.respond_fn(SyncGroupRequest, sync_handler)

    # Spy on _perform_assignment to confirm the leader path ran the assignor.
    spy = mocker.spy(seeded_coord, '_perform_assignment')

    result = seeded_coord._manager.run(seeded_coord._do_join_and_sync_async)

    assert spy.call_count == 1
    leader_id, protocol_name, members_arg = spy.call_args[0]
    assert leader_id == 'member-1'
    assert protocol_name == 'range'
    assert len(members_arg) == 1
    assert members_arg[0].member_id == 'member-1'
    # SyncGroup carried a non-empty assignment list.
    assert len(captured['request'].assignments) >= 1
    # Returned the assignment bytes the broker sent back.
    assert result == ConsumerProtocolAssignment(0, [('foobar', [0, 1])], b'').encode()


def test_do_join_and_sync_async_coordinator_unknown(request, seeded_coord):
    request.addfinalizer(lambda: setattr(seeded_coord, 'state', MemberState.UNJOINED))
    seeded_coord.coordinator_id = None  # force coordinator_unknown
    with pytest.raises(Errors.CoordinatorNotAvailableError):
        seeded_coord._manager.run(seeded_coord._do_join_and_sync_async)


@pytest.mark.parametrize('error_code,error_type', [
    (Errors.CoordinatorLoadInProgressError.errno, Errors.CoordinatorLoadInProgressError),
    (Errors.UnknownMemberIdError.errno, Errors.UnknownMemberIdError),
    (Errors.NotCoordinatorError.errno, Errors.NotCoordinatorError),
    (Errors.MemberIdRequiredError.errno, Errors.MemberIdRequiredError),
    (Errors.RebalanceInProgressError.errno, Errors.RebalanceInProgressError),
    (Errors.GroupAuthorizationFailedError.errno, Errors.GroupAuthorizationFailedError),
])
def test_do_join_and_sync_async_join_error(request, broker, seeded_coord,
                                            error_code, error_type):
    request.addfinalizer(lambda: setattr(seeded_coord, 'state', MemberState.UNJOINED))
    broker.respond(JoinGroupRequest, _join_response_object(error_code=error_code))
    with pytest.raises(error_type):
        seeded_coord._manager.run(seeded_coord._do_join_and_sync_async)


@pytest.mark.parametrize('error_code,error_type', [
    (Errors.GroupAuthorizationFailedError.errno, Errors.GroupAuthorizationFailedError),
    (Errors.RebalanceInProgressError.errno, Errors.RebalanceInProgressError),
    (Errors.UnknownMemberIdError.errno, Errors.UnknownMemberIdError),
    (Errors.IllegalGenerationError.errno, Errors.IllegalGenerationError),
    (Errors.NotCoordinatorError.errno, Errors.NotCoordinatorError),
])
def test_do_join_and_sync_async_sync_error(request, broker, seeded_coord,
                                            error_code, error_type):
    request.addfinalizer(lambda: setattr(seeded_coord, 'state', MemberState.UNJOINED))
    # JoinGroup succeeds (follower) so we get to SyncGroup.
    broker.respond(JoinGroupRequest, _join_response_object(
        leader='leader-x', member_id='member-1'))
    broker.respond(SyncGroupRequest, _sync_response_object(error_code=error_code))
    with pytest.raises(error_type):
        seeded_coord._manager.run(seeded_coord._do_join_and_sync_async)
    # All sync errors flip rejoin_needed via request_rejoin().
    assert seeded_coord.rejoin_needed is True


def test_join_group_async_no_rejoin_returns_true(request, mocker, broker, seeded_coord):
    """need_rejoin() False -> short-circuits to True without any requests."""
    request.addfinalizer(lambda: setattr(seeded_coord, 'state', MemberState.UNJOINED))
    mocker.patch.object(seeded_coord, 'need_rejoin', return_value=False)
    seeded_coord.state = MemberState.STABLE

    before = broker.requests_received
    result = seeded_coord._manager.run(seeded_coord.join_group_async, 5000)

    assert result is True
    assert broker.requests_received == before


def test_join_group_async_happy_path_follower(request, broker, seeded_coord):
    request.addfinalizer(lambda: setattr(seeded_coord, 'state', MemberState.UNJOINED))
    seeded_coord.rejoin_needed = True
    seeded_coord.state = MemberState.UNJOINED
    broker.respond(JoinGroupRequest, _join_response_object(
        leader='leader-x', member_id='member-1', members=[]))
    broker.respond(SyncGroupRequest, _sync_response_object(
        assignment=ConsumerProtocolAssignment(0, [('foobar', [0, 1])], b'').encode()))

    result = seeded_coord._manager.run(seeded_coord.join_group_async, 5000)

    assert result is True
    assert seeded_coord.state == MemberState.STABLE
    assert seeded_coord.rejoin_needed is False
    assert seeded_coord.rejoining is False
    assert seeded_coord._heartbeat_enabled is True


def test_join_group_uses_extended_per_request_timeout(request, mocker, broker, seeded_coord):
    """JoinGroup must be sent with request_timeout_ms = max(request_timeout_ms,
    max_poll_interval_ms + 5000) so the client doesn't time out a healthy
    rebalance that the broker is legitimately holding open. Matches Java's
    joinGroupTimeoutMs override.
    """
    request.addfinalizer(lambda: setattr(seeded_coord, 'state', MemberState.UNJOINED))
    seeded_coord.rejoin_needed = True
    seeded_coord.state = MemberState.UNJOINED
    broker.respond(JoinGroupRequest, _join_response_object(
        leader='leader-x', member_id='member-1', members=[]))
    broker.respond(SyncGroupRequest, _sync_response_object(
        assignment=ConsumerProtocolAssignment(0, [('foobar', [0, 1])], b'').encode()))

    seeded_coord.config['request_timeout_ms'] = 30000
    seeded_coord.config['max_poll_interval_ms'] = 300000
    sends = []
    original_send = seeded_coord._manager.send

    def capturing_send(req, node_id=None, request_timeout_ms=None):
        sends.append((type(req).__name__, request_timeout_ms))
        return original_send(req, node_id=node_id, request_timeout_ms=request_timeout_ms)

    mocker.patch.object(seeded_coord._manager, 'send', side_effect=capturing_send)

    seeded_coord._manager.run(seeded_coord.join_group_async, 5000)

    join_sends = [t for name, t in sends if 'JoinGroup' in name]
    assert join_sends, "expected at least one JoinGroupRequest send"
    # max(30000, 300000 + 5000) = 305000
    assert all(t == 305000 for t in join_sends), \
        "JoinGroup sends used %r, expected 305000" % (join_sends,)
    sync_sends = [t for name, t in sends if 'SyncGroup' in name]
    assert sync_sends, "expected SyncGroupRequest"
    # SyncGroup should NOT get the override - it uses default (None).
    assert all(t is None for t in sync_sends), \
        "SyncGroup sends should pass request_timeout_ms=None, got %r" % (sync_sends,)


def test_join_group_async_retries_on_retriable_error(request, broker, seeded_coord):
    """First JoinGroup fails with RebalanceInProgress; loop retries and succeeds."""
    request.addfinalizer(lambda: setattr(seeded_coord, 'state', MemberState.UNJOINED))
    seeded_coord.rejoin_needed = True
    seeded_coord.state = MemberState.UNJOINED
    broker.respond(JoinGroupRequest, _join_response_object(
        error_code=Errors.RebalanceInProgressError.errno))
    broker.respond(JoinGroupRequest, _join_response_object(
        leader='leader-x', member_id='member-1', members=[]))
    broker.respond(SyncGroupRequest, _sync_response_object(
        assignment=ConsumerProtocolAssignment(0, [('foobar', [0, 1])], b'').encode()))

    result = seeded_coord._manager.run(seeded_coord.join_group_async, 5000)

    assert result is True
    assert seeded_coord.state == MemberState.STABLE


def test_join_group_async_raises_non_retriable(request, broker, seeded_coord):
    request.addfinalizer(lambda: setattr(seeded_coord, 'state', MemberState.UNJOINED))
    seeded_coord.rejoin_needed = True
    seeded_coord.state = MemberState.UNJOINED
    broker.respond(JoinGroupRequest, _join_response_object(
        error_code=Errors.GroupAuthorizationFailedError.errno))

    with pytest.raises(Errors.GroupAuthorizationFailedError):
        seeded_coord._manager.run(seeded_coord.join_group_async, 5000)


def test_join_group_async_returns_false_on_short_timeout_and_caches_task(
        request, broker, net, seeded_coord):
    """Short consumer.poll(timeout_ms=N) should return False instead of
    hanging when the broker is slow to respond to JoinGroup; the in-flight
    task is cached so the next poll re-awaits it instead of sending a fresh
    JoinGroup.

    Regression for the test_group integration hang where 4 consumers tearing
    down concurrently left one stuck awaiting JoinGroup while the broker
    waited for the others to rejoin. Both properties are necessary:
        - timer must fire (else the user thread hangs and never sees stop)
        - in-flight task must be cached (else next poll sends a duplicate
          JoinGroup, confusing the broker's rebalance state)
    """
    request.addfinalizer(lambda: setattr(seeded_coord, 'state', MemberState.UNJOINED))
    seeded_coord.rejoin_needed = True
    seeded_coord.state = MemberState.UNJOINED

    # JoinGroup response future controlled by the test. Hangs until released.
    join_response_pending = net.create_future()  # awaited by slow_join_handler
    join_request_count = [0]

    async def slow_join_handler(api_key, api_version, correlation_id, request_bytes):
        join_request_count[0] += 1
        # Block until the test releases the future, simulating a broker
        # that's holding JoinGroup waiting for other members to rejoin.
        await join_response_pending
        return _join_response_object(
            leader='leader-x', member_id='member-1', members=[])

    broker.respond_fn(JoinGroupRequest, slow_join_handler)

    # First call: 50ms timer must expire and return False quickly. If the
    # await on JoinGroup is not timer-aware, this call hangs until the
    # connection's request_timeout_ms fires (~5s in the fixture).
    start = time.monotonic()
    result = seeded_coord._manager.run(seeded_coord.join_group_async, 50)
    elapsed = time.monotonic() - start
    assert result is False
    assert elapsed < 1.0, (
        'join_group_async did not respect timer.timeout_ms; took %.2fs'
        % elapsed)
    assert join_request_count[0] == 1

    # Second call: broker is still hanging. Should reuse the cached
    # in-flight task instead of sending a duplicate JoinGroup.
    start = time.monotonic()
    result = seeded_coord._manager.run(seeded_coord.join_group_async, 50)
    elapsed = time.monotonic() - start
    assert result is False
    assert elapsed < 1.0
    assert join_request_count[0] == 1, (
        'duplicate JoinGroup sent; cached task was not reused')

    # Release the broker; next call should complete using the cached task.
    broker.respond(SyncGroupRequest, _sync_response_object(
        assignment=ConsumerProtocolAssignment(0, [('foobar', [0, 1])], b'').encode()))
    join_response_pending.success(None)

    result = seeded_coord._manager.run(seeded_coord.join_group_async, 5000)
    assert result is True
    assert join_request_count[0] == 1, (
        'duplicate JoinGroup sent on the success path')
    assert seeded_coord.state == MemberState.STABLE


@pytest.mark.parametrize("broker", [(0, 8, 0)], indirect=True)
def test_join_group_async_unsupported_version(broker, coordinator):
    with pytest.raises(Errors.UnsupportedVersionError):
        coordinator._manager.run(coordinator.join_group_async, None)


def test_ensure_active_group_async_happy_path(request, broker, seeded_coord):
    request.addfinalizer(lambda: setattr(seeded_coord, 'state', MemberState.UNJOINED))
    seeded_coord.rejoin_needed = True
    seeded_coord.state = MemberState.UNJOINED
    broker.respond(JoinGroupRequest, _join_response_object(
        leader='leader-x', member_id='member-1', members=[]))
    broker.respond(SyncGroupRequest, _sync_response_object(
        assignment=ConsumerProtocolAssignment(0, [('foobar', [0, 1])], b'').encode()))

    result = seeded_coord._manager.run(seeded_coord.ensure_active_group_async, 5000)

    assert result is True
    assert seeded_coord.state == MemberState.STABLE
    # Heartbeat loop coroutine was scheduled.
    assert seeded_coord._heartbeat_loop_future is not None


def test_ensure_active_group_sync_facade(request, broker, seeded_coord):
    """The sync ensure_active_group facade dispatches via manager.run."""
    request.addfinalizer(lambda: setattr(seeded_coord, 'state', MemberState.UNJOINED))
    seeded_coord.rejoin_needed = True
    seeded_coord.state = MemberState.UNJOINED
    broker.respond(JoinGroupRequest, _join_response_object(
        leader='leader-x', member_id='member-1', members=[]))
    broker.respond(SyncGroupRequest, _sync_response_object(
        assignment=ConsumerProtocolAssignment(0, [('foobar', [0, 1])], b'').encode()))

    result = seeded_coord.ensure_active_group(timeout_ms=5000)

    assert result is True
    assert seeded_coord.state == MemberState.STABLE


def test_heartbeat(mocker, net, coordinator):
    coordinator.coordinator_id = 0
    coordinator.state = MemberState.STABLE

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
    blocked_send = net.create_future()
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
    # _send_group_coordinator_request is now an async coroutine scheduled
    # via manager.call_soon, so we drive the event loop to let the mock
    # fire before asserting on the returned future.
    async def fake_send():
        raise Exception('foobar')
    mocker.patch.object(coordinator, '_send_group_coordinator_request',
                        side_effect=fake_send)
    future = coordinator.lookup_coordinator()
    coordinator._client.poll(future=future, timeout_ms=1000)
    assert future.failed()


def test_do_join_and_sync_async_join_protocol_type_mismatch(request, broker, seeded_coord):
    """KIP-559: JoinGroupResponse with mismatched protocol_type must raise
    InconsistentGroupProtocolError."""
    request.addfinalizer(lambda: setattr(seeded_coord, 'state', MemberState.UNJOINED))
    broker.respond(JoinGroupRequest, _join_response_object(
        leader='leader-x', member_id='member-1', members=[],
        protocol_type='not-consumer'))

    with pytest.raises(Errors.InconsistentGroupProtocolError):
        seeded_coord._manager.run(seeded_coord._do_join_and_sync_async)


def test_do_join_and_sync_async_sync_protocol_type_mismatch(request, broker, seeded_coord):
    """KIP-559: SyncGroupResponse with mismatched protocol_type must raise."""
    request.addfinalizer(lambda: setattr(seeded_coord, 'state', MemberState.UNJOINED))
    broker.respond(JoinGroupRequest, _join_response_object(
        leader='leader-x', member_id='member-1', members=[]))
    broker.respond(SyncGroupRequest, _sync_response_object(
        protocol_type='not-consumer'))

    with pytest.raises(Errors.InconsistentGroupProtocolError):
        seeded_coord._manager.run(seeded_coord._do_join_and_sync_async)


def test_do_join_and_sync_async_sync_protocol_name_mismatch(request, broker, seeded_coord):
    """KIP-559: SyncGroupResponse with mismatched protocol_name must raise."""
    request.addfinalizer(lambda: setattr(seeded_coord, 'state', MemberState.UNJOINED))
    broker.respond(JoinGroupRequest, _join_response_object(
        leader='leader-x', member_id='member-1', members=[],
        protocol_name='range'))
    broker.respond(SyncGroupRequest, _sync_response_object(
        protocol_name='roundrobin'))

    with pytest.raises(Errors.InconsistentGroupProtocolError):
        seeded_coord._manager.run(seeded_coord._do_join_and_sync_async)


# ---------------------------------------------------------------------------
# KIP-429: Incremental Cooperative Rebalancing
# ---------------------------------------------------------------------------


def _cooperative_coordinator(client, metrics):
    """Build a ConsumerCoordinator configured for cooperative rebalance."""
    from kafka.coordinator.assignors.cooperative_sticky import (
        CooperativeStickyAssignor,
    )
    return ConsumerCoordinator(
        client, SubscriptionState(),
        metrics=metrics,
        api_version=(2, 4),
        assignors=(CooperativeStickyAssignor,))


class TestKip429RebalanceProtocolValidation:
    """All configured assignors must agree on a single RebalanceProtocol."""

    def test_default_is_eager(self, coordinator):
        """Range / RoundRobin / KIP-54 Sticky all support EAGER only."""
        from kafka.coordinator.assignors.abstract import RebalanceProtocol
        assert coordinator._rebalance_protocol == RebalanceProtocol.EAGER

    def test_cooperative_when_only_cooperative_sticky(self, client, metrics):
        from kafka.coordinator.assignors.abstract import RebalanceProtocol
        coord = _cooperative_coordinator(client, metrics)
        try:
            assert coord._rebalance_protocol == RebalanceProtocol.COOPERATIVE
        finally:
            coord.close(timeout_ms=0)

    def test_rejects_mixed_protocols(self, client, metrics):
        """A consumer configured with both EAGER and COOPERATIVE
        assignors must reject the configuration at init - at JoinGroup
        the broker picks one assignor and the consumer has no way to
        know which protocol mode to use until that decision happens."""
        from kafka.coordinator.assignors.cooperative_sticky import (
            CooperativeStickyAssignor,
        )
        with pytest.raises(Errors.KafkaConfigurationError, match='RebalanceProtocol'):
            ConsumerCoordinator(
                client, SubscriptionState(),
                metrics=metrics,
                api_version=(2, 4),
                assignors=(RangePartitionAssignor, CooperativeStickyAssignor))


class TestKip429OnJoinPrepare:
    """Under COOPERATIVE, _on_join_prepare must NOT globally revoke
    the assignment - that's the whole point of incremental rebalance."""

    def test_eager_revokes_everything(self, mocker, coordinator):
        coordinator.config['enable_auto_commit'] = False
        listener = mocker.MagicMock(spec=ConsumerRebalanceListener)
        coordinator._subscription.subscribe(topics=['foobar'], listener=listener)
        coordinator._subscription.assign_from_subscribed(
            [TopicPartition('foobar', 0), TopicPartition('foobar', 1)])
        # Real generation - otherwise is_lost() trips and the lost
        # branch fires on_partitions_lost instead.
        coordinator._generation = Generation(42, 'member-foo', 'range')
        coordinator._manager.run(coordinator._on_join_prepare_async, 42, 'member-foo')
        listener.on_partitions_revoked.assert_called_once_with(
            {TopicPartition('foobar', 0), TopicPartition('foobar', 1)})

    def test_cooperative_skips_global_revoke(self, mocker, client, metrics):
        coord = _cooperative_coordinator(client, metrics)
        try:
            coord.config['enable_auto_commit'] = False
            listener = mocker.MagicMock(spec=ConsumerRebalanceListener)
            coord._subscription.subscribe(topics=['foobar'], listener=listener)
            coord._subscription.assign_from_subscribed(
                [TopicPartition('foobar', 0), TopicPartition('foobar', 1)])
            # Real generation - otherwise is_lost() trips and the lost
            # branch fires on_partitions_lost instead.
            coord._generation = Generation(42, 'member-foo', 'cooperative-sticky')
            coord._manager.run(coord._on_join_prepare_async, 42, 'member-foo')
            # KIP-429: no global on_partitions_revoked in the prepare
            # phase - individual partitions are revoked later in
            # _on_join_complete based on the diff.
            listener.on_partitions_revoked.assert_not_called()
        finally:
            coord.close(timeout_ms=0)

    def test_cooperative_revokes_unsubscribed_topic_partitions_early(
            self, mocker, client, metrics):
        """Java parity: under COOPERATIVE, _on_join_prepare revokes
        partitions whose topic is no longer in the subscription
        *before* the JoinGroup, so the listener can commit those
        offsets while we're still the recognised owner. Partitions
        for still-subscribed topics stay in the assignment."""
        coord = _cooperative_coordinator(client, metrics)
        try:
            coord.config['enable_auto_commit'] = False
            listener = mocker.MagicMock(spec=ConsumerRebalanceListener)
            # Member previously owned partitions for two topics; then
            # the user changed the subscription to drop 'old'.
            coord._subscription.subscribe(topics=['t', 'old'], listener=listener)
            coord._subscription.assign_from_subscribed([
                TopicPartition('t', 0), TopicPartition('t', 1),
                TopicPartition('old', 0), TopicPartition('old', 1)])
            coord._subscription.change_subscription(['t'])
            coord._generation = Generation(42, 'mbr-1', 'cooperative-sticky')

            coord._manager.run(coord._on_join_prepare_async, 42, 'mbr-1')

            # Only the unsubscribed-topic partitions should be revoked,
            # and only those should be dropped from the assignment.
            listener.on_partitions_revoked.assert_called_once_with(
                {TopicPartition('old', 0), TopicPartition('old', 1)})
            assert coord._subscription.assigned_partitions() == {
                TopicPartition('t', 0), TopicPartition('t', 1)}
        finally:
            coord.close(timeout_ms=0)

    def test_cooperative_pre_revoke_marks_pending_revocation(
            self, mocker, client, metrics):
        """KIP-429: the cooperative pre-revoke must mark the
        about-to-be-revoked partitions as pending revocation BEFORE
        invoking the listener so the fetcher won't pull records from
        them while the listener is running. Verified by checking the
        flag at listener-call time."""
        coord = _cooperative_coordinator(client, metrics)
        try:
            coord.config['enable_auto_commit'] = False

            class Capturing(ConsumerRebalanceListener):
                def __init__(self, sub):
                    self.sub = sub
                    self.fetchable_at_revoke = None
                def on_partitions_revoked(self, revoked):
                    self.fetchable_at_revoke = {
                        tp: self.sub.is_fetchable(tp) for tp in revoked}
                def on_partitions_assigned(self, assigned):
                    pass

            listener = Capturing(coord._subscription)
            coord._subscription.subscribe(topics=['t', 'old'], listener=listener)
            coord._subscription.assign_from_subscribed([
                TopicPartition('t', 0),
                TopicPartition('old', 0)])
            coord._subscription.change_subscription(['t'])
            coord._generation = Generation(42, 'mbr-1', 'cooperative-sticky')

            coord._manager.run(coord._on_join_prepare_async, 42, 'mbr-1')

            assert listener.fetchable_at_revoke == {
                TopicPartition('old', 0): False}
        finally:
            coord.close(timeout_ms=0)

    def test_cooperative_no_unsubscribed_topics_skips_listener(
            self, mocker, client, metrics):
        """If every owned partition is still in the subscription,
        nothing is revoked in the prepare phase and the listener is
        not called - the diff is left for _on_join_complete_async."""
        coord = _cooperative_coordinator(client, metrics)
        try:
            coord.config['enable_auto_commit'] = False
            listener = mocker.MagicMock(spec=ConsumerRebalanceListener)
            coord._subscription.subscribe(topics=['t'], listener=listener)
            coord._subscription.assign_from_subscribed([
                TopicPartition('t', 0), TopicPartition('t', 1)])
            coord._generation = Generation(42, 'mbr-1', 'cooperative-sticky')

            coord._manager.run(coord._on_join_prepare_async, 42, 'mbr-1')

            listener.on_partitions_revoked.assert_not_called()
            assert coord._subscription.assigned_partitions() == {
                TopicPartition('t', 0), TopicPartition('t', 1)}
        finally:
            coord.close(timeout_ms=0)


class TestKip429OnJoinComplete:
    """Under COOPERATIVE, _on_join_complete computes the owned-vs-
    assigned diff: revoke removed, add new, no churn on stable."""

    def _make_assignment_bytes(self, topic, partitions):
        from kafka.protocol.consumer.metadata import ConsumerProtocolAssignment
        a = ConsumerProtocolAssignment(
            version=1,
            assigned_partitions=[(topic, sorted(partitions))],
            user_data=b'')
        return a.encode()

    def test_cooperative_revoke_then_assign_diff(self, mocker, client, metrics):
        """Member currently owns [0, 1, 2]; new assignment is [1, 2, 3].
        Listener must see revoked=[0] and assigned=[3], not the full sets."""
        coord = _cooperative_coordinator(client, metrics)
        try:
            coord.config['enable_auto_commit'] = False
            listener = mocker.MagicMock(spec=ConsumerRebalanceListener)
            coord._subscription.subscribe(topics=['t'], listener=listener)
            coord._subscription.assign_from_subscribed([
                TopicPartition('t', 0), TopicPartition('t', 1),
                TopicPartition('t', 2)])

            assignment_bytes = self._make_assignment_bytes('t', [1, 2, 3])
            coord._manager.run(
                coord._on_join_complete_async,
                42, 'member-1', 'cooperative-sticky', assignment_bytes)

            listener.on_partitions_revoked.assert_called_once_with({TopicPartition('t', 0)})
            listener.on_partitions_assigned.assert_called_once_with({TopicPartition('t', 3)})
        finally:
            coord.close(timeout_ms=0)

    def test_cooperative_stable_assignment_no_listener_calls(
            self, mocker, client, metrics):
        """Owned == assigned -> neither listener method fires."""
        coord = _cooperative_coordinator(client, metrics)
        try:
            coord.config['enable_auto_commit'] = False
            listener = mocker.MagicMock(spec=ConsumerRebalanceListener)
            coord._subscription.subscribe(topics=['t'], listener=listener)
            coord._subscription.assign_from_subscribed([
                TopicPartition('t', 0), TopicPartition('t', 1)])

            assignment_bytes = self._make_assignment_bytes('t', [0, 1])
            coord._manager.run(
                coord._on_join_complete_async,
                42, 'member-1', 'cooperative-sticky', assignment_bytes)

            listener.on_partitions_revoked.assert_not_called()
            listener.on_partitions_assigned.assert_not_called()
        finally:
            coord.close(timeout_ms=0)

    def test_cooperative_revoke_triggers_request_rejoin(
            self, mocker, client, metrics):
        """When any partitions are revoked, request a follow-up
        rebalance so the partition lands on its new owner in round 2."""
        coord = _cooperative_coordinator(client, metrics)
        try:
            coord.config['enable_auto_commit'] = False
            coord._subscription.subscribe(topics=['t'])
            coord._subscription.assign_from_subscribed([
                TopicPartition('t', 0), TopicPartition('t', 1)])
            spy = mocker.spy(coord, 'request_rejoin')

            # New assignment drops partition 1.
            assignment_bytes = self._make_assignment_bytes('t', [0])
            coord._manager.run(
                coord._on_join_complete_async,
                42, 'member-1', 'cooperative-sticky', assignment_bytes)

            spy.assert_called()
        finally:
            coord.close(timeout_ms=0)

    def test_cooperative_no_revoke_no_extra_rejoin(
            self, mocker, client, metrics):
        """If nothing is revoked (assignment only grew or stayed same),
        no follow-up rebalance is needed."""
        coord = _cooperative_coordinator(client, metrics)
        try:
            coord.config['enable_auto_commit'] = False
            coord._subscription.subscribe(topics=['t'])
            coord._subscription.assign_from_subscribed([TopicPartition('t', 0)])
            spy = mocker.spy(coord, 'request_rejoin')

            assignment_bytes = self._make_assignment_bytes('t', [0, 1])
            coord._manager.run(
                coord._on_join_complete_async,
                42, 'member-1', 'cooperative-sticky', assignment_bytes)

            spy.assert_not_called()
        finally:
            coord.close(timeout_ms=0)

    def test_cooperative_preserves_state_on_kept_partition(
            self, mocker, client, metrics):
        """KIP-429: a partition the consumer retains across a
        cooperative rebalance must keep its TopicPartitionState
        (position, paused flag, KIP-392 preferred-replica cache).
        Otherwise the rebalance would force a committed-offset
        re-fetch for partitions that didn't move - defeating the
        point of incremental cooperative."""
        from kafka.structs import OffsetAndMetadata
        coord = _cooperative_coordinator(client, metrics)
        try:
            coord.config['enable_auto_commit'] = False
            coord._subscription.subscribe(topics=['t'])
            coord._subscription.assign_from_subscribed([
                TopicPartition('t', 0), TopicPartition('t', 1)])
            # Seed state on partition 0 that must survive the rebalance.
            tp0 = TopicPartition('t', 0)
            coord._subscription.assignment[tp0].seek(
                OffsetAndMetadata(offset=500, metadata='', leader_epoch=-1))
            coord._subscription.assignment[tp0].update_preferred_read_replica(
                3, time.monotonic() + 60)
            original_state = coord._subscription.assignment[tp0]

            # Rebalance: keep partition 0, drop 1, add 2.
            assignment_bytes = self._make_assignment_bytes('t', [0, 2])
            coord._manager.run(
                coord._on_join_complete_async,
                42, 'member-1', 'cooperative-sticky', assignment_bytes)

            # The TopicPartitionState object is the SAME instance - not
            # a fresh one with the same fields. Identity matters because
            # the Fetcher and other components hold references.
            assert coord._subscription.assignment[tp0] is original_state
            assert coord._subscription.assignment[tp0].position.offset == 500
            assert coord._subscription.assignment[tp0].preferred_read_replica() == 3
            # New partition got fresh state.
            assert not coord._subscription.assignment[
                TopicPartition('t', 2)].has_valid_position
        finally:
            coord.close(timeout_ms=0)

    def test_cooperative_assignment_for_unsubscribed_topic_bails(
            self, mocker, client, metrics):
        """If the leader hands us a partition for a topic we're not
        subscribed to (e.g. the topic was deleted under us),
        ``assign_from_subscribed`` raises ValueError. We must bail
        cleanly: request re-join and skip the listener invocation,
        the assignor state update, and the auto-commit deadline reset
        - otherwise the user's listener acts on partitions that aren't
        actually in our SubscriptionState."""
        coord = _cooperative_coordinator(client, metrics)
        try:
            coord.config['enable_auto_commit'] = False
            listener = mocker.MagicMock(spec=ConsumerRebalanceListener)
            coord._subscription.subscribe(topics=['t'], listener=listener)
            coord._subscription.assign_from_subscribed([TopicPartition('t', 0)])
            assignor_spy = mocker.spy(coord._assignors['cooperative-sticky'], 'on_assignment')
            rejoin_spy = mocker.spy(coord, 'request_rejoin')
            old_deadline = coord.next_auto_commit_deadline

            # Assignment names a topic the consumer isn't subscribed to.
            bad_assignment = self._make_assignment_bytes('other-topic', [0])
            coord._manager.run(
                coord._on_join_complete_async,
                42, 'member-1', 'cooperative-sticky', bad_assignment)

            # Re-join requested, but no side effects from the rest of
            # the cooperative happy path.
            rejoin_spy.assert_called_once()
            listener.on_partitions_revoked.assert_not_called()
            listener.on_partitions_assigned.assert_not_called()
            assignor_spy.assert_not_called()
            assert coord.next_auto_commit_deadline == old_deadline
            # Original assignment is preserved.
            assert set(coord._subscription.assigned_partitions()) == {TopicPartition('t', 0)}
        finally:
            coord.close(timeout_ms=0)


class TestKip429OnPartitionsLost:
    """When the broker forcibly removes the member (heartbeat /
    commit / sync UnknownMemberId, IllegalGeneration, or fenced
    instance) the next rebalance must surface on_partitions_lost
    instead of on_partitions_revoked - the prior commit attempts
    have already failed, so the user can't safely commit on the
    way out."""

    def test_default_listener_falls_through_to_revoked(self):
        """ConsumerRebalanceListener.on_partitions_lost defaults to
        on_partitions_revoked so listeners written before KIP-429
        keep working."""
        calls = []

        class OldListener(ConsumerRebalanceListener):
            def on_partitions_revoked(self, revoked):
                calls.append(('revoked', revoked))
            def on_partitions_assigned(self, assigned):
                calls.append(('assigned', assigned))

        lost = {TopicPartition('t', 0)}
        OldListener().on_partitions_lost(lost)
        assert calls == [('revoked', lost)]

    def test_async_default_listener_falls_through_to_revoked(self, coordinator):
        """Same default for the async listener: on_partitions_lost
        awaits on_partitions_revoked."""
        calls = []

        class OldAsync(AsyncConsumerRebalanceListener):
            async def on_partitions_revoked(self, revoked):
                calls.append(('revoked', revoked))
            async def on_partitions_assigned(self, assigned):
                calls.append(('assigned', assigned))

        lost = {TopicPartition('t', 0)}
        coordinator._manager.run(OldAsync().on_partitions_lost, lost)
        assert calls == [('revoked', lost)]

    def test_generation_is_lost(self):
        """Generation.is_lost() mirrors Java's NO_GENERATION-or-empty-memberId
        check used by ConsumerCoordinator.onJoinPrepare for KIP-429."""
        from kafka.coordinator.base import DEFAULT_GENERATION_ID, UNKNOWN_MEMBER_ID
        # The sentinel itself is lost by construction.
        assert Generation.NO_GENERATION.is_lost() is True
        # A real generation is not lost.
        assert Generation(42, 'mbr-1', 'range').is_lost() is False
        # MemberIdRequiredError retry shape: real member id but generation
        # not yet assigned. Java's OR-check trips this; lost branch is a
        # no-op because assigned_partitions is empty at this point.
        assert Generation(DEFAULT_GENERATION_ID, 'mbr-1', None).is_lost() is True
        # Defensive: a real generation_id with a cleared member_id - shouldn't
        # happen in practice but the check should still trip.
        assert Generation(42, UNKNOWN_MEMBER_ID, 'range').is_lost() is True

    def test_reset_generation_marks_generation_lost(self, coordinator):
        """reset_generation() always leaves the live Generation in a
        lost state; ConsumerCoordinator reads that in
        _on_join_prepare_async to fire on_partitions_lost."""
        coordinator._generation = Generation(42, 'mbr-1', 'range')
        assert coordinator._generation.is_lost() is False
        coordinator.reset_generation()
        assert coordinator._generation.is_lost() is True

    def test_on_join_prepare_fires_lost_and_clears_assignment(
            self, mocker, coordinator):
        """After a forced eviction, _on_join_prepare_async invokes
        on_partitions_lost with the prior assignment, clears local
        assignment, and skips the eager on_partitions_revoked path."""
        coordinator.config['enable_auto_commit'] = False
        listener = mocker.MagicMock(spec=ConsumerRebalanceListener)
        coordinator._subscription.subscribe(topics=['t'], listener=listener)
        coordinator._subscription.assign_from_subscribed([
            TopicPartition('t', 0), TopicPartition('t', 1)])
        # Simulate the broker booting us.
        coordinator.reset_generation()
        assert coordinator._generation.is_lost() is True

        coordinator._manager.run(
            coordinator._on_join_prepare_async, 0, 'member-foo')

        listener.on_partitions_lost.assert_called_once_with(
            {TopicPartition('t', 0), TopicPartition('t', 1)})
        listener.on_partitions_revoked.assert_not_called()
        # Local assignment is cleared so subsequent code doesn't keep
        # treating the lost partitions as owned.
        assert coordinator._subscription.assigned_partitions() == set()

    def test_on_join_prepare_skips_auto_commit_when_lost(
            self, mocker, coordinator):
        """A forced eviction means the pre-rebalance commit would fail
        with the same error; skip it instead of logging the spurious
        'likely duplicate delivery' warning."""
        coordinator.config['enable_auto_commit'] = True
        coordinator._subscription.subscribe(topics=['t'])
        coordinator._subscription.assign_from_subscribed([TopicPartition('t', 0)])
        commit_spy = mocker.patch.object(
            coordinator, '_commit_offsets_sync_async')
        coordinator.reset_generation()

        coordinator._manager.run(
            coordinator._on_join_prepare_async, 0, 'member-foo')

        commit_spy.assert_not_called()

    def test_on_join_prepare_after_lost_then_normal(
            self, mocker, coordinator):
        """A subsequent rebalance against a real (non-lost) generation
        runs the normal prepare path. In production the rejoin that
        follows the lost path lands a real generation in
        _process_join_group_response; here we install one directly."""
        coordinator.config['enable_auto_commit'] = False
        listener = mocker.MagicMock(spec=ConsumerRebalanceListener)
        coordinator._subscription.subscribe(topics=['t'], listener=listener)
        coordinator._subscription.assign_from_subscribed([TopicPartition('t', 0)])
        coordinator.reset_generation()

        # First call: lost path.
        coordinator._manager.run(
            coordinator._on_join_prepare_async, 0, 'member-foo')
        # Re-assign and install a real generation; the next prepare
        # should fire revoked, not lost.
        coordinator._generation = Generation(42, 'mbr-1', 'range')
        coordinator._subscription.assign_from_subscribed([TopicPartition('t', 0)])
        listener.reset_mock()
        coordinator._manager.run(
            coordinator._on_join_prepare_async, 42, 'mbr-1')
        listener.on_partitions_lost.assert_not_called()
        listener.on_partitions_revoked.assert_called_once_with(
            {TopicPartition('t', 0)})

    def test_heartbeat_illegal_generation_marks_generation_lost(self, coordinator):
        """Heartbeat IllegalGenerationError forces reset_generation; the
        live generation must trip is_lost() so the next rebalance fires
        on_partitions_lost."""
        from kafka.protocol.consumer import HeartbeatResponse
        coordinator.coordinator_id = 0
        coordinator._generation = Generation(42, 'mbr-1', 'range')
        # Build a HeartbeatResponse with IllegalGenerationError code.
        response = HeartbeatResponse[0](Errors.IllegalGenerationError.errno)
        with pytest.raises(Errors.IllegalGenerationError):
            coordinator._handle_heartbeat_response(response, time.monotonic())
        assert coordinator._generation.is_lost() is True
        assert coordinator.state == MemberState.UNJOINED

    def test_heartbeat_unknown_member_id_marks_generation_lost(self, coordinator):
        from kafka.protocol.consumer import HeartbeatResponse
        coordinator.coordinator_id = 0
        coordinator._generation = Generation(42, 'mbr-1', 'range')
        response = HeartbeatResponse[0](Errors.UnknownMemberIdError.errno)
        with pytest.raises(Errors.UnknownMemberIdError):
            coordinator._handle_heartbeat_response(response, time.monotonic())
        assert coordinator._generation.is_lost() is True

    def test_heartbeat_rebalance_in_progress_keeps_generation(self, coordinator):
        """RebalanceInProgress is a normal rebalance signal, not a forced
        eviction - the live generation must stay valid."""
        from kafka.protocol.consumer import HeartbeatResponse
        coordinator.coordinator_id = 0
        coordinator._generation = Generation(42, 'mbr-1', 'range')
        response = HeartbeatResponse[0](Errors.RebalanceInProgressError.errno)
        with pytest.raises(Errors.RebalanceInProgressError):
            coordinator._handle_heartbeat_response(response, time.monotonic())
        assert coordinator._generation.is_lost() is False

    def test_commit_response_illegal_generation_marks_generation_lost(
            self, coordinator, offsets):
        """OffsetCommit IllegalGeneration forces reset_generation; the
        next rebalance must fire on_partitions_lost."""
        coordinator.coordinator_id = 0
        coordinator._generation = Generation(42, 'mbr-1', 'range')
        response = OffsetCommitResponse[0]([('foobar', [(0, 22), (1, 22)])])
        with pytest.raises(Errors.CommitFailedError):
            coordinator._handle_offset_commit_response(
                offsets, time.monotonic(), response)
        assert coordinator._generation.is_lost() is True

    def test_sync_group_illegal_generation_marks_generation_lost(self, coordinator):
        """SyncGroup IllegalGeneration forces reset_generation; the next
        rebalance must fire on_partitions_lost."""
        coordinator.coordinator_id = 0
        coordinator._generation = Generation(42, 'mbr-1', 'range')
        # SyncGroupResponse[0]: (error_code, assignment_bytes)
        response = SyncGroupResponse[0](Errors.IllegalGenerationError.errno, b'')
        with pytest.raises(Errors.IllegalGenerationError):
            coordinator._process_sync_group_response(response, time.monotonic())
        assert coordinator._generation.is_lost() is True

    def test_lost_async_listener_is_awaited(self, coordinator):
        """An AsyncConsumerRebalanceListener with on_partitions_lost
        override is awaited from the prepare path."""
        coordinator.config['enable_auto_commit'] = False
        calls = []

        class AsyncListener(AsyncConsumerRebalanceListener):
            async def on_partitions_revoked(self, revoked):
                calls.append(('revoked', revoked))
            async def on_partitions_assigned(self, assigned):
                calls.append(('assigned', assigned))
            async def on_partitions_lost(self, lost):
                calls.append(('lost', lost))

        coordinator._subscription.subscribe(topics=['t'], listener=AsyncListener())
        coordinator._subscription.assign_from_subscribed([TopicPartition('t', 0)])
        coordinator.reset_generation()
        coordinator._manager.run(
            coordinator._on_join_prepare_async, 0, 'member-foo')

        assert calls == [('lost', {TopicPartition('t', 0)})]

    def test_lost_listener_exception_propagates(self, mocker, coordinator):
        """Java parity: a throwing on_partitions_lost still runs the
        cleanup (clear assignment, reset_group_subscription) but then
        raises a KafkaError chaining the user exception."""
        coordinator.config['enable_auto_commit'] = False
        listener = mocker.MagicMock(spec=ConsumerRebalanceListener)
        crash = RuntimeError('listener crash')
        listener.on_partitions_lost.side_effect = crash
        coordinator._subscription.subscribe(topics=['t'], listener=listener)
        coordinator._subscription.assign_from_subscribed([TopicPartition('t', 0)])
        coordinator.reset_generation()

        with pytest.raises(Errors.KafkaError) as exc_info:
            coordinator._manager.run(
                coordinator._on_join_prepare_async, 0, 'member-foo')
        assert exc_info.value.__cause__ is crash

        listener.on_partitions_lost.assert_called_once()
        # Cleanup still ran before the exception bubbled out.
        assert coordinator._subscription.assigned_partitions() == set()


class TestKip429MemberIdPreservation:
    """Java parity: IllegalGeneration on heartbeat / commit / sync
    preserves the member_id (only the generation_id is reset);
    UnknownMemberId clears both. Reduces a MemberIdRequired round-trip
    on the subsequent JoinGroup."""

    def test_heartbeat_illegal_generation_preserves_member_id(self, coordinator):
        from kafka.protocol.consumer import HeartbeatResponse
        coordinator.coordinator_id = 0
        coordinator._generation = Generation(42, 'mbr-1', 'range')
        response = HeartbeatResponse[0](Errors.IllegalGenerationError.errno)
        with pytest.raises(Errors.IllegalGenerationError):
            coordinator._handle_heartbeat_response(response, time.monotonic())
        assert coordinator._generation.member_id == 'mbr-1'
        # generation_id still cleared (lost detection still trips).
        assert coordinator._generation.is_lost() is True

    def test_heartbeat_unknown_member_id_resets_member_id(self, coordinator):
        from kafka.protocol.consumer import HeartbeatResponse
        coordinator.coordinator_id = 0
        coordinator._generation = Generation(42, 'mbr-1', 'range')
        response = HeartbeatResponse[0](Errors.UnknownMemberIdError.errno)
        with pytest.raises(Errors.UnknownMemberIdError):
            coordinator._handle_heartbeat_response(response, time.monotonic())
        # Broker has forgotten us; clear member_id entirely.
        assert coordinator._generation.has_member_id() is False

    def test_sync_group_illegal_generation_preserves_member_id(self, coordinator):
        coordinator.coordinator_id = 0
        coordinator._generation = Generation(42, 'mbr-1', 'range')
        response = SyncGroupResponse[0](Errors.IllegalGenerationError.errno, b'')
        with pytest.raises(Errors.IllegalGenerationError):
            coordinator._process_sync_group_response(response, time.monotonic())
        assert coordinator._generation.member_id == 'mbr-1'
        assert coordinator._generation.is_lost() is True

    def test_sync_group_unknown_member_id_resets_member_id(self, coordinator):
        coordinator.coordinator_id = 0
        coordinator._generation = Generation(42, 'mbr-1', 'range')
        response = SyncGroupResponse[0](Errors.UnknownMemberIdError.errno, b'')
        with pytest.raises(Errors.UnknownMemberIdError):
            coordinator._process_sync_group_response(response, time.monotonic())
        assert coordinator._generation.has_member_id() is False

    def test_commit_response_illegal_generation_preserves_member_id(
            self, coordinator, offsets):
        coordinator.coordinator_id = 0
        coordinator._generation = Generation(42, 'mbr-1', 'range')
        response = OffsetCommitResponse[0]([('foobar', [(0, 22), (1, 22)])])
        with pytest.raises(Errors.CommitFailedError):
            coordinator._handle_offset_commit_response(
                offsets, time.monotonic(), response)
        assert coordinator._generation.member_id == 'mbr-1'
        assert coordinator._generation.is_lost() is True

    def test_commit_response_unknown_member_id_resets_member_id(
            self, coordinator, offsets):
        coordinator.coordinator_id = 0
        coordinator._generation = Generation(42, 'mbr-1', 'range')
        # OffsetCommit UnknownMemberId error code = 25
        response = OffsetCommitResponse[0]([('foobar', [(0, 25), (1, 25)])])
        with pytest.raises(Errors.CommitFailedError):
            coordinator._handle_offset_commit_response(
                offsets, time.monotonic(), response)
        assert coordinator._generation.has_member_id() is False


class TestListenerExceptionPropagation:
    """Java parity (Diff 3): listener exceptions are captured during
    cleanup and re-raised at the end of the rebalance phase as a
    KafkaError chaining the user exception."""

    def _make_assignment_bytes(self, topic, partitions):
        from kafka.protocol.consumer.metadata import ConsumerProtocolAssignment
        a = ConsumerProtocolAssignment(
            version=1,
            assigned_partitions=[(topic, sorted(partitions))],
            user_data=b'')
        return a.encode()

    def test_cooperative_both_listeners_called_even_if_revoked_throws(
            self, mocker, client, metrics):
        """Java's invokePartitionsRevoked / invokePartitionsAssigned
        pattern: even if on_partitions_revoked throws, on_partitions_assigned
        still runs (so the user gets a chance to react to the new
        assignment). The final raised exception is the last one captured."""
        coord = _cooperative_coordinator(client, metrics)
        try:
            coord.config['enable_auto_commit'] = False
            listener = mocker.MagicMock(spec=ConsumerRebalanceListener)
            revoked_crash = RuntimeError('revoked crash')
            listener.on_partitions_revoked.side_effect = revoked_crash
            coord._subscription.subscribe(topics=['t'], listener=listener)
            coord._subscription.assign_from_subscribed([
                TopicPartition('t', 0), TopicPartition('t', 1)])

            assignment_bytes = self._make_assignment_bytes('t', [1, 2])
            with pytest.raises(Errors.KafkaError):
                coord._manager.run(
                    coord._on_join_complete_async,
                    42, 'member-1', 'cooperative-sticky', assignment_bytes)

            # Both listeners were called - revoked even though it threw.
            listener.on_partitions_revoked.assert_called_once_with(
                {TopicPartition('t', 0)})
            listener.on_partitions_assigned.assert_called_once_with(
                {TopicPartition('t', 2)})
        finally:
            coord.close(timeout_ms=0)

    def test_cooperative_assigned_throw_propagates(
            self, mocker, client, metrics):
        """If only on_partitions_assigned throws, that's the captured
        exception."""
        coord = _cooperative_coordinator(client, metrics)
        try:
            coord.config['enable_auto_commit'] = False
            listener = mocker.MagicMock(spec=ConsumerRebalanceListener)
            assigned_crash = RuntimeError('assigned crash')
            listener.on_partitions_assigned.side_effect = assigned_crash
            coord._subscription.subscribe(topics=['t'], listener=listener)
            coord._subscription.assign_from_subscribed([TopicPartition('t', 0)])

            assignment_bytes = self._make_assignment_bytes('t', [0, 1])
            with pytest.raises(Errors.KafkaError) as exc_info:
                coord._manager.run(
                    coord._on_join_complete_async,
                    42, 'member-1', 'cooperative-sticky', assignment_bytes)
            assert exc_info.value.__cause__ is assigned_crash
        finally:
            coord.close(timeout_ms=0)


class TestOnJoinCompleteBailOnInvalidAssignment:
    """The EAGER branch has the same bail-on-ValueError behaviour as
    COOPERATIVE - if the leader hands us a topic we don't subscribe
    to, we must request re-join without firing the listener or
    updating assignor state."""

    def _make_assignment_bytes(self, topic, partitions):
        from kafka.protocol.consumer.metadata import ConsumerProtocolAssignment
        a = ConsumerProtocolAssignment(
            version=0,
            assigned_partitions=[(topic, sorted(partitions))],
            user_data=b'')
        return a.encode()

    def test_eager_assignment_for_unsubscribed_topic_bails(
            self, mocker, coordinator):
        coordinator.config['enable_auto_commit'] = False
        listener = mocker.MagicMock(spec=ConsumerRebalanceListener)
        coordinator._subscription.subscribe(topics=['t'], listener=listener)
        coordinator._subscription.assign_from_subscribed([TopicPartition('t', 0)])
        # Pick whichever assignor name is in the default fixture
        # (RangePartitionAssignor).
        assignor_name = next(iter(coordinator._assignors))
        assignor_spy = mocker.spy(
            coordinator._assignors[assignor_name], 'on_assignment')
        rejoin_spy = mocker.spy(coordinator, 'request_rejoin')
        old_deadline = coordinator.next_auto_commit_deadline

        bad_assignment = self._make_assignment_bytes('other-topic', [0])
        coordinator._manager.run(
            coordinator._on_join_complete_async,
            42, 'member-1', assignor_name, bad_assignment)

        rejoin_spy.assert_called_once()
        listener.on_partitions_assigned.assert_not_called()
        assignor_spy.assert_not_called()
        assert coordinator.next_auto_commit_deadline == old_deadline


class TestOnCloseRevokesPartitions:
    """KIP-429 / Java onLeavePrepare parity: closing the consumer
    notifies the rebalance listener that the owned partitions are being
    given up (on_partitions_revoked, or on_partitions_lost if the group
    membership was already lost), then clears the local assignment.

    Changed in kafka-python 3.0: previously the listener was *not*
    invoked on close.
    """

    def _stub_leave_group(self, mocker, coordinator):
        """Mock everything close() touches except the listener path."""
        mocker.patch.object(coordinator, '_maybe_auto_commit_offsets_sync')
        mocker.patch.object(coordinator, '_handle_leave_group_response')
        mocker.patch.object(coordinator, 'coordinator_unknown',
                            return_value=False)
        coordinator.coordinator_id = 0
        cli = coordinator._client
        net = cli._net
        mocker.patch.object(cli._manager, 'send',
                            return_value=net.create_future().success('foobar'))
        mocker.patch.object(cli, 'poll')

    def test_close_revokes_for_live_group(self, mocker, coordinator):
        self._stub_leave_group(mocker, coordinator)
        listener = mocker.MagicMock(spec=ConsumerRebalanceListener)
        coordinator._subscription.subscribe(topics=['t'], listener=listener)
        coordinator._subscription.assign_from_subscribed(
            [TopicPartition('t', 0), TopicPartition('t', 1)])
        coordinator._generation = Generation(42, 'mbr-1', 'range')
        coordinator.state = MemberState.STABLE

        coordinator.close()

        listener.on_partitions_revoked.assert_called_once_with(
            {TopicPartition('t', 0), TopicPartition('t', 1)})
        listener.on_partitions_lost.assert_not_called()
        # Local assignment is cleared once we've given up the partitions.
        assert coordinator._subscription.assigned_partitions() == set()

    def test_close_fires_lost_when_generation_lost(self, mocker, coordinator):
        self._stub_leave_group(mocker, coordinator)
        listener = mocker.MagicMock(spec=ConsumerRebalanceListener)
        coordinator._subscription.subscribe(topics=['t'], listener=listener)
        coordinator._subscription.assign_from_subscribed([TopicPartition('t', 0)])
        # The broker already booted us before close() was called.
        coordinator.reset_generation()
        assert coordinator._generation.is_lost() is True

        coordinator.close()

        listener.on_partitions_lost.assert_called_once_with(
            {TopicPartition('t', 0)})
        listener.on_partitions_revoked.assert_not_called()
        assert coordinator._subscription.assigned_partitions() == set()

    def test_close_manual_assignment_fires_nothing(self, mocker, coordinator):
        """USER_ASSIGNED partitions are never auto-managed, so close()
        must not invoke the listener for them."""
        self._stub_leave_group(mocker, coordinator)
        listener = mocker.MagicMock(spec=ConsumerRebalanceListener)
        # Manual assign() and subscribe() are mutually exclusive, so attach
        # the listener directly rather than via subscribe().
        coordinator._subscription.assign_from_user([TopicPartition('t', 0)])
        coordinator._subscription.rebalance_listener = listener

        coordinator.close()

        listener.on_partitions_revoked.assert_not_called()
        listener.on_partitions_lost.assert_not_called()

    def test_close_empty_assignment_fires_nothing(self, mocker, coordinator):
        """Subscribed but never assigned any partitions -- nothing to
        revoke, so the listener is not called."""
        self._stub_leave_group(mocker, coordinator)
        listener = mocker.MagicMock(spec=ConsumerRebalanceListener)
        coordinator._subscription.subscribe(topics=['t'], listener=listener)

        coordinator.close()

        listener.on_partitions_revoked.assert_not_called()
        listener.on_partitions_lost.assert_not_called()

    def test_close_async_listener_is_awaited(self, mocker, coordinator):
        self._stub_leave_group(mocker, coordinator)
        calls = []

        class AsyncListener(AsyncConsumerRebalanceListener):
            async def on_partitions_revoked(self, revoked):
                calls.append(('revoked', revoked))
            async def on_partitions_assigned(self, assigned):
                calls.append(('assigned', assigned))
            async def on_partitions_lost(self, lost):
                calls.append(('lost', lost))

        coordinator._subscription.subscribe(topics=['t'], listener=AsyncListener())
        coordinator._subscription.assign_from_subscribed([TopicPartition('t', 0)])
        coordinator._generation = Generation(42, 'mbr-1', 'range')
        coordinator.state = MemberState.STABLE

        coordinator.close()

        assert calls == [('revoked', {TopicPartition('t', 0)})]

    def test_close_listener_exception_still_leaves_group(self, mocker, coordinator):
        """A throwing listener must not prevent the consumer from leaving
        the group; the exception surfaces after cleanup as a KafkaError."""
        self._stub_leave_group(mocker, coordinator)
        listener = mocker.MagicMock(spec=ConsumerRebalanceListener)
        crash = RuntimeError('listener crash')
        listener.on_partitions_revoked.side_effect = crash
        coordinator._subscription.subscribe(topics=['t'], listener=listener)
        coordinator._subscription.assign_from_subscribed([TopicPartition('t', 0)])
        coordinator._generation = Generation(42, 'mbr-1', 'range')
        coordinator.state = MemberState.STABLE

        with pytest.raises(Errors.KafkaError) as exc_info:
            coordinator.close()
        assert exc_info.value.__cause__ is crash

        # Cleanup still ran: assignment cleared and the group was left.
        assert coordinator._subscription.assigned_partitions() == set()
        coordinator._handle_leave_group_response.assert_called_with('foobar')

    def test_close_no_autocommit_still_revokes(self, mocker, coordinator):
        """autocommit=False skips the commit but must still fire the
        revoke listener on close."""
        self._stub_leave_group(mocker, coordinator)
        listener = mocker.MagicMock(spec=ConsumerRebalanceListener)
        coordinator._subscription.subscribe(topics=['t'], listener=listener)
        coordinator._subscription.assign_from_subscribed([TopicPartition('t', 0)])
        coordinator._generation = Generation(42, 'mbr-1', 'range')
        coordinator.state = MemberState.STABLE

        coordinator.close(autocommit=False)

        coordinator._maybe_auto_commit_offsets_sync.assert_not_called()
        listener.on_partitions_revoked.assert_called_once_with(
            {TopicPartition('t', 0)})


# ---------------------------------------------------------------------------
# Heartbeat error responses must not route through _failed_request.
#
# Regression: _handle_heartbeat_response used to be called *inside* the
# `_send_heartbeat_request` send try/except, so any error it raised -- even a
# normal RebalanceInProgress -- was caught by `except KafkaError`, logged at
# error level, and marked the coordinator dead via _failed_request(). The fix
# moves response handling into an `else` clause so only genuine transport-level
# send failures reach _failed_request.
# ---------------------------------------------------------------------------


def _dispatch_heartbeat(coord):
    """Dispatch a single _send_heartbeat_request and pump the network until
    the resulting future resolves."""
    future = coord._manager.call_soon(coord._send_heartbeat_request)
    coord._client.poll(future=future, timeout_ms=5000)
    return future


def test_send_heartbeat_rebalance_in_progress_does_not_fail_request(
        mocker, broker, seeded_coord):
    """A normal rebalance (RebalanceInProgress heartbeat response) must request
    a rejoin without marking the coordinator dead or calling _failed_request."""
    failed_spy = mocker.spy(seeded_coord, '_failed_request')
    dead_spy = mocker.spy(seeded_coord, 'coordinator_dead')
    broker.respond(HeartbeatRequest, HeartbeatResponse(
        throttle_time_ms=0,
        error_code=Errors.RebalanceInProgressError.errno))

    future = _dispatch_heartbeat(seeded_coord)

    assert future.failed()
    assert isinstance(future.exception, Errors.RebalanceInProgressError)
    # The regression routed in-band error responses through _failed_request,
    # which logs an error and marks the coordinator dead.
    assert failed_spy.call_count == 0
    assert dead_spy.call_count == 0
    # In-band rebalance signal triggers a rejoin, not a dead coordinator.
    assert seeded_coord.rejoin_needed is True
    assert seeded_coord.coordinator_id == 0


@pytest.mark.parametrize('error_type', [
    Errors.RebalanceInProgressError,
    Errors.IllegalGenerationError,
    Errors.UnknownMemberIdError,
])
def test_send_heartbeat_membership_errors_do_not_mark_coordinator_dead(
        mocker, broker, seeded_coord, error_type):
    """Membership/rebalance heartbeat errors are handled in-band: they raise
    the matching error to the caller but never invoke _failed_request and
    never mark the coordinator dead."""
    failed_spy = mocker.spy(seeded_coord, '_failed_request')
    dead_spy = mocker.spy(seeded_coord, 'coordinator_dead')
    broker.respond(HeartbeatRequest, HeartbeatResponse(
        throttle_time_ms=0, error_code=error_type.errno))

    future = _dispatch_heartbeat(seeded_coord)

    assert future.failed()
    assert isinstance(future.exception, error_type)
    assert failed_spy.call_count == 0
    assert dead_spy.call_count == 0
    assert seeded_coord.coordinator_id == 0


def test_send_heartbeat_transport_failure_marks_coordinator_dead(
        mocker, broker, seeded_coord):
    """A transport-level send failure (not an in-band error response) still
    routes through _failed_request so the coordinator is marked dead."""
    failed_spy = mocker.spy(seeded_coord, '_failed_request')
    error = Errors.KafkaConnectionError('simulated transport failure')
    broker.fail_next(HeartbeatRequest, error=error)

    future = _dispatch_heartbeat(seeded_coord)

    assert future.failed()
    assert future.exception is error
    assert failed_spy.call_count == 1
    # node 0 is captured by the spy; _failed_request -> coordinator_dead has
    # since reset seeded_coord.coordinator_id to None.
    node_id, request, call_error = failed_spy.call_args[0]
    assert node_id == 0
    assert isinstance(request, HeartbeatRequest)
    assert call_error is error
    assert seeded_coord.coordinator_id is None


# ---------------------------------------------------------------------------
# Heartbeat coordinator failover
# (https://github.com/dpkp/kafka-python/issues/1134)
# ---------------------------------------------------------------------------


def _seed_stable_group(coordinator, coordinator_id=0):
    coordinator._subscription.subscribe(topics=['foobar'])
    coordinator.coordinator_id = coordinator_id
    coordinator._generation = Generation(1, 'member-1', b'')
    coordinator.state = MemberState.STABLE
    coordinator.rejoin_needed = False


def _find_coordinator_response(group_id, broker):
    """A successful FindCoordinatorResponse naming `broker` as coordinator.

    Top-level fields cover v0-v3; the coordinators array covers v4+
    (whichever version the connection negotiates)."""
    Coordinator = FindCoordinatorResponse.Coordinator
    return FindCoordinatorResponse(
        throttle_time_ms=0, error_code=0, error_message=None,
        node_id=broker.node_id, host=broker.host, port=broker.port,
        coordinators=[Coordinator(
            key=group_id, node_id=broker.node_id,
            host=broker.host, port=broker.port,
            error_code=0, error_message=None)])


def _run_heartbeat_loop_until(coordinator, condition, timeout=5.0):
    """Run the real _heartbeat_loop coroutine until condition() or timeout,
    then shut the loop down cleanly."""
    net = coordinator._manager._net
    coordinator._maybe_start_heartbeat_loop()
    coordinator._enable_heartbeat()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and not condition():
        net.poll(timeout_ms=10)
    coordinator._close_heartbeat()
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and not coordinator._heartbeat_loop_future.is_done:
        net.poll(timeout_ms=10)


def test_heartbeat_coordinator_not_available_recovers(mocker, broker, client, metrics):
    """A heartbeat answered with CoordinatorNotAvailableError must mark the
    coordinator dead, rediscover it via FindCoordinator, and resume
    heartbeating -- not retry forever against the dead coordinator."""
    coordinator = ConsumerCoordinator(
        client, SubscriptionState(), metrics=metrics,
        api_version=broker.broker_version,
        heartbeat_interval_ms=10, retry_backoff_ms=10)
    try:
        client._manager.bootstrap(timeout_ms=5000)
        _seed_stable_group(coordinator)
        dead_spy = mocker.spy(coordinator, 'coordinator_dead')

        # First heartbeat: the broker reports the coordinator gone.
        broker.respond(HeartbeatRequest, HeartbeatResponse(
            throttle_time_ms=0,
            error_code=Errors.CoordinatorNotAvailableError.errno))
        # Rediscovery: this broker is (still) the coordinator.
        broker.respond_always(
            FindCoordinatorRequest,
            _find_coordinator_response(coordinator.group_id, broker))
        # All heartbeats after the scripted failure succeed.
        recovered = []
        def ok_heartbeat(api_key, api_version, correlation_id, request_bytes):
            recovered.append(correlation_id)
            return HeartbeatResponse(throttle_time_ms=0, error_code=0)
        broker.respond_always(HeartbeatRequest, ok_heartbeat)

        _run_heartbeat_loop_until(coordinator, lambda: recovered)

        assert recovered, 'heartbeat never resumed after coordinator rediscovery'
        assert isinstance(dead_spy.call_args_list[0][0][0],
                          Errors.CoordinatorNotAvailableError)
        # Rediscovered via FindCoordinator (synthesized coordinator node id).
        assert coordinator.coordinator_id == 'coordinator-0'
        # The error does not invalidate the generation or group membership.
        assert coordinator.state is MemberState.STABLE
        assert not coordinator._generation.is_lost()
    finally:
        coordinator._close_heartbeat()
        coordinator.reset_generation()  # skip LeaveGroup wire traffic in close()
        coordinator.close(timeout_ms=0)


def test_heartbeat_coordinator_broker_failover(mocker, net, metrics):
    """When the coordinator broker dies (connections dropped and refused),
    the consumer must fail the heartbeat at the transport level, rediscover
    the coordinator on a surviving broker, and resume heartbeating against
    the newly elected coordinator."""
    mock_cluster = MockCluster(num_brokers=2)
    manager = KafkaConnectionManager(
        net,
        bootstrap_servers=mock_cluster.bootstrap_servers(),
        api_version=mock_cluster.broker_version,
        request_timeout_ms=5000)
    mock_cluster.attach(manager)
    client = KafkaNetClient(net=net, manager=manager)
    coordinator = ConsumerCoordinator(
        client, SubscriptionState(), metrics=metrics,
        api_version=mock_cluster.broker_version,
        heartbeat_interval_ms=10, retry_backoff_ms=10)
    try:
        manager.bootstrap(timeout_ms=5000)
        _seed_stable_group(coordinator, coordinator_id=0)
        dead_spy = mocker.spy(coordinator, 'coordinator_dead')

        # Broker 0 (the coordinator) dies before the next heartbeat, and the
        # cluster elects broker 1: surviving brokers answer FindCoordinator
        # with the new coordinator.
        mock_cluster[0].stop()
        mock_cluster.set_coordinator(coordinator.group_id, 1)
        recovered = []
        def ok_heartbeat(api_key, api_version, correlation_id, request_bytes):
            recovered.append(correlation_id)
            return HeartbeatResponse(throttle_time_ms=0, error_code=0)
        mock_cluster[1].respond_always(HeartbeatRequest, ok_heartbeat)

        _run_heartbeat_loop_until(coordinator, lambda: recovered)

        assert recovered, 'heartbeat never resumed on the new coordinator'
        # The failed send to the dead broker marked the coordinator dead...
        assert dead_spy.call_count >= 1
        # ...and rediscovery pointed at broker 1.
        assert coordinator.coordinator_id == 'coordinator-1'
        assert coordinator.state is MemberState.STABLE
        assert not coordinator._generation.is_lost()
    finally:
        coordinator._close_heartbeat()
        coordinator.reset_generation()
        coordinator.close(timeout_ms=0)
        client.close()
        manager.close()


def _apply_topic_metadata(cluster, topic, num_partitions, node_id=0):
    """Build a MetadataResponse advertising `topic` with `num_partitions` and
    apply it to `cluster`, firing its metadata-update listeners (which includes
    the coordinator's _handle_metadata_update)."""
    Broker = MetadataResponse.MetadataResponseBroker
    Topic = MetadataResponse.MetadataResponseTopic
    Partition = Topic.MetadataResponsePartition
    response = MetadataResponse(
        version=8, throttle_time_ms=0,
        brokers=[Broker(node_id=node_id, host='localhost', port=9092, rack=None, version=8)],
        cluster_id='mock-cluster', controller_id=node_id,
        topics=[Topic(version=8, error_code=0, name=topic, is_internal=False,
                      partitions=[
                          Partition(version=8, error_code=0, partition_index=p,
                                    leader_id=node_id, leader_epoch=0,
                                    replica_nodes=[node_id], isr_nodes=[node_id],
                                    offline_replicas=[])
                          for p in range(num_partitions)])])
    # Round-trip through the wire so every nested container carries _version,
    # the same shape update_metadata sees from a real MetadataResponse.
    cluster.update_metadata(MetadataResponse.decode(response.encode(), version=8))


def _single_member(topic, member_id='member-1'):
    return [JoinGroupResponse.JoinGroupResponseMember(
        member_id=member_id,
        metadata=ConsumerProtocolSubscription(0, [topic], b'').encode())]


def test_leader_rejoins_when_metadata_changes_after_assignment(coordinator):
    """Once the leader has assigned partitions, a metadata change that alters
    the subscribed topic's partition count must flip need_rejoin()
    to True so the change is not silently lost."""
    coordinator._subscription.subscribe(topics=['t'])
    # Topic has 1 partition when the leader computes the assignment.
    _apply_topic_metadata(coordinator._cluster, 't', num_partitions=1)
    assert coordinator._metadata_snapshot == {'t': {0}}

    # Perform assignment -> becomes leader and snapshots the metadata used.
    coordinator._perform_assignment('member-1', 'range', _single_member('t'))
    assert coordinator._is_leader
    assert coordinator._assignment_snapshot == {'t': {0}}

    # Simulate the rest of a completed join (the leader keeps its snapshot).
    coordinator._joined_subscription = {'t'}
    coordinator._generation = Generation(1, 'member-1', 'range')
    coordinator.state = MemberState.STABLE
    coordinator.rejoin_needed = False
    assert not coordinator.need_rejoin()

    # Topic grows to 2 partitions while the member is assigned. The metadata
    # listener updates _metadata_snapshot; the assignment snapshot is now stale.
    _apply_topic_metadata(coordinator._cluster, 't', num_partitions=2)
    assert coordinator._metadata_snapshot == {'t': {0, 1}}
    assert coordinator._assignment_snapshot == {'t': {0}}
    assert coordinator.need_rejoin()


def test_follower_does_not_rejoin_on_metadata_change(coordinator):
    """Only the leader watches metadata for partition changes. A follower
    (assignment_snapshot is None, cleared in _on_join_complete) must not rejoin
    just because the topic's partition count changed."""
    coordinator._subscription.subscribe(topics=['t'])
    _apply_topic_metadata(coordinator._cluster, 't', num_partitions=1)

    # Simulate a completed join as a follower: snapshot cleared, joined.
    coordinator._assignment_snapshot = None
    coordinator._is_leader = False
    coordinator._joined_subscription = {'t'}
    coordinator._generation = Generation(1, 'member-1', 'range')
    coordinator.state = MemberState.STABLE
    coordinator.rejoin_needed = False
    assert not coordinator.need_rejoin()

    # Metadata changes -- a follower must not be driven to rejoin by it.
    _apply_topic_metadata(coordinator._cluster, 't', num_partitions=2)
    assert coordinator._metadata_snapshot == {'t': {0, 1}}
    assert not coordinator.need_rejoin()


def _metadata_topic(name, num_partitions, node_id=0):
    """A MetadataResponseTopic for MockCluster.set_metadata(topics=[...])."""
    Topic = MetadataResponse.MetadataResponseTopic
    Partition = Topic.MetadataResponsePartition
    return Topic(version=8, error_code=0, name=name, is_internal=False,
                 partitions=[
                     Partition(version=8, error_code=0, partition_index=p,
                               leader_id=node_id, leader_epoch=0,
                               replica_nodes=[node_id], isr_nodes=[node_id],
                               offline_replicas=[])
                     for p in range(num_partitions)])


def _group_coordinator(net, metrics, mock_cluster, **configs):
    """Build a ConsumerCoordinator wired to a MockCluster, bootstrapped."""
    manager = KafkaConnectionManager(
        net, bootstrap_servers=mock_cluster.bootstrap_servers(),
        api_version=mock_cluster.broker_version, request_timeout_ms=5000)
    mock_cluster.attach(manager)
    client = KafkaNetClient(net=net, manager=manager)
    coordinator = ConsumerCoordinator(
        client, SubscriptionState(), metrics=metrics,
        api_version=mock_cluster.broker_version,
        heartbeat_interval_ms=20, retry_backoff_ms=20, **configs)
    manager.bootstrap(timeout_ms=5000)
    return coordinator, manager, client


def test_mock_group_single_member_join_assigns_all_partitions(net, metrics):
    """Smoke test for MockCluster.add_group: a single consumer joins the group,
    is elected leader, runs the assignor, and is assigned every partition of
    the subscribed topic."""
    mock_cluster = MockCluster(num_brokers=1)
    mock_cluster.set_metadata(topics=[_metadata_topic('t', num_partitions=3)])
    group = mock_cluster.add_group('my-group', coordinator=0)
    coordinator, manager, client = _group_coordinator(
        net, metrics, mock_cluster, group_id='my-group')
    try:
        coordinator._subscription.subscribe(topics=['t'])
        # Ensure the cluster has the topic metadata the assignor needs.
        client.cluster.request_update()

        assert coordinator.ensure_active_group(timeout_ms=5000)

        assert coordinator._subscription.assigned_partitions() == {
            TopicPartition('t', 0), TopicPartition('t', 1), TopicPartition('t', 2)}
        assert coordinator._is_leader
        assert len(group.members) == 1
        assert group.generation == 1
    finally:
        coordinator._close_heartbeat()
        coordinator.reset_generation()
        coordinator.close(timeout_ms=0)
        client.close()
        manager.close()


def test_metadata_growth_triggers_rejoin_end_to_end(net, metrics):
    """KAFKA-3949 end-to-end: a consumer is the group leader with a 1-partition
    topic; the topic grows to 2 partitions; a metadata refresh must drive a
    rejoin so the consumer ends up assigned both partitions (the change is not
    lost)."""
    mock_cluster = MockCluster(num_brokers=1)
    mock_cluster.set_metadata(topics=[_metadata_topic('t', num_partitions=1)])
    group = mock_cluster.add_group('my-group', coordinator=0)
    coordinator, manager, client = _group_coordinator(
        net, metrics, mock_cluster, group_id='my-group')
    try:
        coordinator._subscription.subscribe(topics=['t'])
        assert coordinator.ensure_active_group(timeout_ms=5000)
        assert coordinator._subscription.assigned_partitions() == {TopicPartition('t', 0)}
        assert group.generation == 1

        # The topic grows to 2 partitions. Refresh metadata so the coordinator's
        # listener sees the change (the racing metadata update from KAFKA-3949).
        mock_cluster.set_metadata(topics=[_metadata_topic('t', num_partitions=2)])
        future = client.cluster.request_update()
        client.poll(future=future, timeout_ms=5000)

        # The leader's assignment snapshot is now stale -> must rejoin.
        assert coordinator.need_rejoin()

        # Rejoin picks up the new partition; the change was not lost.
        assert coordinator.ensure_active_group(timeout_ms=5000)
        assert coordinator._subscription.assigned_partitions() == {
            TopicPartition('t', 0), TopicPartition('t', 1)}
        assert group.generation == 2
    finally:
        coordinator._close_heartbeat()
        coordinator.reset_generation()
        coordinator.close(timeout_ms=0)
        client.close()
        manager.close()
