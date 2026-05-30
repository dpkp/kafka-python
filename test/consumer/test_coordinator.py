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
from kafka.protocol.consumer import (
    OffsetCommitRequest, OffsetCommitResponse,
    OffsetFetchRequest, OffsetFetchResponse,
    JoinGroupRequest, JoinGroupResponse,
    SyncGroupRequest, SyncGroupResponse,
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


def test_group_metadata_after_join(coordinator):
    """After joining, group_metadata() reflects the live generation."""
    coordinator._generation = Generation(generation_id=42,
                                         member_id='mbr-1',
                                         protocol='range')
    coordinator.state = MemberState.STABLE
    gm = coordinator.group_metadata()
    assert gm.generation_id == 42
    assert gm.member_id == 'mbr-1'
    # group_instance_id comes from config (None by default for this fixture).
    assert gm.group_instance_id is None

    # Still returns the snapshot even while rebalancing - the producer needs
    # *something* to send and the broker handles fencing.
    coordinator.state = MemberState.REBALANCING
    assert coordinator.group_metadata().generation_id == 42


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
    node_id, request, call_future, call_error = spy.call_args[0]
    assert node_id == 0
    assert isinstance(request, OffsetCommitRequest)
    assert call_future is None
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
    node_id, request, call_future, call_error = spy.call_args[0]
    assert node_id == 0
    assert isinstance(request, OffsetFetchRequest)
    assert call_future is None
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

    future = seeded_coord._manager.call_soon(
        seeded_coord._send_offset_fetch_request, partitions)
    seeded_coord._client.poll(future=future, timeout_ms=5000)

    assert future.succeeded()
    assert future.value == offsets
    assert spy.call_count == 1
    (response,) = spy.call_args[0]
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
        request, broker, seeded_coord):
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
    join_response_pending = Future()
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
        max_poll_interval_ms=300000,
        session_timeout_ms=10000,
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
                max_poll_interval_ms=300000,
                session_timeout_ms=10000,
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

    def test_lost_listener_exception_is_caught(self, mocker, coordinator):
        """A throwing on_partitions_lost must not abort the prepare path."""
        coordinator.config['enable_auto_commit'] = False
        listener = mocker.MagicMock(spec=ConsumerRebalanceListener)
        listener.on_partitions_lost.side_effect = RuntimeError('listener crash')
        coordinator._subscription.subscribe(topics=['t'], listener=listener)
        coordinator._subscription.assign_from_subscribed([TopicPartition('t', 0)])
        coordinator.reset_generation()

        # Must not raise; assignment still cleared.
        coordinator._manager.run(
            coordinator._on_join_prepare_async, 0, 'member-foo')

        listener.on_partitions_lost.assert_called_once()
        assert coordinator._subscription.assigned_partitions() == set()


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
