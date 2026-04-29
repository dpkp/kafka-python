# pylint: skip-file
"""MockBroker tests for TransactionManager.

These tests drive each TxnRequestHandler through a full wire round-trip via
the MockBroker, mirroring what the real Sender does for a transactional
request:

  handler = tm.next_request_handler(has_incomplete_batches=False)
  correlation_id = tm.next_in_flight_request_correlation_id()
  future = client.send(target_node, handler.request)
  future.add_both(handler.on_complete, correlation_id)
"""

import pytest

import kafka.errors as Errors
from kafka import KafkaProducer
from kafka.net.compat import KafkaNetClient
from kafka.producer.transaction_manager import (
    AddOffsetsToTxnHandler,
    AddPartitionsToTxnHandler,
    EndTxnHandler,
    FindCoordinatorHandler,
    InitProducerIdHandler,
    ProducerIdAndEpoch,
    TransactionManager,
    TransactionState,
    TxnOffsetCommitHandler,
)
from kafka.protocol.metadata import FindCoordinatorResponse, MetadataResponse
from kafka.protocol.producer import (
    AddOffsetsToTxnResponse,
    AddPartitionsToTxnResponse,
    EndTxnResponse,
    InitProducerIdResponse,
    ProduceRequest,
    ProduceResponse,
    TxnOffsetCommitResponse,
)
from kafka.record import MemoryRecords
from kafka.structs import OffsetAndMetadata, TopicPartition

from test.mock_broker import MockBroker
from test.test_mock_broker import _poll_for_future


_TXN_ID = 'test-txn'
_API_VERSION = (2, 5)
_PRODUCER_ID = 1234
_PRODUCER_EPOCH = 5


# ---------------------------------------------------------------------------
# Test harness
# ---------------------------------------------------------------------------


def _make_client(broker):
    """Build a KafkaNetClient wired to the MockBroker (version-negotiated)."""
    client = KafkaNetClient(
        bootstrap_servers='%s:%d' % (broker.host, broker.port),
        api_version=broker.broker_version,
        request_timeout_ms=5000,
        metadata_max_age_ms=300000,
    )
    broker.attach(client._manager)
    # Bootstrap so cluster metadata has the broker node and we have an
    # api_version mapping available for subsequent sends.
    client.check_version(timeout_ms=5000)
    return client


def _make_manager(client, api_version=_API_VERSION, seed_coord=True):
    """Build a TransactionManager with a valid producer_id in READY state.

    If ``seed_coord`` is True, pre-populates ``_transaction_coordinator``
    with the MockBroker's node_id so handlers that need a coordinator
    can dispatch directly without first going through FindCoordinator.
    """
    tm = TransactionManager(
        transactional_id=_TXN_ID,
        transaction_timeout_ms=60000,
        retry_backoff_ms=100,
        api_version=api_version,
        metadata=client.cluster,
    )
    tm.set_producer_id_and_epoch(ProducerIdAndEpoch(_PRODUCER_ID, _PRODUCER_EPOCH))
    tm._current_state = TransactionState.READY
    if seed_coord:
        # MockBroker's default node_id is 0; use it as the coordinator.
        tm._transaction_coordinator = 0
    return tm


def _dispatch_next(client, tm, default_target_node=0):
    """Pop the next handler from the manager's queue and send it over the wire.

    Mirrors the dispatch logic in ``Sender._maybe_send_transactional_request``:
    for coordinator-bound requests we pick the seeded coordinator node; for
    coordinator-less requests (FindCoordinator) we use ``default_target_node``.

    Returns ``(handler, future)`` where ``future`` resolves once the broker
    has responded and the handler's ``on_complete`` callback has run.
    """
    handler = tm.next_request_handler(has_incomplete_batches=False)
    assert handler is not None, 'no handler to dispatch'
    if handler.needs_coordinator():
        target_node = tm.coordinator(handler.coordinator_type)
        assert target_node is not None, (
            'coordinator for %s not seeded in test setup'
            % handler.coordinator_type)
    else:
        target_node = default_target_node
    client.await_ready(target_node, timeout_ms=5000)
    correlation_id = tm.next_in_flight_request_correlation_id()
    future = client.send(target_node, handler.request)
    future.add_both(handler.on_complete, correlation_id)
    return handler, future


def _pending_handlers(tm):
    return [h for _, _, h in tm._pending_requests]


@pytest.fixture
def broker():
    return MockBroker()


@pytest.fixture
def client(broker):
    c = _make_client(broker)
    yield c
    c.close()


# ---------------------------------------------------------------------------
# InitProducerIdHandler
# ---------------------------------------------------------------------------


class TestInitProducerIdHandlerMockBroker:

    def _enqueue_init(self, tm, is_epoch_bump=False):
        handler = InitProducerIdHandler(
            tm, transaction_timeout_ms=tm.transaction_timeout_ms,
            is_epoch_bump=is_epoch_bump)
        tm._enqueue_request(handler)
        return handler

    @pytest.mark.parametrize("error", [
        Errors.CoordinatorNotAvailableError,
        Errors.CoordinatorLoadInProgressError,
        Errors.NotCoordinatorError,
        Errors.ConcurrentTransactionsError,
    ])
    def test_retriable_error_reenqueues(self, broker, client, error):
        tm = _make_manager(client)
        # Manager is currently READY; InitProducerIdHandler treats retriable
        # errors uniformly via the getattr(error_type, 'retriable', False)
        # dispatch path.
        tm._current_state = TransactionState.INITIALIZING
        enqueued = self._enqueue_init(tm)

        broker.respond(InitProducerIdResponse, InitProducerIdResponse(
            throttle_time_ms=0,
            error_code=error.errno,
            producer_id=-1,
            producer_epoch=-1,
        ))

        handler, future = _dispatch_next(client, tm)
        assert handler is enqueued
        _poll_for_future(client, future)

        # Retriable error: handler should be back in the pending queue and
        # the manager should still be in INITIALIZING (no terminal transition).
        assert handler in _pending_handlers(tm)
        assert tm._current_state == TransactionState.INITIALIZING
        assert not tm.has_error()

    def test_coordinator_error_triggers_coordinator_lookup(self, broker, client):
        tm = _make_manager(client)
        tm._current_state = TransactionState.INITIALIZING
        self._enqueue_init(tm)

        broker.respond(InitProducerIdResponse, InitProducerIdResponse(
            throttle_time_ms=0,
            error_code=Errors.NotCoordinatorError.errno,
            producer_id=-1,
            producer_epoch=-1,
        ))

        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        # Coordinator lookup is enqueued ahead of the retry, and the current
        # coordinator has been invalidated.
        pending = _pending_handlers(tm)
        assert any(isinstance(h, FindCoordinatorHandler) for h in pending)
        assert any(isinstance(h, InitProducerIdHandler) for h in pending)
        assert tm._transaction_coordinator is None

    def test_success_transitions_to_ready(self, broker, client):
        tm = _make_manager(client)
        tm._current_state = TransactionState.INITIALIZING
        # Reset producer id to force a fresh allocation via the response.
        tm.set_producer_id_and_epoch(
            ProducerIdAndEpoch(-1, -1))
        handler = self._enqueue_init(tm)

        broker.respond(InitProducerIdResponse, InitProducerIdResponse(
            throttle_time_ms=0,
            error_code=0,
            producer_id=99,
            producer_epoch=0,
        ))

        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        assert tm._current_state == TransactionState.READY
        assert tm.producer_id_and_epoch.producer_id == 99
        assert tm.producer_id_and_epoch.epoch == 0
        assert handler._result.is_done
        assert not handler._result.failed

    @pytest.mark.parametrize("error,expected", [
        (Errors.ProducerFencedError, Errors.ProducerFencedError),
        # Java client normalizes non-bump INVALID_PRODUCER_EPOCH to
        # PRODUCER_FENCED on the InitProducerId path (KIP-360).
        (Errors.InvalidProducerEpochError, Errors.ProducerFencedError),
        (Errors.TransactionalIdAuthorizationFailedError,
         Errors.TransactionalIdAuthorizationFailedError),
    ])
    def test_fatal_error(self, broker, client, error, expected):
        tm = _make_manager(client)
        tm._current_state = TransactionState.INITIALIZING
        handler = self._enqueue_init(tm)

        broker.respond(InitProducerIdResponse, InitProducerIdResponse(
            throttle_time_ms=0, error_code=error.errno,
            producer_id=-1, producer_epoch=-1))

        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        assert tm._current_state == TransactionState.FATAL_ERROR
        assert tm.has_fatal_error()
        assert isinstance(tm.last_error, expected)
        assert handler._result.failed
        assert handler not in _pending_handlers(tm)

    def test_unknown_error_is_fatal(self, broker, client):
        tm = _make_manager(client)
        tm._current_state = TransactionState.INITIALIZING
        handler = self._enqueue_init(tm)

        # UnsupportedForMessageFormatError is not retriable, not in any of
        # the handler's named-error branches -- falls through to the catch-all
        # fatal_error.
        broker.respond(InitProducerIdResponse, InitProducerIdResponse(
            throttle_time_ms=0,
            error_code=Errors.UnsupportedForMessageFormatError.errno,
            producer_id=-1, producer_epoch=-1))

        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        assert tm._current_state == TransactionState.FATAL_ERROR
        assert handler._result.failed

    def test_bump_invalid_epoch_falls_back_to_fresh_init(self, broker, client):
        tm = _make_manager(client)
        # Bump from READY: bump_producer_id_and_epoch enqueues an epoch-bump
        # InitProducerIdHandler and transitions to BUMPING_PRODUCER_EPOCH.
        tm.bump_producer_id_and_epoch()
        assert tm.is_bumping_epoch()

        broker.respond(InitProducerIdResponse, InitProducerIdResponse(
            throttle_time_ms=0,
            error_code=Errors.InvalidProducerEpochError.errno,
            producer_id=-1, producer_epoch=-1))

        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        # INVALID_PRODUCER_EPOCH during a bump means our producer_id/epoch
        # are stale -- fall back to a fresh (non-bump) init. Manager stays
        # in BUMPING_PRODUCER_EPOCH; producer_id is reset; a new non-bump
        # InitProducerIdHandler is enqueued sharing the original result.
        assert tm.is_bumping_epoch()
        assert tm.producer_id_and_epoch.producer_id == -1
        fallbacks = [h for h in _pending_handlers(tm)
                     if isinstance(h, InitProducerIdHandler)]
        assert len(fallbacks) == 1
        assert fallbacks[0]._is_epoch_bump is False

    def test_idempotent_producer_init_then_bump(self, broker, client):
        """Idempotent (non-transactional) producer drives the same state
        machine as transactional: UNINITIALIZED -> INITIALIZING -> READY
        on init, then READY -> BUMPING_PRODUCER_EPOCH -> READY on a KIP-360
        epoch bump. Reproduces the failure mode from
        test_idempotent_producer_high_throughput where the idempotent
        producer was never driven out of UNINITIALIZED, causing a bump from
        an OUT_OF_ORDER_SEQUENCE_NUMBER produce response to raise an
        invalid-state-transition KafkaError.
        """
        tm = TransactionManager(
            transactional_id=None,
            api_version=_API_VERSION,
            metadata=client.cluster,
        )
        assert tm._current_state == TransactionState.UNINITIALIZED
        assert not tm.has_producer_id()

        # Step 1: enqueue the initial InitProducerIdHandler.
        tm.init_producer_id()
        assert tm._current_state == TransactionState.INITIALIZING

        broker.respond(InitProducerIdResponse, InitProducerIdResponse(
            throttle_time_ms=0,
            error_code=0,
            producer_id=_PRODUCER_ID,
            producer_epoch=_PRODUCER_EPOCH,
        ))
        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        # Init complete: producer_id set, state advanced to READY.
        assert tm._current_state == TransactionState.READY
        assert tm.producer_id_and_epoch.producer_id == _PRODUCER_ID
        assert tm.producer_id_and_epoch.epoch == _PRODUCER_EPOCH

        # Step 2: simulate the OUT_OF_ORDER_SEQUENCE_NUMBER recovery path.
        tm.bump_producer_id_and_epoch()
        assert tm.is_bumping_epoch()

        broker.respond(InitProducerIdResponse, InitProducerIdResponse(
            throttle_time_ms=0,
            error_code=0,
            producer_id=_PRODUCER_ID,
            producer_epoch=_PRODUCER_EPOCH + 1,
        ))
        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        # Bump complete: same producer_id, epoch incremented, READY again.
        assert tm._current_state == TransactionState.READY
        assert tm.producer_id_and_epoch.producer_id == _PRODUCER_ID
        assert tm.producer_id_and_epoch.epoch == _PRODUCER_EPOCH + 1


# ---------------------------------------------------------------------------
# AddPartitionsToTxnHandler
# ---------------------------------------------------------------------------


class TestAddPartitionsToTxnHandlerMockBroker:

    def _partition_result(self, topic, partition, error_code):
        TopicResult = AddPartitionsToTxnResponse.AddPartitionsToTxnTopicResult
        PartitionResult = TopicResult.AddPartitionsToTxnPartitionResult
        return TopicResult(
            name=topic,
            results_by_partition=[PartitionResult(
                partition_index=partition,
                partition_error_code=error_code,
            )],
        )

    def _response(self, topic, partition, error_code):
        return AddPartitionsToTxnResponse(
            throttle_time_ms=0,
            results_by_topic_v3_and_below=[
                self._partition_result(topic, partition, error_code)],
        )

    def _enqueue_add_partitions(self, tm, topic='foo', partition=0):
        tp = TopicPartition(topic, partition)
        tm._current_state = TransactionState.IN_TRANSACTION
        tm._transaction_started = True
        tm._new_partitions_in_transaction.add(tp)
        handler = tm._add_partitions_to_transaction_handler()
        tm._enqueue_request(handler)
        return handler, tp

    @pytest.mark.parametrize("error", [
        Errors.CoordinatorLoadInProgressError,
        Errors.UnknownTopicOrPartitionError,
        Errors.ConcurrentTransactionsError,
    ])
    def test_retriable_partition_error_reenqueues(self, broker, client, error):
        tm = _make_manager(client)
        handler, tp = self._enqueue_add_partitions(tm)

        broker.respond(AddPartitionsToTxnResponse,
                       self._response(tp.topic, tp.partition, error.errno))

        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        # Retriable: handler reenqueued, transaction not advanced, no error
        assert handler in _pending_handlers(tm)
        assert tp not in tm._partitions_in_transaction
        assert not tm.has_error()

    def test_concurrent_transactions_overrides_backoff(self, broker, client):
        tm = _make_manager(client)
        handler, tp = self._enqueue_add_partitions(tm)
        # No partitions yet added -> CONCURRENT_TRANSACTIONS should lower
        # the retry backoff to ADD_PARTITIONS_RETRY_BACKOFF_MS (20ms).
        assert not tm._partitions_in_transaction
        original_backoff = handler.retry_backoff_ms

        broker.respond(
            AddPartitionsToTxnResponse,
            self._response(tp.topic, tp.partition,
                           Errors.ConcurrentTransactionsError.errno))

        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        assert handler in _pending_handlers(tm)
        assert handler.retry_backoff_ms <= TransactionManager.ADD_PARTITIONS_RETRY_BACKOFF_MS
        assert handler.retry_backoff_ms < original_backoff

    def test_coordinator_error_invalidates_coordinator(self, broker, client):
        tm = _make_manager(client)
        handler, tp = self._enqueue_add_partitions(tm)

        broker.respond(AddPartitionsToTxnResponse,
                       self._response(tp.topic, tp.partition,
                                      Errors.NotCoordinatorError.errno))

        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        assert handler in _pending_handlers(tm)
        # A FindCoordinatorHandler is pushed ahead of the retry.
        assert any(isinstance(h, FindCoordinatorHandler)
                   for h in _pending_handlers(tm))
        assert tm._transaction_coordinator is None

    def test_success_marks_partition_added(self, broker, client):
        tm = _make_manager(client)
        handler, tp = self._enqueue_add_partitions(tm)

        broker.respond(AddPartitionsToTxnResponse,
                       self._response(tp.topic, tp.partition, 0))

        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        assert tp in tm._partitions_in_transaction
        assert handler not in _pending_handlers(tm)
        assert handler._result.is_done and not handler._result.failed

    @pytest.mark.parametrize("error,expected", [
        # Java client normalizes INVALID_PRODUCER_EPOCH and PRODUCER_FENCED to
        # PRODUCER_FENCED on the txn-coordinator RPC paths (KIP-360).
        (Errors.InvalidProducerEpochError, Errors.ProducerFencedError),
        (Errors.ProducerFencedError, Errors.ProducerFencedError),
        (Errors.TransactionalIdAuthorizationFailedError,
         Errors.TransactionalIdAuthorizationFailedError),
        (Errors.InvalidProducerIdMappingError, Errors.KafkaError),
        (Errors.InvalidTxnStateError, Errors.KafkaError),
    ])
    def test_fatal_partition_error(self, broker, client, error, expected):
        tm = _make_manager(client)
        handler, tp = self._enqueue_add_partitions(tm)

        broker.respond(AddPartitionsToTxnResponse,
                       self._response(tp.topic, tp.partition, error.errno))

        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        assert tm._current_state == TransactionState.FATAL_ERROR
        assert tm.has_fatal_error()
        assert isinstance(tm.last_error, expected)
        assert handler._result.failed
        assert handler not in _pending_handlers(tm)

    def test_topic_auth_failed_is_abortable(self, broker, client):
        tm = _make_manager(client)
        handler, tp = self._enqueue_add_partitions(tm)

        broker.respond(AddPartitionsToTxnResponse,
                       self._response(tp.topic, tp.partition,
                                      Errors.TopicAuthorizationFailedError.errno))

        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        # Abortable: application can abort the transaction and continue.
        assert tm._current_state == TransactionState.ABORTABLE_ERROR
        assert tm.has_abortable_error()
        assert isinstance(tm.last_error, Errors.TopicAuthorizationFailedError)
        # Handler reports the unauthorized topic via the error's args.
        assert tp.topic in tm.last_error.args[0]
        assert handler._result.failed

    def test_operation_not_attempted_is_abortable(self, broker, client):
        tm = _make_manager(client)
        handler, tp = self._enqueue_add_partitions(tm)

        broker.respond(AddPartitionsToTxnResponse,
                       self._response(tp.topic, tp.partition,
                                      Errors.OperationNotAttemptedError.errno))

        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        # has_partition_errors path -> abortable with a generic KafkaError
        assert tm._current_state == TransactionState.ABORTABLE_ERROR
        assert handler._result.failed


# ---------------------------------------------------------------------------
# FindCoordinatorHandler
# ---------------------------------------------------------------------------


class TestFindCoordinatorHandlerMockBroker:

    def _response(self, error_code=0, node_id=0, host='localhost', port=9092):
        return FindCoordinatorResponse(
            throttle_time_ms=0,
            error_code=error_code,
            error_message='',
            node_id=node_id,
            host=host,
            port=port,
            coordinators=[],
        )

    @pytest.mark.parametrize("error", [
        Errors.CoordinatorNotAvailableError,
        Errors.CoordinatorLoadInProgressError,
    ])
    def test_retriable_error_reenqueues(self, broker, client, error):
        # Fresh manager with no coordinator seeded.
        tm = _make_manager(client, seed_coord=False)
        handler = FindCoordinatorHandler(tm, 'transaction', _TXN_ID)
        tm._enqueue_request(handler)

        broker.respond(FindCoordinatorResponse,
                       self._response(error_code=error.errno,
                                      node_id=-1, host='', port=-1))

        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        assert handler in _pending_handlers(tm)
        assert tm._transaction_coordinator is None
        assert not tm.has_error()

    def test_success_populates_coordinator(self, broker, client):
        tm = _make_manager(client, seed_coord=False)
        handler = FindCoordinatorHandler(tm, 'transaction', _TXN_ID)
        tm._enqueue_request(handler)

        broker.respond(FindCoordinatorResponse,
                       self._response(error_code=0, node_id=0,
                                      host='localhost', port=9092))

        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        assert tm._transaction_coordinator is not None
        assert handler._result.is_done and not handler._result.failed

    def test_transactional_id_authz_failed_is_fatal(self, broker, client):
        tm = _make_manager(client, seed_coord=False)
        handler = FindCoordinatorHandler(tm, 'transaction', _TXN_ID)
        tm._enqueue_request(handler)

        broker.respond(FindCoordinatorResponse, self._response(
            error_code=Errors.TransactionalIdAuthorizationFailedError.errno,
            node_id=-1, host='', port=-1))

        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        assert tm._current_state == TransactionState.FATAL_ERROR
        assert handler._result.failed

    def test_group_authz_failed_is_abortable(self, broker, client):
        # Group-coordinator lookups happen inside an active transaction
        # (via send_offsets_to_transaction), so GROUP_AUTHORIZATION_FAILED
        # aborts the transaction rather than being fatal.
        tm = _make_manager(client, seed_coord=False)
        tm._current_state = TransactionState.IN_TRANSACTION
        handler = FindCoordinatorHandler(tm, 'group', 'my-group')
        tm._enqueue_request(handler)

        broker.respond(FindCoordinatorResponse, self._response(
            error_code=Errors.GroupAuthorizationFailedError.errno,
            node_id=-1, host='', port=-1))

        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        assert tm._current_state == TransactionState.ABORTABLE_ERROR
        assert isinstance(tm.last_error, Errors.GroupAuthorizationFailedError)
        assert handler._result.failed

    def test_unknown_error_is_fatal(self, broker, client):
        tm = _make_manager(client, seed_coord=False)
        handler = FindCoordinatorHandler(tm, 'transaction', _TXN_ID)
        tm._enqueue_request(handler)

        # InvalidRequestError is non-retriable and not in any named branch.
        broker.respond(FindCoordinatorResponse, self._response(
            error_code=Errors.InvalidRequestError.errno,
            node_id=-1, host='', port=-1))

        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        assert tm._current_state == TransactionState.FATAL_ERROR
        assert handler._result.failed


# ---------------------------------------------------------------------------
# EndTxnHandler
# ---------------------------------------------------------------------------


class TestEndTxnHandlerMockBroker:

    def _response(self, error_code=0):
        return EndTxnResponse(throttle_time_ms=0, error_code=error_code)

    def _enqueue_end_txn(self, tm, committed=True):
        tm._current_state = TransactionState.IN_TRANSACTION
        tm._transaction_started = True
        tm._partitions_in_transaction.add(TopicPartition('foo', 0))
        tm._transition_to(TransactionState.COMMITTING_TRANSACTION)
        handler = EndTxnHandler(tm, committed=committed)
        tm._enqueue_request(handler)
        return handler

    @pytest.mark.parametrize("error", [
        Errors.CoordinatorNotAvailableError,
        Errors.CoordinatorLoadInProgressError,
        Errors.NotCoordinatorError,
        Errors.ConcurrentTransactionsError,
    ])
    def test_retriable_error_reenqueues(self, broker, client, error):
        tm = _make_manager(client)
        handler = self._enqueue_end_txn(tm)

        broker.respond(EndTxnResponse, self._response(error_code=error.errno))

        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        assert handler in _pending_handlers(tm)
        # Transaction did not complete; we're still in COMMITTING_TRANSACTION.
        assert tm._current_state == TransactionState.COMMITTING_TRANSACTION
        assert not tm.has_error()

    def test_coordinator_error_invalidates_coordinator(self, broker, client):
        tm = _make_manager(client)
        self._enqueue_end_txn(tm)

        broker.respond(EndTxnResponse,
                       self._response(Errors.NotCoordinatorError.errno))

        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        # A FindCoordinatorHandler is enqueued, and the stored coordinator
        # is cleared.
        assert tm._transaction_coordinator is None
        assert any(isinstance(h, FindCoordinatorHandler)
                   for h in _pending_handlers(tm))

    def test_success_completes_transaction(self, broker, client):
        tm = _make_manager(client)
        handler = self._enqueue_end_txn(tm)

        broker.respond(EndTxnResponse, self._response(error_code=0))

        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        assert tm._current_state == TransactionState.READY
        assert not tm._transaction_started
        assert not tm._partitions_in_transaction
        assert handler._result.is_done and not handler._result.failed

    @pytest.mark.parametrize("error,expected", [
        # Java client normalizes INVALID_PRODUCER_EPOCH and PRODUCER_FENCED to
        # PRODUCER_FENCED on the txn-coordinator RPC paths (KIP-360).
        (Errors.InvalidProducerEpochError, Errors.ProducerFencedError),
        (Errors.ProducerFencedError, Errors.ProducerFencedError),
        (Errors.TransactionalIdAuthorizationFailedError,
         Errors.TransactionalIdAuthorizationFailedError),
        (Errors.InvalidTxnStateError, Errors.InvalidTxnStateError),
    ])
    def test_fatal_error(self, broker, client, error, expected):
        tm = _make_manager(client)
        handler = self._enqueue_end_txn(tm)

        broker.respond(EndTxnResponse, self._response(error_code=error.errno))

        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        assert tm._current_state == TransactionState.FATAL_ERROR
        assert tm.has_fatal_error()
        assert isinstance(tm.last_error, expected)
        assert handler._result.failed
        assert handler not in _pending_handlers(tm)

    def test_unknown_error_is_fatal(self, broker, client):
        tm = _make_manager(client)
        handler = self._enqueue_end_txn(tm)

        broker.respond(EndTxnResponse,
                       self._response(Errors.InvalidRequestError.errno))

        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        assert tm._current_state == TransactionState.FATAL_ERROR
        assert handler._result.failed


# ---------------------------------------------------------------------------
# AddOffsetsToTxnHandler
# ---------------------------------------------------------------------------


class TestAddOffsetsToTxnHandlerMockBroker:

    def _response(self, error_code=0):
        return AddOffsetsToTxnResponse(
            throttle_time_ms=0, error_code=error_code)

    def _enqueue_add_offsets(self, tm, group_id='my-group',
                             offsets=None):
        tm._current_state = TransactionState.IN_TRANSACTION
        if offsets is None:
            offsets = {
                TopicPartition('foo', 0):
                    OffsetAndMetadata(offset=10, metadata='', leader_epoch=-1),
            }
        handler = AddOffsetsToTxnHandler(tm, group_id, offsets)
        tm._enqueue_request(handler)
        return handler, group_id, offsets

    @pytest.mark.parametrize("error", [
        Errors.CoordinatorNotAvailableError,
        Errors.CoordinatorLoadInProgressError,
        Errors.NotCoordinatorError,
        Errors.ConcurrentTransactionsError,
    ])
    def test_retriable_error_reenqueues(self, broker, client, error):
        tm = _make_manager(client)
        handler, *_ = self._enqueue_add_offsets(tm)

        broker.respond(AddOffsetsToTxnResponse,
                       self._response(error_code=error.errno))

        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        assert handler in _pending_handlers(tm)
        assert not tm.has_error()

    def test_coordinator_error_invalidates_coordinator(self, broker, client):
        tm = _make_manager(client)
        self._enqueue_add_offsets(tm)

        broker.respond(AddOffsetsToTxnResponse,
                       self._response(Errors.NotCoordinatorError.errno))

        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        assert tm._transaction_coordinator is None
        assert any(isinstance(h, FindCoordinatorHandler)
                   for h in _pending_handlers(tm))

    def test_success_enqueues_offset_commit(self, broker, client):
        tm = _make_manager(client)
        # Also need the group coordinator for the follow-up TxnOffsetCommit.
        tm._consumer_group_coordinator = 0
        handler, group_id, offsets = self._enqueue_add_offsets(tm)

        broker.respond(AddOffsetsToTxnResponse, self._response(error_code=0))

        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        # A TxnOffsetCommitHandler should now be queued, sharing the same
        # result object as the AddOffsets handler.
        pending = _pending_handlers(tm)
        commit_handlers = [h for h in pending if isinstance(h, TxnOffsetCommitHandler)]
        assert len(commit_handlers) == 1
        assert commit_handlers[0]._result is handler._result
        # AddOffsets result is not done yet--it completes when the commit does.
        assert not handler._result.is_done
        assert tm._pending_txn_offset_commits == offsets

    @pytest.mark.parametrize("error,expected", [
        # Java client normalizes INVALID_PRODUCER_EPOCH and PRODUCER_FENCED to
        # PRODUCER_FENCED on the txn-coordinator RPC paths (KIP-360).
        (Errors.InvalidProducerEpochError, Errors.ProducerFencedError),
        (Errors.ProducerFencedError, Errors.ProducerFencedError),
        (Errors.TransactionalIdAuthorizationFailedError,
         Errors.TransactionalIdAuthorizationFailedError),
    ])
    def test_fatal_error(self, broker, client, error, expected):
        tm = _make_manager(client)
        handler, _, _ = self._enqueue_add_offsets(tm)

        broker.respond(AddOffsetsToTxnResponse,
                       self._response(error_code=error.errno))

        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        assert tm._current_state == TransactionState.FATAL_ERROR
        assert isinstance(tm.last_error, expected)
        assert handler._result.failed

    def test_group_authz_failed_is_abortable(self, broker, client):
        tm = _make_manager(client)
        handler, group_id, _ = self._enqueue_add_offsets(tm)

        broker.respond(AddOffsetsToTxnResponse, self._response(
            error_code=Errors.GroupAuthorizationFailedError.errno))

        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        assert tm._current_state == TransactionState.ABORTABLE_ERROR
        assert isinstance(tm.last_error, Errors.GroupAuthorizationFailedError)
        assert handler._result.failed

    def test_unknown_error_is_fatal(self, broker, client):
        tm = _make_manager(client)
        handler, _, _ = self._enqueue_add_offsets(tm)

        broker.respond(AddOffsetsToTxnResponse, self._response(
            error_code=Errors.InvalidRequestError.errno))

        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        assert tm._current_state == TransactionState.FATAL_ERROR
        assert handler._result.failed


# ---------------------------------------------------------------------------
# TxnOffsetCommitHandler
# ---------------------------------------------------------------------------


class TestTxnOffsetCommitHandlerMockBroker:

    def _response(self, topic_partition_errors):
        """Build a TxnOffsetCommitResponse from {(topic, partition): errno}."""
        Topic = TxnOffsetCommitResponse.TxnOffsetCommitResponseTopic
        Partition = Topic.TxnOffsetCommitResponsePartition
        by_topic = {}
        for (topic, partition), errno in topic_partition_errors.items():
            by_topic.setdefault(topic, []).append(
                Partition(partition_index=partition, error_code=errno))
        return TxnOffsetCommitResponse(
            throttle_time_ms=0,
            topics=[Topic(name=t, partitions=parts)
                    for t, parts in by_topic.items()],
        )

    def _enqueue_offset_commit(self, tm, group_id='my-group',
                               topic='foo', partition=0):
        tp = TopicPartition(topic, partition)
        offsets = {tp: OffsetAndMetadata(offset=10, metadata='', leader_epoch=-1)}
        tm._current_state = TransactionState.IN_TRANSACTION
        tm._consumer_group_coordinator = 0
        tm._pending_txn_offset_commits.update(offsets)
        from kafka.producer.transaction_manager import TransactionalRequestResult
        result = TransactionalRequestResult()
        handler = TxnOffsetCommitHandler(tm, group_id, offsets, result)
        tm._enqueue_request(handler)
        return handler, tp

    @pytest.mark.parametrize("error", [
        Errors.CoordinatorLoadInProgressError,
        Errors.CoordinatorNotAvailableError,
        Errors.NotCoordinatorError,
        Errors.RequestTimedOutError,
    ])
    def test_per_partition_retriable_reenqueues(self, broker, client, error):
        tm = _make_manager(client)
        handler, tp = self._enqueue_offset_commit(tm)

        broker.respond(
            TxnOffsetCommitResponse,
            self._response({(tp.topic, tp.partition): error.errno}))

        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        # Retriable per-partition error: handler reenqueued, result still
        # pending, pending_txn_offset_commits still populated.
        assert handler in _pending_handlers(tm)
        assert not handler._result.is_done
        assert tp in tm._pending_txn_offset_commits
        assert not tm.has_error()

    def test_request_timed_out_triggers_coordinator_lookup(self, broker, client):
        tm = _make_manager(client)
        handler, tp = self._enqueue_offset_commit(tm)

        broker.respond(
            TxnOffsetCommitResponse,
            self._response({(tp.topic, tp.partition):
                            Errors.RequestTimedOutError.errno}))

        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        # RequestTimedOutError in TxnOffsetCommit forces a group-coordinator
        # rediscovery in addition to the reenqueue.
        assert tm._consumer_group_coordinator is None
        assert any(isinstance(h, FindCoordinatorHandler)
                   for h in _pending_handlers(tm))

    def test_success_completes_result(self, broker, client):
        tm = _make_manager(client)
        handler, tp = self._enqueue_offset_commit(tm)

        broker.respond(
            TxnOffsetCommitResponse,
            self._response({(tp.topic, tp.partition): 0}))

        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        assert handler._result.is_done and not handler._result.failed
        assert tp not in tm._pending_txn_offset_commits

    @pytest.mark.parametrize("error,expected", [
        # Java client normalizes INVALID_PRODUCER_EPOCH and PRODUCER_FENCED to
        # PRODUCER_FENCED on the txn-coordinator RPC paths (KIP-360).
        (Errors.InvalidProducerEpochError, Errors.ProducerFencedError),
        (Errors.ProducerFencedError, Errors.ProducerFencedError),
        (Errors.TransactionalIdAuthorizationFailedError,
         Errors.TransactionalIdAuthorizationFailedError),
        (Errors.UnsupportedForMessageFormatError,
         Errors.UnsupportedForMessageFormatError),
    ])
    def test_fatal_partition_error(self, broker, client, error, expected):
        tm = _make_manager(client)
        handler, tp = self._enqueue_offset_commit(tm)

        broker.respond(
            TxnOffsetCommitResponse,
            self._response({(tp.topic, tp.partition): error.errno}))

        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        assert tm._current_state == TransactionState.FATAL_ERROR
        assert isinstance(tm.last_error, expected)
        assert handler._result.failed

    def test_group_authz_failed_is_abortable(self, broker, client):
        tm = _make_manager(client)
        handler, tp = self._enqueue_offset_commit(tm)

        broker.respond(
            TxnOffsetCommitResponse,
            self._response({(tp.topic, tp.partition):
                            Errors.GroupAuthorizationFailedError.errno}))

        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        assert tm._current_state == TransactionState.ABORTABLE_ERROR
        assert isinstance(tm.last_error, Errors.GroupAuthorizationFailedError)
        assert handler._result.failed

    def test_unknown_partition_error_is_fatal(self, broker, client):
        tm = _make_manager(client)
        handler, tp = self._enqueue_offset_commit(tm)

        broker.respond(
            TxnOffsetCommitResponse,
            self._response({(tp.topic, tp.partition):
                            Errors.InvalidRequestError.errno}))

        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        assert tm._current_state == TransactionState.FATAL_ERROR
        assert handler._result.failed

    def test_partial_retriable_retries_only_failed(self, broker, client):
        """If one partition succeeds and another is retriable, the retry
        request only contains the still-pending partition."""
        tm = _make_manager(client)
        tp_ok = TopicPartition('foo', 0)
        tp_retry = TopicPartition('bar', 1)
        offsets = {
            tp_ok: OffsetAndMetadata(offset=10, metadata='', leader_epoch=-1),
            tp_retry: OffsetAndMetadata(offset=20, metadata='', leader_epoch=-1),
        }
        tm._current_state = TransactionState.IN_TRANSACTION
        tm._consumer_group_coordinator = 0
        tm._pending_txn_offset_commits.update(offsets)
        from kafka.producer.transaction_manager import TransactionalRequestResult
        result = TransactionalRequestResult()
        handler = TxnOffsetCommitHandler(tm, 'my-group', offsets, result)
        tm._enqueue_request(handler)

        broker.respond(
            TxnOffsetCommitResponse,
            self._response({
                (tp_ok.topic, tp_ok.partition): 0,
                (tp_retry.topic, tp_retry.partition):
                    Errors.CoordinatorLoadInProgressError.errno,
            }))

        _, future = _dispatch_next(client, tm)
        _poll_for_future(client, future)

        assert handler in _pending_handlers(tm)
        assert tp_ok not in tm._pending_txn_offset_commits
        assert tp_retry in tm._pending_txn_offset_commits
        # Result not yet done--the retry has to complete first.
        assert not result.is_done


# ---------------------------------------------------------------------------
# Idempotent producer ordering on retry
# ---------------------------------------------------------------------------


def _decode_produce_base_sequences(request_bytes, api_version):
    """Decode a ProduceRequest and return [(topic, partition, base_sequence)] in order."""
    request = ProduceRequest.decode(request_bytes, version=api_version, header=True)
    out = []
    for topic_data in request.topic_data:
        for partition_data in topic_data.partition_data:
            records = MemoryRecords(bytes(partition_data.records))
            batch = records.next_batch()
            out.append((topic_data.name, partition_data.index, batch.base_sequence))
    return out


class TestIdempotentProducerOrderingMockBroker:
    """Regression test: idempotent producer must mute the partition after a
    retryable error so retried batches are not reordered against later batches.

    Without partition muting, when the first ProduceRequest gets a transient
    NotLeaderForPartition error, the sender re-enqueues that batch via
    deque.appendleft. Meanwhile, additional batches (queued by send() while
    the first was in flight) drain and go out with sequences ahead of the
    retried base_sequence. The broker rejects them with
    OutOfOrderSequenceNumberError. With muting, no further batch for the
    partition drains until the in-flight batch's response is processed, so
    the retry is sent before the next batch.
    """

    _TOPIC = 'tx-order'

    def _make_metadata_topic(self, version):
        Topic = MetadataResponse.MetadataResponseTopic
        Partition = Topic.MetadataResponsePartition
        return Topic(version=version, error_code=0, name=self._TOPIC,
                     is_internal=False,
                     partitions=[
                         Partition(version=version, error_code=0,
                                   partition_index=0, leader_id=0,
                                   leader_epoch=0,
                                   replica_nodes=[0], isr_nodes=[0],
                                   offline_replicas=[]),
                     ])

    def _produce_response(self, version, error_code, base_offset):
        Topic = ProduceResponse.TopicProduceResponse
        Partition = Topic.PartitionProduceResponse
        return ProduceResponse(
            throttle_time_ms=0,
            responses=[
                Topic(name=self._TOPIC, partition_responses=[
                    Partition(index=0, error_code=error_code,
                              base_offset=base_offset, log_append_time_ms=-1,
                              log_start_offset=0, record_errors=[],
                              error_message=None, current_leader=None),
                ]),
            ],
        )

    def test_retry_does_not_reorder_against_later_batches(self):
        """First ProduceRequest fails with NotLeader; assert that the very
        next ProduceRequest the broker sees is the retry (same base_sequence),
        not a later batch that drained while the first was in flight."""
        broker = MockBroker(broker_version=(2, 5))
        broker.set_metadata(topics=[self._make_metadata_topic(version=8)])
        # Producer's auto-version negotiation will land on (2, 5).
        broker.respond(InitProducerIdResponse, InitProducerIdResponse(
            throttle_time_ms=0, error_code=0,
            producer_id=42, producer_epoch=0,
        ))

        # Capture each ProduceRequest's first-partition base_sequence as it
        # arrives. First call: hold the response (via an awaitable Future)
        # until enough subsequent requests have arrived to demonstrate the
        # in-flight window contains multiple batches; then return NotLeader
        # (transient retryable). All subsequent calls: success.
        #
        # The hold is what surfaces the bug — without partition muting, the
        # sender drains additional batches while the first is still in flight,
        # and the first batch's retry (reenqueued via appendleft) is
        # sequenced *after* them.
        from kafka.future import Future
        received_sequences = []
        call_count = [0]
        release_first = Future()

        async def _held_notleader_response(api_version):
            # Awaiting a kafka.future.Future yields until success/failure is
            # set. While we're parked here, the broker's IO loop is free to
            # process other queued ProduceRequests (each write() schedules its
            # own _process_requests).
            await release_first
            return self._produce_response(
                version=api_version,
                error_code=Errors.NotLeaderForPartitionError.errno,
                base_offset=-1)

        def produce_response(api_key, api_version, correlation_id, request_bytes):
            seqs = _decode_produce_base_sequences(request_bytes, api_version)
            assert seqs, 'ProduceRequest had no partition data'
            received_sequences.append(seqs[0][2])
            call_count[0] += 1
            if call_count[0] == 1:
                # Return a coroutine; handle_request will await it.
                return _held_notleader_response(api_version)
            # Once a couple more requests have arrived, release the held one
            # so the producer sees the NotLeader and reenqueues. With muting,
            # call_count won't reach 3 (only one batch in flight at a time)
            # and we'd hit the safety release in the test body below.
            if call_count[0] >= 3 and not release_first.is_done:
                release_first.success(None)
            return self._produce_response(
                version=api_version, error_code=0, base_offset=0)

        # Register a respond_fn that handles every ProduceRequest the test
        # sends. The MockBroker pops one queue entry per request, so we need
        # one respond_fn per expected request. We don't know ahead of time
        # how many will be sent, so register a generous batch.
        for _ in range(64):
            broker.respond_fn(ProduceRequest, produce_response)

        producer = KafkaProducer(
            kafka_client=broker.client_factory(),
            bootstrap_servers=['%s:%d' % (broker.host, broker.port)],
            api_version=(2, 5),
            enable_idempotence=True,
            max_in_flight_requests_per_connection=5,
            batch_size=64,        # tiny so multiple batches form quickly
            linger_ms=5,
            retry_backoff_ms=10,
            request_timeout_ms=5000,
        )
        # Safety release: if muting *is* working, call_count never reaches 3
        # (only one ProduceRequest in flight at a time), so the held NotLeader
        # response would never fire on its own. Time-bound release after a
        # short delay to keep the test fast in either case.
        import threading
        threading.Timer(0.1, lambda: (
            release_first.success(None) if not release_first.is_done else None
        )).start()
        try:
            futures = [
                producer.send(self._TOPIC, value=('msg-%02d' % i).encode(),
                              partition=0)
                for i in range(20)
            ]
            for f in futures:
                f.get(timeout=10)
        finally:
            producer.close(timeout=2)

        # The first ProduceRequest had base_sequence 0 and was rejected with
        # NotLeader. With partition muting (the fix), the second
        # ProduceRequest the broker sees must be the *retry* of that batch
        # — same base_sequence. Without muting, a later batch with a higher
        # base_sequence would have drained while the first was in flight,
        # arriving here ahead of the retry.
        assert len(received_sequences) >= 2, (
            'expected at least 2 ProduceRequests, got %r' % received_sequences)
        assert received_sequences[0] == 0, (
            'first ProduceRequest should carry base_sequence 0; got %r'
            % received_sequences)
        assert received_sequences[1] == 0, (
            'second ProduceRequest must be the retry of base_sequence 0; '
            'got %r — partition was not muted, later batch drained ahead of '
            'retry' % received_sequences)
