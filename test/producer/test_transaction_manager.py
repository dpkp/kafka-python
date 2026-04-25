# pylint: skip-file
from unittest.mock import MagicMock

import pytest

import kafka.errors as Errors
from kafka.cluster import ClusterMetadata
from kafka.producer.transaction_manager import (
    TransactionManager,
    TransactionState,
)
from kafka.structs import TopicPartition


class _FakeBatch:
    """Minimal ProducerBatch stand-in for classifier tests.

    The classifier only reads `topic_partition`; nothing else.
    """
    def __init__(self, topic='foo', partition=0):
        self.topic_partition = TopicPartition(topic, partition)


def _make_manager(transactional_id=None, api_version=(2, 5)):
    return TransactionManager(
        transactional_id=transactional_id,
        transaction_timeout_ms=60000,
        retry_backoff_ms=100,
        api_version=api_version,
        metadata=ClusterMetadata(MagicMock()),
    )


def _set_producer_id(tm, producer_id=1234, epoch=5):
    """Give the manager a valid producer_id/epoch without going through the
    full initialize_transactions handshake."""
    from kafka.producer.transaction_manager import ProducerIdAndEpoch
    tm.set_producer_id_and_epoch(ProducerIdAndEpoch(producer_id, epoch))
    # Drive to READY from UNINITIALIZED via the (fake) init path.
    tm._current_state = TransactionState.READY


class TestBumpingProducerEpochState:
    def test_state_value(self):
        assert TransactionState.BUMPING_PRODUCER_EPOCH == 8

    @pytest.mark.parametrize("source", [
        TransactionState.READY,
        TransactionState.IN_TRANSACTION,
        TransactionState.COMMITTING_TRANSACTION,
        TransactionState.ABORTING_TRANSACTION,
        TransactionState.ABORTABLE_ERROR,
    ])
    def test_valid_entry_states(self, source):
        assert TransactionState.is_transition_valid(
            source, TransactionState.BUMPING_PRODUCER_EPOCH
        )

    @pytest.mark.parametrize("source", [
        TransactionState.UNINITIALIZED,
        TransactionState.INITIALIZING,
        TransactionState.FATAL_ERROR,
    ])
    def test_invalid_entry_states(self, source):
        assert not TransactionState.is_transition_valid(
            source, TransactionState.BUMPING_PRODUCER_EPOCH
        )

    @pytest.mark.parametrize("target", [
        TransactionState.READY,
        TransactionState.INITIALIZING,
        TransactionState.ABORTABLE_ERROR,
        TransactionState.FATAL_ERROR,
    ])
    def test_valid_exit_targets(self, target):
        assert TransactionState.is_transition_valid(
            TransactionState.BUMPING_PRODUCER_EPOCH, target
        )

    def test_cannot_transition_bumping_to_in_transaction_directly(self):
        # Must go through READY first
        assert not TransactionState.is_transition_valid(
            TransactionState.BUMPING_PRODUCER_EPOCH, TransactionState.IN_TRANSACTION
        )

    def test_is_bumping_epoch_helper(self):
        tm = _make_manager()
        assert not tm.is_bumping_epoch()
        tm._current_state = TransactionState.BUMPING_PRODUCER_EPOCH
        assert tm.is_bumping_epoch()
        tm._current_state = TransactionState.READY
        assert not tm.is_bumping_epoch()


class TestClassifyBatchError:
    def test_fatal_errors_always_fatal(self):
        tm = _make_manager(api_version=(2, 5))
        batch = _FakeBatch()
        for err in (
            Errors.ClusterAuthorizationFailedError,
            Errors.TransactionalIdAuthorizationFailedError,
            Errors.ProducerFencedError,
            Errors.InvalidTxnStateError,
        ):
            assert tm.classify_batch_error(err, batch) == TransactionManager.ERROR_CLASS_FATAL

    def test_epoch_bump_errors_on_modern_broker(self):
        tm = _make_manager(api_version=(2, 5))
        batch = _FakeBatch()
        for err in (
            Errors.OutOfOrderSequenceNumberError,
            Errors.UnknownProducerIdError,
            Errors.InvalidProducerEpochError,
        ):
            assert tm.classify_batch_error(err, batch) == TransactionManager.ERROR_CLASS_NEEDS_EPOCH_BUMP

    def test_epoch_bump_errors_on_old_broker_idempotent(self):
        tm = _make_manager(transactional_id=None, api_version=(2, 0))
        batch = _FakeBatch()
        for err in (
            Errors.OutOfOrderSequenceNumberError,
            Errors.UnknownProducerIdError,
            Errors.InvalidProducerEpochError,
        ):
            assert tm.classify_batch_error(err, batch) == TransactionManager.ERROR_CLASS_NEEDS_PRODUCER_ID_RESET

    def test_epoch_bump_errors_on_old_broker_transactional(self):
        tm = _make_manager(transactional_id='txn-id', api_version=(2, 0))
        batch = _FakeBatch()
        for err in (
            Errors.OutOfOrderSequenceNumberError,
            Errors.UnknownProducerIdError,
            Errors.InvalidProducerEpochError,
        ):
            assert tm.classify_batch_error(err, batch) == TransactionManager.ERROR_CLASS_FATAL

    def test_retention_based_unknown_producer_id_is_retriable(self):
        tm = _make_manager(api_version=(2, 5))
        batch = _FakeBatch()
        # Simulate successful writes: last_acked_offset = 9
        tm.update_last_acked_offset(batch.topic_partition, 0, 10)
        # Broker reports log_start_offset strictly past our last_acked_offset
        # -> records aged out, safe to retry (with sequence reset)
        result = tm.classify_batch_error(
            Errors.UnknownProducerIdError, batch, log_start_offset=100
        )
        assert result == TransactionManager.ERROR_CLASS_RETRIABLE

    def test_real_data_loss_unknown_producer_id_still_needs_bump(self):
        tm = _make_manager(api_version=(2, 5))
        batch = _FakeBatch()
        tm.update_last_acked_offset(batch.topic_partition, 0, 100)  # last_acked = 99
        # log_start_offset within our acked range -> real loss, not retention
        result = tm.classify_batch_error(
            Errors.UnknownProducerIdError, batch, log_start_offset=50
        )
        assert result == TransactionManager.ERROR_CLASS_NEEDS_EPOCH_BUMP

    def test_unknown_producer_id_without_log_start_offset_needs_bump(self):
        tm = _make_manager(api_version=(2, 5))
        batch = _FakeBatch()
        tm.update_last_acked_offset(batch.topic_partition, 0, 5)
        # Old broker (log_start_offset = -1) -> no retention escape hatch
        result = tm.classify_batch_error(
            Errors.UnknownProducerIdError, batch, log_start_offset=-1
        )
        assert result == TransactionManager.ERROR_CLASS_NEEDS_EPOCH_BUMP

    def test_retriable_client_error(self):
        tm = _make_manager(api_version=(2, 5))
        batch = _FakeBatch()
        assert tm.classify_batch_error(Errors.KafkaConnectionError, batch) == TransactionManager.ERROR_CLASS_RETRIABLE

    def test_retriable_broker_error(self):
        tm = _make_manager(api_version=(2, 5))
        batch = _FakeBatch()
        assert tm.classify_batch_error(Errors.NotLeaderForPartitionError, batch) == TransactionManager.ERROR_CLASS_RETRIABLE

    def test_non_retriable_non_fatal_idempotent(self):
        tm = _make_manager(transactional_id=None, api_version=(2, 5))
        batch = _FakeBatch()
        # InvalidTopicError is non-retriable, not in FATAL_ERRORS, not in epoch-bump set
        assert tm.classify_batch_error(Errors.InvalidTopicError, batch) == TransactionManager.ERROR_CLASS_FATAL

    def test_non_retriable_non_fatal_transactional(self):
        tm = _make_manager(transactional_id='txn-id', api_version=(2, 5))
        batch = _FakeBatch()
        # Transactional producers can abort the current transaction and retry
        assert tm.classify_batch_error(Errors.InvalidTopicError, batch) == TransactionManager.ERROR_CLASS_ABORTABLE

    def test_classifier_accepts_exception_instance(self):
        tm = _make_manager(api_version=(2, 5))
        batch = _FakeBatch()
        # Passing an instance rather than a class should work identically
        exc = Errors.OutOfOrderSequenceNumberError("some message")
        assert tm.classify_batch_error(exc, batch) == TransactionManager.ERROR_CLASS_NEEDS_EPOCH_BUMP

    def test_supports_epoch_bump_version_gate(self):
        assert not _make_manager(api_version=(0, 11))._supports_epoch_bump()
        assert not _make_manager(api_version=(2, 0))._supports_epoch_bump()
        assert not _make_manager(api_version=(2, 4))._supports_epoch_bump()
        assert _make_manager(api_version=(2, 5))._supports_epoch_bump()
        assert _make_manager(api_version=(3, 0))._supports_epoch_bump()


class TestBumpProducerIdAndEpoch:
    def test_transitions_to_bumping_state(self):
        tm = _make_manager(api_version=(2, 5))
        _set_producer_id(tm)
        tp = TopicPartition('foo', 0)
        # Populate some partition state so we can assert it gets cleared.
        tm._sequence_numbers[tp] = 7
        tm.update_last_acked_offset(tp, 0, 5)
        assert tm.sequence_number(tp) == 7
        assert tm.last_acked_offset(tp) == 4

        tm.bump_producer_id_and_epoch()

        assert tm._current_state == TransactionState.BUMPING_PRODUCER_EPOCH
        assert tm.is_bumping_epoch()
        # Per-partition sequence state cleared
        assert tm.sequence_number(tp) == 0
        assert tm.last_acked_offset(tp) == -1

    def test_enqueues_init_producer_id_request_v3_with_bump_flag(self):
        tm = _make_manager(api_version=(2, 5))
        _set_producer_id(tm, producer_id=42, epoch=7)

        tm.bump_producer_id_and_epoch()

        # There should be exactly one pending request -- an InitProducerIdHandler
        # in epoch-bump mode carrying the current producer_id/epoch.
        from kafka.producer.transaction_manager import InitProducerIdHandler
        assert len(tm._pending_requests) == 1
        _, _, handler = tm._pending_requests[0]
        assert isinstance(handler, InitProducerIdHandler)
        assert handler._is_epoch_bump is True
        # InitProducerIdRequest v3 has producer_id + producer_epoch fields
        assert handler.request.producer_id == 42
        assert handler.request.producer_epoch == 7

    def test_is_idempotent_across_concurrent_calls(self):
        tm = _make_manager(api_version=(2, 5))
        _set_producer_id(tm)

        tm.bump_producer_id_and_epoch()
        # Second call from a second in-flight batch failure should be a no-op
        tm.bump_producer_id_and_epoch()
        tm.bump_producer_id_and_epoch()

        # Only ONE InitProducerIdHandler should have been enqueued
        from kafka.producer.transaction_manager import InitProducerIdHandler
        init_handlers = [h for _, _, h in tm._pending_requests
                         if isinstance(h, InitProducerIdHandler)]
        assert len(init_handlers) == 1

    def test_does_nothing_in_fatal_state(self):
        tm = _make_manager(api_version=(2, 5))
        _set_producer_id(tm)
        tm._current_state = TransactionState.FATAL_ERROR

        tm.bump_producer_id_and_epoch()

        # Should not transition to BUMPING_PRODUCER_EPOCH from FATAL
        assert tm._current_state == TransactionState.FATAL_ERROR
        assert not tm.is_bumping_epoch()

    def test_raises_on_old_broker(self):
        tm = _make_manager(api_version=(2, 0))
        _set_producer_id(tm)
        with pytest.raises(Errors.IllegalStateError):
            tm.bump_producer_id_and_epoch()

    def test_transactional_producer_clears_partition_state(self):
        tm = _make_manager(transactional_id='txn-id', api_version=(2, 5))
        _set_producer_id(tm)
        # Simulate an in-progress transaction with some partitions
        tp1 = TopicPartition('foo', 0)
        tp2 = TopicPartition('bar', 1)
        tm._transaction_started = True
        tm._partitions_in_transaction.add(tp1)
        tm._new_partitions_in_transaction.add(tp2)

        tm.bump_producer_id_and_epoch()

        # All transactional partition tracking should be cleared -- the
        # broker aborts the transaction as part of processing the bump.
        assert tm._transaction_started is False
        assert tm._partitions_in_transaction == set()
        assert tm._new_partitions_in_transaction == set()
        assert tm._pending_partitions_in_transaction == set()


class TestCompleteEpochBump:
    def test_transitions_to_ready(self):
        tm = _make_manager(api_version=(2, 5))
        _set_producer_id(tm)
        tm.bump_producer_id_and_epoch()
        assert tm.is_bumping_epoch()

        # Simulate the handler's success path
        tm._complete_epoch_bump()

        assert tm._current_state == TransactionState.READY
        assert tm._last_error is None


class TestInitProducerIdHandlerV3:
    def _make_handler(self, api_version=(2, 5), transactional_id=None, is_epoch_bump=False, producer_id=42, epoch=7):
        from kafka.producer.transaction_manager import InitProducerIdHandler, ProducerIdAndEpoch
        tm = _make_manager(transactional_id=transactional_id, api_version=api_version)
        tm.set_producer_id_and_epoch(ProducerIdAndEpoch(producer_id, epoch))
        tm._current_state = TransactionState.READY
        if is_epoch_bump:
            # The bump helper does the state transition and enqueue for us;
            # pull the enqueued handler back out for inspection.
            tm.bump_producer_id_and_epoch()
            _, _, handler = tm._pending_requests[0]
        else:
            handler = InitProducerIdHandler(tm, transaction_timeout_ms=1000)
        return tm, handler

    def _fake_init_producer_id_response(self, error_code=0, producer_id=42, producer_epoch=1):
        """Construct a minimal InitProducerIdResponse for handler tests."""
        from kafka.protocol.producer import InitProducerIdResponse
        # Use v3 which matches our target KIP-360 broker version.
        return InitProducerIdResponse[3](
            throttle_time_ms=0,
            error_code=error_code,
            producer_id=producer_id,
            producer_epoch=producer_epoch,
        )

    def test_v3_version_on_modern_broker(self):
        _, handler = self._make_handler(api_version=(2, 5))
        assert handler.request.version == 3

    def test_v2_version_on_2_4_broker(self):
        _, handler = self._make_handler(api_version=(2, 4))
        assert handler.request.version == 2

    def test_v1_version_on_2_0_broker(self):
        _, handler = self._make_handler(api_version=(2, 0))
        assert handler.request.version == 1

    def test_v0_version_on_old_broker(self):
        _, handler = self._make_handler(api_version=(0, 11))
        assert handler.request.version == 0

    def test_non_bump_request_has_no_producer_id(self):
        _, handler = self._make_handler(api_version=(2, 5), is_epoch_bump=False)
        # v3 request fields default to NO_PRODUCER_ID / NO_PRODUCER_EPOCH
        assert handler.request.producer_id == -1
        assert handler.request.producer_epoch == -1
        assert handler._is_epoch_bump is False

    def test_bump_request_carries_current_producer_id_and_epoch(self):
        _, handler = self._make_handler(
            api_version=(2, 5), is_epoch_bump=True, producer_id=42, epoch=7)
        assert handler.request.producer_id == 42
        assert handler.request.producer_epoch == 7
        assert handler._is_epoch_bump is True

    def test_successful_bump_transitions_to_ready(self):
        tm, handler = self._make_handler(
            api_version=(2, 5), is_epoch_bump=True, producer_id=42, epoch=7)
        assert tm.is_bumping_epoch()

        # Simulate a NoError response with the bumped epoch
        response = self._fake_init_producer_id_response(
            error_code=0, producer_id=42, producer_epoch=8)
        handler.handle_response(response)

        assert tm._current_state == TransactionState.READY
        assert tm.producer_id_and_epoch.producer_id == 42
        assert tm.producer_id_and_epoch.epoch == 8
        assert handler._result.is_done

    def test_successful_non_bump_transitions_to_ready(self):
        tm, handler = self._make_handler(
            api_version=(2, 5), is_epoch_bump=False)
        tm._current_state = TransactionState.INITIALIZING  # fresh init

        response = self._fake_init_producer_id_response(
            error_code=0, producer_id=100, producer_epoch=0)
        handler.handle_response(response)

        assert tm._current_state == TransactionState.READY
        assert tm.producer_id_and_epoch.producer_id == 100

    def test_bump_invalid_epoch_falls_back_to_fresh_init(self):
        tm, handler = self._make_handler(
            api_version=(2, 5), is_epoch_bump=True, producer_id=42, epoch=7)
        assert tm.is_bumping_epoch()
        # Simulate the normal dequeue-then-dispatch flow: the bump handler
        # has been pulled out of the queue by the sender before
        # handle_response fires.
        import heapq
        heapq.heappop(tm._pending_requests)

        response = self._fake_init_producer_id_response(
            error_code=Errors.InvalidProducerEpochError.errno,
            producer_id=-1, producer_epoch=-1)
        handler.handle_response(response)

        # Stay in BUMPING_PRODUCER_EPOCH with producer_id reset to NO_PRODUCER_ID
        assert tm._current_state == TransactionState.BUMPING_PRODUCER_EPOCH
        assert tm.producer_id_and_epoch.producer_id == -1
        assert tm.producer_id_and_epoch.epoch == -1
        # A fresh (non-bump) InitProducerIdHandler should be enqueued as the
        # fallback.
        from kafka.producer.transaction_manager import InitProducerIdHandler
        fallback_handlers = [h for _, _, h in tm._pending_requests
                             if isinstance(h, InitProducerIdHandler)]
        assert len(fallback_handlers) == 1
        assert fallback_handlers[0]._is_epoch_bump is False

    def test_non_bump_invalid_epoch_is_fatal(self):
        tm, handler = self._make_handler(api_version=(2, 5), is_epoch_bump=False)
        tm._current_state = TransactionState.INITIALIZING

        response = self._fake_init_producer_id_response(
            error_code=Errors.InvalidProducerEpochError.errno,
            producer_id=-1, producer_epoch=-1)
        handler.handle_response(response)

        # Non-bump path treats INVALID_PRODUCER_EPOCH as fatal
        assert tm._current_state == TransactionState.FATAL_ERROR

    def test_producer_fenced_is_fatal(self):
        tm, handler = self._make_handler(
            api_version=(2, 5), transactional_id='txn', is_epoch_bump=True)

        response = self._fake_init_producer_id_response(
            error_code=Errors.ProducerFencedError.errno,
            producer_id=-1, producer_epoch=-1)
        handler.handle_response(response)

        assert tm._current_state == TransactionState.FATAL_ERROR
