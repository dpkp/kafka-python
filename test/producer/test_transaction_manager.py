# pylint: skip-file

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
        metadata=ClusterMetadata(),
    )


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
