"""Config resolution tests for KIP-679 (producer enables strongest delivery
guarantee by default).

This module covers the explicit-conflict and broker-version-fallback paths in
``KafkaProducer.__init__``. Default-driven silent-disable cases are added in
the same module once the defaults flip; until then the conflict-resolution
machinery is exercised with ``enable_idempotence=True`` set explicitly.
"""
import logging

import pytest

from kafka import KafkaProducer
import kafka.errors as Errors


class TestExplicitIdempotenceConflicts:
    """User explicitly set enable_idempotence=True -> conflicts must raise."""

    def test_acks_1_conflicts(self):
        with pytest.raises(Errors.KafkaConfigurationError, match="acks=1"):
            KafkaProducer(
                enable_idempotence=True,
                acks=1,
                api_version=(0, 11),
            )

    def test_acks_0_conflicts(self):
        with pytest.raises(Errors.KafkaConfigurationError, match="acks=0"):
            KafkaProducer(
                enable_idempotence=True,
                acks=0,
                api_version=(0, 11),
            )

    def test_retries_zero_conflicts(self):
        with pytest.raises(Errors.KafkaConfigurationError, match="retries=0"):
            KafkaProducer(
                enable_idempotence=True,
                retries=0,
                api_version=(0, 11),
            )

    def test_max_in_flight_too_high_conflicts(self):
        with pytest.raises(Errors.KafkaConfigurationError,
                           match="max_in_flight_requests_per_connection=10"):
            KafkaProducer(
                enable_idempotence=True,
                max_in_flight_requests_per_connection=10,
                api_version=(0, 11),
            )

    def test_multiple_conflicts_named_together(self):
        with pytest.raises(Errors.KafkaConfigurationError) as excinfo:
            KafkaProducer(
                enable_idempotence=True,
                acks=1,
                retries=0,
                api_version=(0, 11),
            )
        msg = str(excinfo.value)
        assert "acks=1" in msg
        assert "retries=0" in msg

    def test_acks_all_string_accepted(self):
        """'all' normalizes to -1 before the conflict check fires."""
        p = KafkaProducer(
            enable_idempotence=True,
            acks='all',
            api_version=(0, 11),
        )
        assert p.config['acks'] == -1
        assert p._transaction_manager is not None
        p.close(timeout=0)

    def test_old_broker_with_explicit_idempotence_raises(self):
        with pytest.raises(Errors.KafkaConfigurationError, match="api_version"):
            KafkaProducer(
                enable_idempotence=True,
                api_version=(0, 10),
            )


class TestTransactionalIdempotenceStrict:
    """Transactional path is strict: even default-driven conflicts must raise."""

    def test_transactional_id_with_acks_1_raises(self):
        with pytest.raises(Errors.KafkaConfigurationError, match="acks=1"):
            KafkaProducer(
                transactional_id='tx-1',
                acks=1,
                api_version=(0, 11),
            )

    def test_transactional_id_with_retries_zero_raises(self):
        with pytest.raises(Errors.KafkaConfigurationError, match="retries=0"):
            KafkaProducer(
                transactional_id='tx-1',
                retries=0,
                api_version=(0, 11),
            )

    def test_transactional_id_on_old_broker_raises(self):
        with pytest.raises(Errors.KafkaConfigurationError, match="api_version"):
            KafkaProducer(
                transactional_id='tx-1',
                api_version=(0, 10),
            )

    def test_transactional_id_with_disabled_idempotence_raises(self):
        with pytest.raises(Errors.KafkaConfigurationError,
                           match="transactional_id without enable_idempotence"):
            KafkaProducer(
                transactional_id='tx-1',
                enable_idempotence=False,
                api_version=(0, 11),
            )
