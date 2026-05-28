"""Config resolution tests for KIP-679 (producer enables strongest delivery
guarantee by default)."""
import pytest

from kafka import KafkaProducer
import kafka.errors as Errors


# Log-content assertions are deliberately absent. KafkaProducer.close(null_logger=True)
# globally swaps the module-level `log` in kafka.producer.kafka for a NullLogger
# to silence atexit noise, and that mutation persists across producers within the
# same process. Observable behavior on the producer instance (enable_idempotence
# flag, _transaction_manager presence, config['acks']) is what callers actually
# depend on, and is what these tests verify.


class TestDefaultsAreIdempotent:
    """KIP-679: defaults instantiate an idempotent producer with acks=all."""

    def test_bare_construct_is_idempotent(self):
        p = KafkaProducer(api_version=(0, 11))
        try:
            assert p.config['enable_idempotence'] is True
            assert p.config['acks'] == -1
            assert p._transaction_manager is not None
            assert p._transaction_manager.is_transactional() is False
        finally:
            p.close(timeout=0)

    def test_opt_out_disables_idempotence_but_keeps_acks_default(self):
        p = KafkaProducer(enable_idempotence=False, api_version=(0, 11))
        try:
            assert p.config['enable_idempotence'] is False
            assert p.config['acks'] == -1
            assert p._transaction_manager is None
        finally:
            p.close(timeout=0)


class TestDefaultIdempotenceSilentDisable:
    """User-provided incompatible config + default idempotence -> silent disable."""

    def test_acks_1_silently_disables(self):
        p = KafkaProducer(acks=1, api_version=(0, 11))
        try:
            assert p.config['enable_idempotence'] is False
            assert p.config['acks'] == 1
            assert p._transaction_manager is None
        finally:
            p.close(timeout=0)

    def test_acks_0_silently_disables(self):
        p = KafkaProducer(acks=0, api_version=(0, 11))
        try:
            assert p.config['enable_idempotence'] is False
            assert p._transaction_manager is None
        finally:
            p.close(timeout=0)

    def test_retries_zero_silently_disables(self):
        p = KafkaProducer(retries=0, api_version=(0, 11))
        try:
            assert p.config['enable_idempotence'] is False
            assert p._transaction_manager is None
        finally:
            p.close(timeout=0)

    def test_max_in_flight_too_high_silently_disables(self):
        p = KafkaProducer(max_in_flight_requests_per_connection=10, api_version=(0, 11))
        try:
            assert p.config['enable_idempotence'] is False
            assert p._transaction_manager is None
        finally:
            p.close(timeout=0)


class TestOldBrokerFallback:
    """Defaults against a pre-0.11 broker: idempotence silently disabled,
    acks=-1 retained (acks=all is valid on any wire version)."""

    def test_old_broker_silently_disables(self):
        p = KafkaProducer(api_version=(0, 10))
        try:
            assert p.config['enable_idempotence'] is False
            # Per KIP-679 + project decision: acks stays at -1 even on old brokers.
            assert p.config['acks'] == -1
            assert p._transaction_manager is None
        finally:
            p.close(timeout=0)


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
        try:
            assert p.config['acks'] == -1
            assert p._transaction_manager is not None
        finally:
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
