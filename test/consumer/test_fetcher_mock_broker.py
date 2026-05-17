# pylint: skip-file
"""Wire-level integration tests for KIP-320 offset validation.

These tests drive the Fetcher through a real KafkaConnectionManager wired
to a scriptable MockBroker, so every ``OffsetForLeaderEpochRequest`` and
``FetchRequest`` actually goes through the protocol layer. Use these to
verify end-to-end behavior that the pure-mock tests in
``test_fetcher.py`` can't cover - request encoding, response decoding,
and the interactions between Fetcher, ClusterMetadata, and SubscriptionState
as metadata updates arrive over the wire.
"""

import time

import pytest

import kafka.errors as Errors
from kafka.consumer.fetcher import Fetcher
from kafka.consumer.subscription_state import SubscriptionState
from kafka.protocol.consumer import (
    FetchRequest, FetchResponse,
    OffsetForLeaderEpochRequest, OffsetForLeaderEpochResponse,
)
from kafka.protocol.metadata import MetadataResponse
from kafka.structs import OffsetAndMetadata, TopicPartition


TOPIC = 'foobar'
PARTITION = 0


# --------------------------------------------------------------------------- #
# Builders                                                                    #
# --------------------------------------------------------------------------- #

_MetaTopic = MetadataResponse.MetadataResponseTopic
_MetaPartition = _MetaTopic.MetadataResponsePartition
_MetaBroker = MetadataResponse.MetadataResponseBroker
_OFLE_Topic = OffsetForLeaderEpochResponse.OffsetForLeaderTopicResult
_OFLE_Partition = _OFLE_Topic.EpochEndOffset
_FetchTopic = FetchResponse.FetchableTopicResponse
_FetchPartition = _FetchTopic.PartitionData


def _broker_metadata(broker, leader_epoch):
    """Configure the MockBroker to advertise TOPIC/PARTITION with the
    given leader_epoch, leader=node 0 (self)."""
    broker.set_metadata(
        brokers=[_MetaBroker(node_id=broker.node_id, host=broker.host,
                             port=broker.port, rack=None)],
        topics=[_MetaTopic(
            error_code=0, name=TOPIC, is_internal=False,
            partitions=[_MetaPartition(
                error_code=0, partition_index=PARTITION,
                leader_id=broker.node_id, leader_epoch=leader_epoch,
                replica_nodes=[broker.node_id], isr_nodes=[broker.node_id],
                offline_replicas=[])],
        )],
    )


def _ofle_response(error_code, leader_epoch, end_offset):
    return OffsetForLeaderEpochResponse(
        throttle_time_ms=0,
        topics=[_OFLE_Topic(topic=TOPIC, partitions=[_OFLE_Partition(
            error_code=error_code, partition=PARTITION,
            leader_epoch=leader_epoch, end_offset=end_offset)])])


def _fetch_response_with_error(errno):
    """FetchResponse for TOPIC/PARTITION carrying a single error code."""
    return FetchResponse(
        throttle_time_ms=0, error_code=0, session_id=0,
        responses=[_FetchTopic(topic=TOPIC, partitions=[_FetchPartition(
            partition_index=PARTITION, error_code=errno, high_watermark=-1,
            last_stable_offset=-1, log_start_offset=-1,
            aborted_transactions=[], preferred_read_replica=-1,
            records=b'')])])


# --------------------------------------------------------------------------- #
# Fixtures                                                                    #
# --------------------------------------------------------------------------- #


@pytest.fixture
def subscription_state():
    return SubscriptionState()


@pytest.fixture
def fetcher(broker, client, manager, subscription_state, metrics):
    """Fetcher wired to a bootstrapped client+manager+MockBroker."""
    # Bootstrap so the manager has a connected node and cluster metadata.
    _broker_metadata(broker, leader_epoch=3)
    manager.bootstrap(timeout_ms=5000)

    subscription_state.subscribe(topics=[TOPIC])
    subscription_state.assign_from_subscribed([TopicPartition(TOPIC, PARTITION)])
    fetcher = Fetcher(client, subscription_state, metrics=metrics,
                      retry_backoff_ms=50)
    return fetcher


# --------------------------------------------------------------------------- #
# Tests                                                                       #
# --------------------------------------------------------------------------- #


class TestKIP320OffsetValidation:
    """End-to-end OffsetsForLeaderEpoch flow through the wire."""

    def test_advanced_cluster_epoch_triggers_validation_request(
            self, broker, manager, fetcher):
        """When the metadata-cached leader_epoch advances past the
        consumer's position epoch, the next ``maybe_validate_positions``
        marks the partition and ``_validate_offsets_async`` issues an
        OffsetForLeaderEpochRequest over the wire."""
        tp = TopicPartition(TOPIC, PARTITION)
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(50, '', 3))

        # Bump the broker's leader_epoch and force a metadata refresh so
        # the consumer's cluster cache sees the new epoch.
        _broker_metadata(broker, leader_epoch=5)
        manager.cluster.request_update()
        manager._net.run(manager.wait_for, manager.cluster.request_update(), 1000)

        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            captured['version'] = api_version
            return _ofle_response(error_code=0, leader_epoch=5, end_offset=100)
        broker.respond_fn(OffsetForLeaderEpochRequest, handler)

        fetcher.maybe_validate_positions()
        assert fetcher._subscriptions.assignment[tp].awaiting_validation

        manager.run(fetcher._validate_offsets_async, 1000)

        assert 'version' in captured, 'OffsetForLeaderEpochRequest never reached broker'
        assert not fetcher._subscriptions.assignment[tp].awaiting_validation
        # Position's epoch advanced to the broker-confirmed epoch.
        assert fetcher._subscriptions.assignment[tp].position.leader_epoch == 5
        assert fetcher._subscriptions.assignment[tp].position.offset == 50

    def test_truncation_triggers_auto_reset_with_policy(
            self, broker, manager, fetcher):
        """end_offset < position.offset on the wire response triggers
        ``request_offset_reset`` instead of surfacing
        LogTruncationError (default policy is 'earliest')."""
        tp = TopicPartition(TOPIC, PARTITION)
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(100, '', 3))
        fetcher._subscriptions.maybe_validate_position_for_current_leader(tp, 5)

        broker.respond(OffsetForLeaderEpochRequest,
                       _ofle_response(error_code=0, leader_epoch=3, end_offset=80))

        manager.run(fetcher._validate_offsets_async, 1000)

        # Auto-reset path: no cached truncation, partition awaiting reset
        assert fetcher._cached_log_truncation is None
        assert fetcher._subscriptions.assignment[tp].awaiting_reset
        assert not fetcher._subscriptions.assignment[tp].awaiting_validation

    def test_truncation_raises_when_no_reset_policy(
            self, broker, client, manager, metrics):
        """With offset_reset_strategy=NONE, the same wire response
        produces LogTruncationError carrying the diverging offsets."""
        # Re-create the fetcher with explicit NONE policy.
        _broker_metadata(broker, leader_epoch=3)
        manager.bootstrap(timeout_ms=5000)
        subs = SubscriptionState(offset_reset_strategy='none')
        subs.subscribe(topics=[TOPIC])
        tp = TopicPartition(TOPIC, PARTITION)
        subs.assign_from_subscribed([tp])
        subs.seek(tp, OffsetAndMetadata(100, '', 3))
        fetcher = Fetcher(client, subs, metrics=metrics, retry_backoff_ms=50)
        subs.maybe_validate_position_for_current_leader(tp, 5)

        broker.respond(OffsetForLeaderEpochRequest,
                       _ofle_response(error_code=0, leader_epoch=3, end_offset=80))

        manager.run(fetcher._validate_offsets_async, 1000)

        with pytest.raises(Errors.LogTruncationError) as exc_info:
            fetcher.validate_offsets_if_needed()
        divergent = exc_info.value.divergent_offsets
        assert tp in divergent
        assert divergent[tp].offset == 80
        assert divergent[tp].leader_epoch == 3

    def test_fenced_epoch_on_fetch_marks_validation_then_succeeds(
            self, broker, manager, fetcher):
        """A FENCED_LEADER_EPOCH on a real Fetch response routes through
        ``request_position_validation``; a subsequent
        ``_validate_offsets_async`` issues the OffsetForLeaderEpochRequest
        and clears the flag."""
        tp = TopicPartition(TOPIC, PARTITION)
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(50, '', 3))

        # Script: one Fetch -> FENCED_LEADER_EPOCH, one OffsetForLeaderEpoch -> success
        broker.respond(FetchRequest, _fetch_response_with_error(
            Errors.FencedLeaderEpochError.errno))
        broker.respond(OffsetForLeaderEpochRequest,
                       _ofle_response(error_code=0, leader_epoch=3, end_offset=100))

        # Drive a fetch+poll cycle. send_fetches schedules the request;
        # we then await it and let the response handler run.
        fetcher.send_fetches()
        # Drain the fetch response into _completed_fetches and parse it.
        # fetch_records pulls everything pending and runs _parse_fetched_data.
        fetcher.fetch_records(timeout_ms=500)
        assert fetcher._subscriptions.assignment[tp].awaiting_validation

        # Now run the validation driver; it should send OffsetForLeaderEpoch
        # and clear the flag.
        manager.run(fetcher._validate_offsets_async, 1000)
        assert not fetcher._subscriptions.assignment[tp].awaiting_validation

    def test_validation_retries_on_fenced_epoch_response(
            self, broker, manager, fetcher):
        """FENCED_LEADER_EPOCH in the OffsetForLeaderEpoch response itself
        is retried after ``retry_backoff_ms`` within a single
        ``_validate_offsets_async`` invocation."""
        tp = TopicPartition(TOPIC, PARTITION)
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(50, '', 3))
        fetcher._subscriptions.maybe_validate_position_for_current_leader(tp, 5)

        # First response: FENCED_LEADER_EPOCH. Second: success.
        broker.respond(OffsetForLeaderEpochRequest, _ofle_response(
            error_code=Errors.FencedLeaderEpochError.errno,
            leader_epoch=-1, end_offset=-1))
        broker.respond(OffsetForLeaderEpochRequest, _ofle_response(
            error_code=0, leader_epoch=5, end_offset=100))

        start = time.monotonic()
        manager.run(fetcher._validate_offsets_async, 2000)
        elapsed = time.monotonic() - start

        assert not fetcher._subscriptions.assignment[tp].awaiting_validation
        assert fetcher._subscriptions.assignment[tp].position.leader_epoch == 5
        assert elapsed >= 0.05, (
            '_validate_offsets_async did not sleep for retry_backoff_ms; %.3fs'
            % elapsed)
