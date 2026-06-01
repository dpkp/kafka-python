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

    def test_diverged_seeks_to_endpoint_with_policy(
            self, broker, manager, fetcher):
        """end_offset < position.offset (valid epoch) on the wire triggers
        a seek to the broker-reported divergence point - preserves progress
        and tags position with the confirmed epoch. Mirrors Java's
        ``state.seekValidated(newPosition)``."""
        tp = TopicPartition(TOPIC, PARTITION)
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(100, '', 3))
        fetcher._subscriptions.maybe_validate_position_for_current_leader(tp, 5)

        broker.respond(OffsetForLeaderEpochRequest,
                       _ofle_response(error_code=0, leader_epoch=3, end_offset=80))

        manager.run(fetcher._validate_offsets_async, 1000)

        assert fetcher._cached_log_truncation is None
        assert not fetcher._subscriptions.assignment[tp].awaiting_reset
        assert not fetcher._subscriptions.assignment[tp].awaiting_validation
        pos = fetcher._subscriptions.assignment[tp].position
        assert pos.offset == 80
        assert pos.leader_epoch == 3

    def test_diverged_raises_when_no_reset_policy(
            self, broker, client, manager, metrics):
        """With offset_reset_strategy=NONE, the same wire response
        produces LogTruncationError carrying the divergent offset and
        leaves the position untouched."""
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
        assert divergent[tp] is not None
        assert divergent[tp].offset == 80
        assert divergent[tp].leader_epoch == 3
        # Position untouched.
        assert subs.assignment[tp].position.offset == 100

    def test_undefined_response_resets_with_policy(
            self, broker, manager, fetcher):
        """UNDEFINED end_offset/leader_epoch (broker has no record of our
        epoch) with a reset policy: no known seek point, so fall back to
        auto_offset_reset rather than silently dropping the epoch."""
        tp = TopicPartition(TOPIC, PARTITION)
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(100, '', 3))
        fetcher._subscriptions.maybe_validate_position_for_current_leader(tp, 5)

        broker.respond(OffsetForLeaderEpochRequest,
                       _ofle_response(error_code=0, leader_epoch=-1, end_offset=-1))

        manager.run(fetcher._validate_offsets_async, 1000)

        assert fetcher._cached_log_truncation is None
        assert fetcher._subscriptions.assignment[tp].awaiting_reset
        assert not fetcher._subscriptions.assignment[tp].awaiting_validation

    def test_undefined_response_raises_when_no_reset_policy(
            self, broker, client, manager, metrics):
        """UNDEFINED response with reset policy NONE: LogTruncationError
        with divergent_offsets[tp] == None (no known recovery point)."""
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
                       _ofle_response(error_code=0, leader_epoch=-1, end_offset=-1))

        manager.run(fetcher._validate_offsets_async, 1000)

        with pytest.raises(Errors.LogTruncationError) as exc_info:
            fetcher.validate_offsets_if_needed()
        assert tp in exc_info.value.divergent_offsets
        assert exc_info.value.divergent_offsets[tp] is None
        # Position untouched; caller decides.
        assert subs.assignment[tp].position.offset == 100

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


# --------------------------------------------------------------------------- #
# KIP-392: rack-aware fetching                                                #
# --------------------------------------------------------------------------- #


def _fetch_response_with_preferred_replica(preferred_read_replica):
    """Empty-records FetchResponse advertising a preferred read replica."""
    return FetchResponse(
        throttle_time_ms=0, error_code=0, session_id=0,
        responses=[_FetchTopic(topic=TOPIC, partitions=[_FetchPartition(
            partition_index=PARTITION, error_code=0, high_watermark=100,
            last_stable_offset=-1, log_start_offset=-1,
            aborted_transactions=[],
            preferred_read_replica=preferred_read_replica,
            records=b'')])])


class TestKIP392RackAwareFetching:
    """End-to-end: client_rack arrives on the wire and the broker's
    preferred_read_replica is honored on the next fetch."""

    def test_rack_id_sent_on_fetch_request(self, broker, manager, fetcher):
        """FetchRequest carries ``rack_id`` when client_rack is configured,
        and negotiates to v11+ against a modern broker."""
        fetcher.config['client_rack'] = 'us-east-1a'
        tp = TopicPartition(TOPIC, PARTITION)
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(0, '', 3))

        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            captured['api_version'] = api_version
            decoded = FetchRequest.decode(
                request_bytes, version=api_version, header=True)
            captured['rack_id'] = decoded.rack_id
            return _fetch_response_with_preferred_replica(-1)
        broker.respond_fn(FetchRequest, handler)

        # Build the FetchRequest via the Fetcher (so client_rack is wired in)
        # and send it directly via manager - sidesteps the IO-thread driving
        # complexity of fetcher.send_fetches() in a synchronous test.
        requests = fetcher._create_fetch_requests()
        assert 0 in requests, 'expected one fetch routed to the leader (node 0)'
        request, _ = requests[0]
        future = manager.send(request, node_id=0)
        manager.run(manager.wait_for, future, 2000)

        assert captured['api_version'] >= 11, (
            'KIP-392 requires FetchRequest v11+; got v%s' % captured.get('api_version'))
        assert captured['rack_id'] == 'us-east-1a'

    def test_preferred_replica_cached_and_used_on_next_fetch(
            self, broker, manager, fetcher):
        """First fetch goes to the leader; broker returns
        ``preferred_read_replica=N``; second fetch routes to node N."""
        tp = TopicPartition(TOPIC, PARTITION)
        # Make node 5 reachable via metadata (still pointing at the same
        # MockBroker socket so the request actually completes).
        broker.set_metadata(
            brokers=[
                _MetaBroker(node_id=0, host=broker.host, port=broker.port, rack=None),
                _MetaBroker(node_id=5, host=broker.host, port=broker.port, rack=None),
            ],
            topics=[_MetaTopic(
                error_code=0, name=TOPIC, is_internal=False,
                partitions=[_MetaPartition(
                    error_code=0, partition_index=PARTITION,
                    leader_id=0, leader_epoch=3,
                    replica_nodes=[0, 5], isr_nodes=[0, 5],
                    offline_replicas=[])],
            )])
        # Re-pull metadata so the cluster cache knows about node 5.
        manager._net.run(manager.wait_for, manager.cluster.request_update(), 2000)

        fetcher._subscriptions.seek(tp, OffsetAndMetadata(0, '', 3))

        # Without a cached preferred replica, the first fetch must go to
        # the leader (node 0).
        assert fetcher._select_read_replica(tp) == 0

        # Synthesize a fetch response advertising node 5 as preferred.
        from unittest.mock import MagicMock
        from kafka.consumer.fetcher import CompletedFetch
        completed = CompletedFetch(
            tp, 0, 11,
            _fetch_response_with_preferred_replica(5).responses[0].partitions[0],
            MagicMock())
        fetcher._parse_fetched_data(completed)

        # Next fetch should route to node 5.
        assert fetcher._select_read_replica(tp) == 5

    def test_preferred_replica_negative_one_means_leader(
            self, broker, manager, fetcher):
        """``preferred_read_replica == -1`` is the broker explicitly telling
        the client to stop using a cached follower."""
        tp = TopicPartition(TOPIC, PARTITION)
        fetcher._subscriptions.assignment[tp].update_preferred_read_replica(
            5, time.monotonic() + 60)

        from unittest.mock import MagicMock
        from kafka.consumer.fetcher import CompletedFetch
        completed = CompletedFetch(
            tp, 0, 11,
            _fetch_response_with_preferred_replica(-1).responses[0].partitions[0],
            MagicMock())
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(0, '', 3))
        fetcher._parse_fetched_data(completed)
        assert fetcher._subscriptions.assignment[tp].preferred_read_replica() is None


# --------------------------------------------------------------------------- #
# Fetch v12: split request epochs + tagged response fields                    #
# --------------------------------------------------------------------------- #


_EpochEndOffset = _FetchPartition.EpochEndOffset
_LeaderIdAndEpoch = _FetchPartition.LeaderIdAndEpoch


def _fetch_v12_partition_data(error_code=0, high_watermark=100,
                              diverging_epoch=None, current_leader=None,
                              records=b''):
    return _FetchPartition(
        partition_index=PARTITION, error_code=error_code,
        high_watermark=high_watermark, last_stable_offset=-1,
        log_start_offset=-1,
        diverging_epoch=diverging_epoch,
        current_leader=current_leader,
        snapshot_id=None,
        aborted_transactions=[], preferred_read_replica=-1,
        records=records)


class TestFetchV12Epoch:
    """FetchRequest v12 split-epoch request encoding and tagged response handling."""

    def test_negotiates_v12_and_sends_split_epoch_fields(
            self, broker, manager, fetcher):
        """current_leader_epoch comes from cluster metadata, last_fetched_epoch
        from the position - and they are sent distinctly on the wire."""
        tp = TopicPartition(TOPIC, PARTITION)
        # position.leader_epoch = 3 (record view), cluster epoch = 5 (metadata view)
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(50, '', 3))
        assert manager.cluster.update_partition_leader(
            tp, leader_id=broker.node_id, leader_epoch=5) is True

        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            captured['api_version'] = api_version
            decoded = FetchRequest.decode(
                request_bytes, version=api_version, header=True)
            partition = decoded.topics[0].partitions[0]
            captured['current_leader_epoch'] = partition.current_leader_epoch
            captured['last_fetched_epoch'] = partition.last_fetched_epoch
            return _fetch_response_with_preferred_replica(-1)
        broker.respond_fn(FetchRequest, handler)

        requests = fetcher._create_fetch_requests()
        assert broker.node_id in requests
        request, _ = requests[broker.node_id]
        future = manager.send(request, node_id=broker.node_id)
        manager.run(manager.wait_for, future, 2000)

        assert captured['api_version'] >= 12, (
            'expected Fetch v12+ negotiation; got v%s' % captured.get('api_version'))
        assert captured['current_leader_epoch'] == 5
        assert captured['last_fetched_epoch'] == 3

    def test_last_fetched_epoch_is_minus_one_when_position_has_no_epoch(
            self, broker, manager, fetcher):
        """A position without a known epoch (e.g. bare seek) sends -1."""
        tp = TopicPartition(TOPIC, PARTITION)
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(0, '', -1))

        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            captured['api_version'] = api_version
            decoded = FetchRequest.decode(
                request_bytes, version=api_version, header=True)
            captured['last_fetched_epoch'] = decoded.topics[0].partitions[0].last_fetched_epoch
            return _fetch_response_with_preferred_replica(-1)
        broker.respond_fn(FetchRequest, handler)

        requests = fetcher._create_fetch_requests()
        request, _ = requests[broker.node_id]
        future = manager.send(request, node_id=broker.node_id)
        manager.run(manager.wait_for, future, 2000)

        assert captured['last_fetched_epoch'] == -1

    def test_current_leader_epoch_minus_one_when_metadata_has_no_epoch(
            self, broker, manager, fetcher):
        """If the cluster cache has no epoch for the partition, send -1
        (not the position epoch) - we honestly don't know the current leader."""
        tp = TopicPartition(TOPIC, PARTITION)
        # Force the cached partition's leader_epoch to -1 (unknown).
        manager.cluster._partitions[TOPIC][PARTITION].leader_epoch = -1
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(0, '', 7))

        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            decoded = FetchRequest.decode(
                request_bytes, version=api_version, header=True)
            partition = decoded.topics[0].partitions[0]
            captured['current_leader_epoch'] = partition.current_leader_epoch
            captured['last_fetched_epoch'] = partition.last_fetched_epoch
            return _fetch_response_with_preferred_replica(-1)
        broker.respond_fn(FetchRequest, handler)

        requests = fetcher._create_fetch_requests()
        request, _ = requests[broker.node_id]
        future = manager.send(request, node_id=broker.node_id)
        manager.run(manager.wait_for, future, 2000)

        assert captured['current_leader_epoch'] == -1
        assert captured['last_fetched_epoch'] == 7

    def test_diverging_epoch_response_marks_partition_for_validation(
            self, broker, manager, fetcher, mocker):
        """A v12 response carrying diverging_epoch (with empty records)
        routes the partition into KIP-320 validation instead of parsing
        records."""
        from unittest.mock import MagicMock
        from kafka.consumer.fetcher import CompletedFetch

        tp = TopicPartition(TOPIC, PARTITION)
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(100, '', 3))
        spy = mocker.spy(manager.cluster, 'request_update')

        partition_data = _fetch_v12_partition_data(
            diverging_epoch=_EpochEndOffset(epoch=3, end_offset=80))
        completed = CompletedFetch(tp, 100, 12, partition_data, MagicMock())

        result = fetcher._parse_fetched_data(completed)

        assert result is None
        assert fetcher._subscriptions.assignment[tp].awaiting_validation
        assert spy.call_count >= 1

    def test_diverging_epoch_with_unset_end_offset_is_ignored(
            self, broker, manager, fetcher):
        """A divergence struct with end_offset = -1 is treated as 'no
        divergence reported' and records are parsed normally."""
        from unittest.mock import MagicMock
        from kafka.consumer.fetcher import CompletedFetch

        tp = TopicPartition(TOPIC, PARTITION)
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(100, '', 3))

        partition_data = _fetch_v12_partition_data(
            diverging_epoch=_EpochEndOffset(epoch=-1, end_offset=-1),
            high_watermark=100, records=b'')
        completed = CompletedFetch(tp, 100, 12, partition_data, MagicMock())

        fetcher._parse_fetched_data(completed)
        assert not fetcher._subscriptions.assignment[tp].awaiting_validation

    def test_current_leader_hint_updates_cluster_cache_on_known_broker(
            self, broker, manager, fetcher, mocker):
        """A leader-change error response carrying current_leader with a
        newer epoch updates the cached leader id+epoch. If the new leader id
        is a broker we already know, no metadata refresh is needed."""
        from unittest.mock import MagicMock
        from kafka.consumer.fetcher import CompletedFetch

        # Seed a second broker (node_id=7) into the cluster cache so the
        # hinted leader_id is already known.
        broker.set_metadata(
            brokers=[
                _MetaBroker(node_id=broker.node_id, host=broker.host,
                            port=broker.port, rack=None),
                _MetaBroker(node_id=7, host=broker.host,
                            port=broker.port, rack=None),
            ],
            topics=[_MetaTopic(
                error_code=0, name=TOPIC, is_internal=False,
                partitions=[_MetaPartition(
                    error_code=0, partition_index=PARTITION,
                    leader_id=broker.node_id, leader_epoch=3,
                    replica_nodes=[broker.node_id, 7],
                    isr_nodes=[broker.node_id, 7],
                    offline_replicas=[])],
            )])
        manager._net.run(manager.wait_for, manager.cluster.request_update(), 2000)

        tp = TopicPartition(TOPIC, PARTITION)
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(50, '', 3))
        spy = mocker.spy(manager.cluster, 'request_update')

        partition_data = _fetch_v12_partition_data(
            error_code=Errors.NotLeaderForPartitionError.errno,
            current_leader=_LeaderIdAndEpoch(leader_id=7, leader_epoch=9))
        completed = CompletedFetch(tp, 50, 12, partition_data, MagicMock())

        fetcher._parse_fetched_data(completed)

        # Cache was updated with the hinted leader.
        assert manager.cluster.leader_for_partition(tp) == 7
        assert manager.cluster.leader_epoch_for_partition(tp) == 9
        # request_update is still called from the NotLeaderForPartition branch
        # itself (existing behavior); the hint just avoided an *additional*
        # forced refresh for an unknown broker.
        assert spy.call_count >= 1

    def test_current_leader_hint_unknown_broker_requests_metadata_update(
            self, broker, manager, fetcher, mocker):
        """The case we explicitly care about: the broker advertises a new
        leader (node_id=99) that we don't have broker metadata for. The
        fetcher must trigger a metadata refresh so the consumer learns the
        new leader's address (v12 has no node_endpoints)."""
        from unittest.mock import MagicMock
        from kafka.consumer.fetcher import CompletedFetch

        tp = TopicPartition(TOPIC, PARTITION)
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(50, '', 3))
        assert manager.cluster.broker_metadata(99) is None, (
            'precondition: node 99 must be unknown')
        spy = mocker.spy(manager.cluster, 'request_update')

        partition_data = _fetch_v12_partition_data(
            error_code=Errors.FencedLeaderEpochError.errno,
            current_leader=_LeaderIdAndEpoch(leader_id=99, leader_epoch=11))
        completed = CompletedFetch(tp, 50, 12, partition_data, MagicMock())

        fetcher._parse_fetched_data(completed)

        # Cache updated with the (unknown) leader id and new epoch.
        assert manager.cluster.leader_for_partition(tp) == 99
        assert manager.cluster.leader_epoch_for_partition(tp) == 11
        # request_update fires twice in this branch: once by the
        # _maybe_update_current_leader helper (because leader 99 is unknown)
        # and once by the FencedLeaderEpoch handler itself. Either way, the
        # important thing is that *at least one* refresh was requested.
        assert spy.call_count >= 1
        # And validation was queued (FencedLeaderEpoch path).
        assert fetcher._subscriptions.assignment[tp].awaiting_validation

    def test_current_leader_hint_with_stale_epoch_is_ignored(
            self, broker, manager, fetcher):
        """A hint whose epoch is not strictly newer than the cache must not
        rewrite the cached leader."""
        from unittest.mock import MagicMock
        from kafka.consumer.fetcher import CompletedFetch

        tp = TopicPartition(TOPIC, PARTITION)
        fetcher._subscriptions.seek(tp, OffsetAndMetadata(50, '', 3))
        # Cache currently has leader_id=broker.node_id, leader_epoch=3.
        partition_data = _fetch_v12_partition_data(
            error_code=Errors.NotLeaderForPartitionError.errno,
            current_leader=_LeaderIdAndEpoch(leader_id=99, leader_epoch=3))
        completed = CompletedFetch(tp, 50, 12, partition_data, MagicMock())

        fetcher._parse_fetched_data(completed)

        assert manager.cluster.leader_for_partition(tp) == broker.node_id
        assert manager.cluster.leader_epoch_for_partition(tp) == 3
