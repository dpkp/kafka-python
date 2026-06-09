"""Hanging-transaction tooling mixin for KafkaAdminClient (KIP-664).

Exposes four wire APIs (ListTransactions, DescribeTransactions,
DescribeProducers, WriteTxnMarkers in admin abort mode) plus the
``find_hanging_transactions`` convenience that ties them together,
mirroring the Java tool's ``kafka-transactions.sh --find-hanging``.
"""
from __future__ import annotations

from collections import defaultdict
from enum import Enum
import logging
from typing import TYPE_CHECKING, Dict, List, NamedTuple, Optional, Set

import kafka.errors as Errors
from kafka.protocol.admin.transactions import (
    DescribeProducersRequest, DescribeTransactionsRequest,
    ListTransactionsRequest,
)
from kafka.protocol.metadata import CoordinatorType
from kafka.protocol.producer.transaction import WriteTxnMarkersRequest
from kafka.structs import TopicPartition
from kafka.util import EnumHelper

if TYPE_CHECKING:
    from kafka.net.manager import KafkaConnectionManager

log = logging.getLogger(__name__)


class TransactionState(EnumHelper, str, Enum):
    """Broker-reported transaction states (DescribeTransactions /
    ListTransactions wire values)."""
    EMPTY = 'Empty'
    ONGOING = 'Ongoing'
    PREPARE_COMMIT = 'PrepareCommit'
    PREPARE_ABORT = 'PrepareAbort'
    COMPLETE_COMMIT = 'CompleteCommit'
    COMPLETE_ABORT = 'CompleteAbort'
    DEAD = 'Dead'
    PREPARE_EPOCH_FENCE = 'PrepareEpochFence'
    UNKNOWN = 'Unknown'


class TransactionListing(NamedTuple):
    """One row from a ListTransactions response."""
    transactional_id: str
    producer_id: int
    state: TransactionState


class TransactionDescription(NamedTuple):
    """One transactional id's state as returned by DescribeTransactions,
    plus the coordinator that owns it."""
    coordinator_id: int
    state: TransactionState
    producer_id: int
    producer_epoch: int
    transaction_timeout_ms: int
    transaction_start_time_ms: int
    topic_partitions: Set[TopicPartition]


class ProducerState(NamedTuple):
    """One ActiveProducer row from DescribeProducers."""
    producer_id: int
    producer_epoch: int
    last_sequence: int
    last_timestamp: int
    coordinator_epoch: int
    current_transaction_start_offset: int


class PartitionProducerState(NamedTuple):
    active_producers: List[ProducerState]


class AbortTransactionSpec(NamedTuple):
    """Inputs for ``abort_transaction``. ``coordinator_epoch=-1`` is the
    sentinel used by the Java admin tool to bypass the epoch check; the
    partition leader still validates ``producer_id``/``producer_epoch``
    against current state."""
    topic_partition: TopicPartition
    producer_id: int
    producer_epoch: int
    coordinator_epoch: int = -1


class TransactionsAdminMixin:
    """Mixin providing KIP-664 hanging-transaction tooling."""
    _manager: "KafkaConnectionManager"
    config: dict

    # -- ListTransactions ---------------------------------------------------

    @staticmethod
    def _list_transactions_request(state_filters=None, producer_id_filters=None,
                                   duration_filter_ms=None,
                                   transactional_id_pattern=None):
        kwargs = {'min_version': 0}
        kwargs['state_filters'] = list(state_filters) if state_filters else []
        kwargs['producer_id_filters'] = list(producer_id_filters) if producer_id_filters else []
        if duration_filter_ms is not None:
            kwargs['duration_filter'] = int(duration_filter_ms)
            kwargs['min_version'] = max(kwargs['min_version'], 1)
        if transactional_id_pattern is not None:
            kwargs['transactional_id_pattern'] = transactional_id_pattern
            kwargs['min_version'] = max(kwargs['min_version'], 2)
        return ListTransactionsRequest(**kwargs)

    @staticmethod
    def _list_transactions_process_response(response):
        error_type = Errors.for_code(response.error_code)
        if error_type is not Errors.NoError:
            raise error_type(
                "ListTransactionsRequest failed with response '{}'.".format(response))
        listings = []
        for txn in response.transaction_states:
            listings.append(TransactionListing(
                transactional_id=txn.transactional_id,
                producer_id=txn.producer_id,
                state=TransactionState(txn.transaction_state),
            ))
        return listings

    async def _async_list_transactions(self, broker_ids=None, producer_id_filters=None,
                                       state_filters=None, duration_filter_ms=None,
                                       transactional_id_pattern=None):
        # Semantic version pre-checks: the user-visible flag silently
        # disappears at the wire if we don't enforce it.
        if duration_filter_ms is not None:
            if self._manager.broker_version_data.api_version(ListTransactionsRequest) < 1:
                raise Errors.UnsupportedVersionError(
                    'duration_filter_ms requires broker support for '
                    'ListTransactions v1+ (Apache Kafka 3.8+).')
        if transactional_id_pattern is not None:
            if self._manager.broker_version_data.api_version(ListTransactionsRequest) < 2:
                raise Errors.UnsupportedVersionError(
                    'transactional_id_pattern requires broker support for '
                    'ListTransactions v2+ (Apache Kafka 4.1+, KIP-1152).')

        if broker_ids is None:
            broker_ids = [broker.node_id for broker in self._manager.cluster.brokers()]
        results = {}
        for broker_id in broker_ids:
            request = self._list_transactions_request(
                state_filters=state_filters,
                producer_id_filters=producer_id_filters,
                duration_filter_ms=duration_filter_ms,
                transactional_id_pattern=transactional_id_pattern,
            )
            response = await self._manager.send(request, node_id=broker_id)
            results[broker_id] = self._list_transactions_process_response(response)
        return results

    def list_transactions(self, broker_ids=None, producer_id_filters=None,
                          state_filters=None, duration_filter_ms=None,
                          transactional_id_pattern=None):
        """List active transactions across all brokers (or a subset).

        Each broker hosts a slice of the ``__transaction_state`` topic,
        so a full listing requires sharding the request to every broker
        and concatenating the results.

        Keyword Arguments:
            broker_ids ([int], optional): Brokers to query. Default: every
                broker in the cluster metadata.
            producer_id_filters ([int], optional): Only return transactions
                whose ``producer_id`` is in this list.
            state_filters ([str], optional): Only return transactions whose
                broker-reported state matches. Accepts :class:`TransactionState`
                members or their string wire values.
            duration_filter_ms (int, optional): Only return transactions
                running longer than this. Requires broker >= 3.8
                (ListTransactions v1+).
            transactional_id_pattern (str, optional): Only return
                transactions whose transactional id matches this regex.
                Requires broker >= 4.1 (ListTransactions v2+, KIP-1152).

        Returns:
            dict: A dict mapping broker ``node_id`` to a list of
            :class:`TransactionListing`.
        """
        return self._manager.run(
            self._async_list_transactions, broker_ids, producer_id_filters,
            state_filters, duration_filter_ms, transactional_id_pattern)

    # -- DescribeTransactions ----------------------------------------------

    @staticmethod
    def _describe_transactions_request(transactional_ids):
        return DescribeTransactionsRequest(transactional_ids=list(transactional_ids))

    @staticmethod
    def _describe_transactions_process_response(response, coordinator_id):
        results = {}
        for txn in response.transaction_states:
            error_type = Errors.for_code(txn.error_code)
            if error_type is not Errors.NoError:
                raise error_type(
                    "DescribeTransactionsRequest failed for transactional id '{}'."
                    .format(txn.transactional_id))
            topic_partitions = set()
            for topic in txn.topics:
                for partition in topic.partitions:
                    topic_partitions.add(TopicPartition(topic.topic, partition))
            results[txn.transactional_id] = TransactionDescription(
                coordinator_id=coordinator_id,
                state=TransactionState(txn.transaction_state),
                producer_id=txn.producer_id,
                producer_epoch=txn.producer_epoch,
                transaction_timeout_ms=txn.transaction_timeout_ms,
                transaction_start_time_ms=txn.transaction_start_time_ms,
                topic_partitions=topic_partitions,
            )
        return results

    async def _async_describe_transactions(self, transactional_ids):
        transactional_ids = list(transactional_ids)
        if not transactional_ids:
            return {}
        coordinator_ids = await self._find_coordinator_ids(
            transactional_ids, key_type=CoordinatorType.TRANSACTION)
        coordinator_to_txn_ids = defaultdict(list)
        for txn_id, coord_id in coordinator_ids.items():
            coordinator_to_txn_ids[coord_id].append(txn_id)
        results = {}
        for coord_id, txn_ids in coordinator_to_txn_ids.items():
            request = self._describe_transactions_request(txn_ids)
            response = await self._manager.send(request, node_id=coord_id)
            results.update(self._describe_transactions_process_response(response, coord_id))
        return results

    def describe_transactions(self, transactional_ids):
        """Describe one or more transactions by transactional id.

        Each request is routed to the transaction coordinator that owns
        the transactional id (discovered via FindCoordinator with
        ``CoordinatorType.TRANSACTION``).

        Arguments:
            transactional_ids: Iterable of transactional id strings.

        Returns:
            dict: A dict mapping ``transactional_id`` (str) to
            :class:`TransactionDescription`.

        Raises:
            TransactionalIdNotFoundError: If a transactional id is unknown
                to its coordinator.
            BrokerResponseError: For any other per-id error.
        """
        return self._manager.run(self._async_describe_transactions, transactional_ids)

    # -- DescribeProducers --------------------------------------------------

    @staticmethod
    def _describe_producers_request(partitions_by_topic):
        Topic = DescribeProducersRequest.TopicRequest
        topics = [
            Topic(name=name, partition_indexes=list(parts))
            for name, parts in partitions_by_topic.items()
        ]
        return DescribeProducersRequest(topics=topics)

    @staticmethod
    def _describe_producers_process_response(response):
        results = {}
        for topic in response.topics:
            for partition in topic.partitions:
                tp = TopicPartition(topic.name, partition.partition_index)
                error_type = Errors.for_code(partition.error_code)
                if error_type is not Errors.NoError:
                    raise error_type(
                        "DescribeProducersRequest failed for {}: {}"
                        .format(tp, partition.error_message))
                producers = [
                    ProducerState(
                        producer_id=p.producer_id,
                        producer_epoch=p.producer_epoch,
                        last_sequence=p.last_sequence,
                        last_timestamp=p.last_timestamp,
                        coordinator_epoch=p.coordinator_epoch,
                        current_transaction_start_offset=p.current_txn_start_offset,
                    ) for p in partition.active_producers
                ]
                results[tp] = PartitionProducerState(active_producers=producers)
        return results

    async def _async_describe_producers(self, partitions, broker_id=None):
        partitions = list(partitions)
        if not partitions:
            return {}

        if broker_id is not None:
            # Send a single request to the specified replica.
            partitions_by_topic = defaultdict(list)
            for tp in partitions:
                partitions_by_topic[tp.topic].append(tp.partition)
            request = self._describe_producers_request(partitions_by_topic)
            response = await self._manager.send(request, node_id=broker_id)
            return self._describe_producers_process_response(response)

        # Route per-partition to the current leader. Shares the metadata
        # round-trip helper used by partition-level operations.
        leader2partitions = await self._async_get_leader_for_partitions(partitions)
        results = {}
        for leader, leader_tps in leader2partitions.items():
            partitions_by_topic = defaultdict(list)
            for tp in leader_tps:
                partitions_by_topic[tp.topic].append(tp.partition)
            request = self._describe_producers_request(partitions_by_topic)
            response = await self._manager.send(request, node_id=leader)
            results.update(self._describe_producers_process_response(response))
        return results

    def describe_producers(self, partitions, broker_id=None):
        """Describe active producer state on a set of topic partitions.

        Arguments:
            partitions: Iterable of :class:`~kafka.TopicPartition`.

        Keyword Arguments:
            broker_id (int, optional): Replica to query. Default: the
                partition leader (discovered from cluster metadata).

        Returns:
            dict: A dict mapping :class:`~kafka.TopicPartition` to
            :class:`PartitionProducerState`.

        Raises:
            BrokerResponseError: For any per-partition error (e.g.
                ``NotLeaderOrFollowerError`` if the chosen broker is not
                a replica).
        """
        return self._manager.run(self._async_describe_producers, partitions, broker_id)

    # -- AbortTransaction (WriteTxnMarkers) --------------------------------

    @staticmethod
    def _abort_transaction_request(spec):
        Marker = WriteTxnMarkersRequest.WritableTxnMarker
        Topic = Marker.WritableTxnMarkerTopic
        marker = Marker(
            producer_id=spec.producer_id,
            producer_epoch=spec.producer_epoch,
            transaction_result=False,  # False -> ABORT
            topics=[Topic(name=spec.topic_partition.topic,
                          partition_indexes=[spec.topic_partition.partition])],
            coordinator_epoch=spec.coordinator_epoch,
        )
        return WriteTxnMarkersRequest(markers=[marker])

    @staticmethod
    def _abort_transaction_process_response(response, spec):
        for result in response.markers:
            for topic in result.topics:
                for partition in topic.partitions:
                    error_type = Errors.for_code(partition.error_code)
                    if error_type is not Errors.NoError:
                        raise error_type(
                            "WriteTxnMarkers (abort) failed for {}".format(spec.topic_partition))

    async def _async_abort_transaction(self, spec):
        leader2partitions = await self._async_get_leader_for_partitions([spec.topic_partition])
        leader = next(iter(leader2partitions))
        request = self._abort_transaction_request(spec)
        response = await self._manager.send(request, node_id=leader)
        self._abort_transaction_process_response(response, spec)

    def abort_transaction(self, spec):
        """Administratively abort an open transaction on a partition.

        Sends a WriteTxnMarkers request (with ``transaction_result=False``)
        to the partition leader. The leader validates ``producer_id`` /
        ``producer_epoch`` against current state before writing the
        abort marker. Pass ``coordinator_epoch=-1`` (the default) to
        signal an admin abort that bypasses the coordinator-epoch
        guard, matching the Java AdminClient behaviour.

        Arguments:
            spec (:class:`AbortTransactionSpec`): Target partition,
                producer id/epoch, and optional coordinator epoch.
        """
        return self._manager.run(self._async_abort_transaction, spec)

    # -- find_hanging convenience ------------------------------------------

    async def _async_find_hanging_transactions(self, broker_ids=None,
                                               max_transaction_timeout_ms=900000):
        # Padding matches the Java tool: a transaction is only flagged
        # "hanging" if it has been alive longer than the broker-side
        # max-transaction-timeout plus the 5-minute slack the tool uses.
        threshold_ms = max_transaction_timeout_ms + 5 * 60 * 1000
        listings_by_broker = await self._async_list_transactions(broker_ids=broker_ids)
        txn_ids = sorted({t.transactional_id for txns in listings_by_broker.values() for t in txns})
        if not txn_ids:
            return []
        descriptions = await self._async_describe_transactions(txn_ids)
        # Resolve "now" via the latest start-time we observed; we don't
        # have a reliable broker clock otherwise. Fall back to local time
        # for an empty result.
        import time
        now_ms = int(time.time() * 1000)
        hanging = []
        for txn_id, desc in descriptions.items():
            if desc.state in (TransactionState.EMPTY, TransactionState.COMPLETE_COMMIT,
                              TransactionState.COMPLETE_ABORT, TransactionState.DEAD):
                continue
            if desc.transaction_start_time_ms < 0:
                continue
            age_ms = now_ms - desc.transaction_start_time_ms
            if age_ms < threshold_ms:
                continue
            hanging.append({
                'transactional_id': txn_id,
                'producer_id': desc.producer_id,
                'producer_epoch': desc.producer_epoch,
                'state': desc.state.value,
                'age_ms': age_ms,
                'coordinator_id': desc.coordinator_id,
                'topic_partitions': sorted(desc.topic_partitions),
            })
        return hanging

    def find_hanging_transactions(self, broker_ids=None,
                                  max_transaction_timeout_ms=900000):
        """Detect transactions whose age exceeds the broker timeout + 5min.

        Convenience wrapper that runs :meth:`list_transactions` against
        each broker, then :meth:`describe_transactions` to read
        ``transaction_start_time_ms``, and filters to transactions in an
        active state whose age exceeds the threshold. Mirrors
        ``kafka-transactions.sh --find-hanging``.

        Keyword Arguments:
            broker_ids ([int], optional): Brokers to query. Default: all.
            max_transaction_timeout_ms (int): Suspected-hang threshold.
                Default: 900000 (15 minutes -- Kafka's default
                ``transaction.max.timeout.ms``).

        Returns:
            list: One dict per suspected hanging transaction with keys
            ``transactional_id``, ``producer_id``, ``producer_epoch``,
            ``state``, ``age_ms``, ``coordinator_id``,
            ``topic_partitions``.
        """
        return self._manager.run(
            self._async_find_hanging_transactions, broker_ids, max_transaction_timeout_ms)

