"""Partition management mixin for KafkaAdminClient.

Also defines NewPartitions data class.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import TYPE_CHECKING

import kafka.errors as Errors
from kafka.errors import UnknownTopicOrPartitionError
from kafka.protocol.admin import (
    AlterPartitionReassignmentsRequest,
    CreatePartitionsRequest,
    DeleteRecordsRequest,
    DescribeTopicPartitionsRequest,
    ElectLeadersRequest,
    ElectionType,
    ListPartitionReassignmentsRequest,
)
from kafka.protocol.consumer import ListOffsetsRequest, IsolationLevel, OffsetSpec
from kafka.structs import TopicPartition, OffsetAndTimestamp


if TYPE_CHECKING:
    from kafka.net.manager import KafkaConnectionManager

log = logging.getLogger(__name__)


class PartitionAdminMixin:
    """Mixin providing partition and record management methods."""
    _manager: KafkaConnectionManager
    config: dict

    @staticmethod
    def _process_create_partitions_input(topic_partitions):
        _Topic = CreatePartitionsRequest.CreatePartitionsTopic
        _Assignment = CreatePartitionsRequest.CreatePartitionsTopic.CreatePartitionsAssignment
        topics = []
        for topic, count in topic_partitions.items():
            if isinstance(count, int):
                topics.append(_Topic(name=topic, count=count))
            elif isinstance(count, dict):
                topics.append(
                    _Topic(
                        name=topic,
                        count=count['count'],
                        assignments=[_Assignment(broker_ids=broker_ids)
                                     for broker_ids in count['assignments']]))
            else:
                topics.append(
                    _Topic(
                        name=topic,
                        count=count.total_count,
                        assignments=[_Assignment(broker_ids=broker_ids)
                                     for broker_ids in count.new_assignments]))
        return topics

    def create_partitions(self, topic_partitions, timeout_ms=None, validate_only=False, raise_errors=True):
        """Create additional partitions for an existing topic.

        Arguments:
            topic_partitions: A dict of topic name strings to total partition count (int),
                or a dict of {topic_name: {count: int, assignments: [[broker_ids]]}}
                if manual assignment is desired.
                dict of {topic_name: NewPartitions} is deprecated.

        Keyword Arguments:
            timeout_ms (numeric, optional): Milliseconds to wait for new partitions to be
                created before the broker returns.
            validate_only (bool, optional): If True, don't actually create new partitions.
                Default: False
            raise_errors (bool, optional): Whether to raise errors as exceptions. Default True.

        Returns:
            Appropriate version of CreatePartitionsResponse class.
        """
        timeout_ms = self._validate_timeout(timeout_ms)
        request = CreatePartitionsRequest(
            topics=self._process_create_partitions_input(topic_partitions),
            timeout_ms=timeout_ms,
            validate_only=validate_only)

        def response_errors(r):
            for result in r.results:
                yield Errors.for_code(result.error_code)
        return self._manager.run(self._send_request_to_controller, request, response_errors, raise_errors)

    async def _async_get_leader_for_partitions(self, partitions):
        """Finds ID of the leader node for every given topic partition."""
        partitions = set(partitions)
        topics = set(tp.topic for tp in partitions)

        metadata = await self._get_cluster_metadata(topics)

        leader2partitions = defaultdict(set)
        valid_partitions = set()
        for topic in metadata.get("topics", ()):
            for partition in topic.get("partitions", ()):
                t2p = TopicPartition(topic=topic["name"], partition=partition["partition_index"])
                if t2p in partitions:
                    leader2partitions[partition["leader_id"]].add(t2p)
                    valid_partitions.add(t2p)

        if partitions != valid_partitions:
            unknown = partitions - valid_partitions
            raise UnknownTopicOrPartitionError(
                "The following partitions are not known: %s"
                % ", ".join(str(x) for x in unknown)
            )
        return leader2partitions

    async def _async_delete_records(self, records_to_delete, timeout_ms=None, partition_leader_id=None):
        timeout_ms = self._validate_timeout(timeout_ms)
        if partition_leader_id is None:
            leader2partitions = await self._async_get_leader_for_partitions(set(records_to_delete))
        else:
            leader2partitions = {partition_leader_id: set(records_to_delete)}

        responses = []
        for leader, partitions in leader2partitions.items():
            topic2partitions = defaultdict(list)
            for partition in partitions:
                topic2partitions[partition.topic].append(partition)

            request = DeleteRecordsRequest(
                topics=[
                    (topic, [(tp.partition, records_to_delete[tp]) for tp in partitions])
                    for topic, partitions in topic2partitions.items()
                ],
                timeout_ms=timeout_ms
            )
            response = await self._manager.send(request, node_id=leader)
            responses.append(response.to_dict())

        partition2result = {}
        partition2error = {}
        for response in responses:
            for topic in response["topics"]:
                for partition in topic["partitions"]:
                    tp = TopicPartition(topic["name"], partition["partition_index"])
                    partition2result[tp] = partition
                    if partition["error_code"] != 0:
                        partition2error[tp] = partition["error_code"]

        if partition2error:
            if len(partition2error) == 1:
                key, error = next(iter(partition2error.items()))
                raise Errors.for_code(error)(
                    "Error deleting records from topic %s partition %s" % (key.topic, key.partition)
                )
            else:
                raise Errors.BrokerResponseError(
                    "The following errors occured when trying to delete records: " +
                    ", ".join(
                        "%s(partition=%d): %s" %
                        (partition.topic, partition.partition, Errors.for_code(error).__name__)
                        for partition, error in partition2error.items()
                    )
                )

        return partition2result

    def delete_records(self, records_to_delete, timeout_ms=None, partition_leader_id=None):
        """Delete records whose offset is smaller than the given offset of the corresponding partition.

        Arguments:
            records_to_delete ({TopicPartition: int}): The earliest available offsets for the
                given partitions.

        Keyword Arguments:
            timeout_ms (numeric, optional): Timeout in milliseconds.
            partition_leader_id (node_id / int, optional): If specified, all deletion requests
                will be sent to this node.

        Returns:
            dict {topicPartition -> metadata}
        """
        return self._manager.run(self._async_delete_records, records_to_delete, timeout_ms, partition_leader_id)

    def _get_all_topic_partitions(self, topics=None):
        return [
            (
                topic['name'],
                [p['partition_index'] for p in topic['partitions']]
            )
            for topic in self.describe_topics(topics)
        ]

    def _get_topic_partitions(self, topic_partitions):
        if isinstance(topic_partitions, dict):
            return topic_partitions.items()
        else:
            return self._get_all_topic_partitions(topic_partitions)

    def elect_leaders(self, election_type, topic_partitions=None, timeout_ms=None, raise_errors=True):
        """Trigger leader election for the specified topic partitions.

        Arguments:
            election_type: Type of election to attempt. 0 for Preferred, 1 for Unclean

        Keyword Arguments:
            topic_partitions (dict, list, optional):
                Either: dict of {topic_name: [partition ids]}.
                Or:     list of [topic_name], and election will run on all partitions for topic.
                Or:     None, and election runs against all topics / all partitions.
                Default: None
            timeout_ms (num, optional): Milliseconds to wait for the leader election process.
            raise_errors (bool, optional): Whether to raise errors as exceptions. Default True.

        Returns:
            Appropriate version of ElectLeadersResponse class.
        """
        timeout_ms = self._validate_timeout(timeout_ms)
        request = ElectLeadersRequest(
            election_type=ElectionType(election_type),
            topic_partitions=self._get_topic_partitions(topic_partitions),
            timeout_ms=timeout_ms,
        )
        def response_errors(r):
            if r.API_VERSION >= 1:
                yield Errors.for_code(r.error_code)
            for result in r.replica_election_results:
                for partition in result.partition_result:
                    yield Errors.for_code(partition.error_code)
        ignore_errors = (Errors.ElectionNotNeededError,)
        return self._manager.run(self._send_request_to_controller, request, response_errors, raise_errors, ignore_errors)

    @staticmethod
    def _process_alter_partition_reassignments_input(reassignments):
        _Topic = AlterPartitionReassignmentsRequest.ReassignableTopic
        _Partition = _Topic.ReassignablePartition
        topic2partitions = defaultdict(list)
        for tp, replicas in reassignments.items():
            topic2partitions[tp.topic].append(_Partition(
                partition_index=tp.partition,
                replicas=list(replicas) if replicas is not None else None,
            ))
        return [_Topic(name=topic, partitions=parts) for topic, parts in topic2partitions.items()]

    def alter_partition_reassignments(self, reassignments, timeout_ms=None, raise_errors=True):
        """Alter the replica sets for the given partitions.

        Arguments:
            reassignments (dict): A dict mapping
                :class:`~kafka.TopicPartition` to a list of broker IDs for
                the new replica set, or ``None`` to cancel a pending
                reassignment for that partition.

        Keyword Arguments:
            timeout_ms (numeric, optional): The time in ms to wait for the
                request to complete.
            raise_errors (bool, optional): Whether to raise errors as
                exceptions. Default True.

        Returns:
            Decoded AlterPartitionReassignmentsResponse (as a dict).
        """
        timeout_ms = self._validate_timeout(timeout_ms)

        def response_errors(r):
            yield Errors.for_code(r.error_code)
            for topic in r.responses:
                for partition in topic.partitions:
                    yield Errors.for_code(partition.error_code)

        request = AlterPartitionReassignmentsRequest(
            timeout_ms=timeout_ms,
            topics=self._process_alter_partition_reassignments_input(reassignments),
        )
        response = self._manager.run(
            self._send_request_to_controller, request, response_errors, raise_errors)
        return response.to_dict()

    async def _async_list_partition_reassignments(self, topic_partitions=None, timeout_ms=None):
        timeout_ms = self._validate_timeout(timeout_ms)

        if topic_partitions is None:
            topics_field = None
        else:
            _Topic = ListPartitionReassignmentsRequest.ListPartitionReassignmentsTopics
            if isinstance(topic_partitions, dict):
                topics_field = [
                    _Topic(name=topic, partition_indexes=list(partitions))
                    for topic, partitions in topic_partitions.items()
                ]
            else:
                topic2partitions = defaultdict(list)
                for tp in topic_partitions:
                    topic2partitions[tp.topic].append(tp.partition)
                topics_field = [
                    _Topic(name=topic, partition_indexes=partitions)
                    for topic, partitions in topic2partitions.items()
                ]

        request = ListPartitionReassignmentsRequest(
            timeout_ms=timeout_ms,
            topics=topics_field,
        )
        response = await self._manager.send(request)

        top_level_error = Errors.for_code(response.error_code)
        if top_level_error is not Errors.NoError:
            raise top_level_error(
                "ListPartitionReassignmentsRequest failed: %s" % response.error_message)

        ret = {}
        for topic in response.topics:
            for partition in topic.partitions:
                ret[TopicPartition(topic.name, partition.partition_index)] = {
                    'replicas': list(partition.replicas),
                    'adding_replicas': list(partition.adding_replicas),
                    'removing_replicas': list(partition.removing_replicas),
                }
        return ret

    def list_partition_reassignments(self, topic_partitions=None, timeout_ms=None):
        """List the current ongoing partition reassignments.

        Arguments:
            topic_partitions (dict, list, optional):
                Either: a dict of ``{topic_name: [partition_ids]}``,
                or a list of :class:`~kafka.TopicPartition`,
                or ``None`` to list ongoing reassignments for all partitions.
                Default: None.

        Keyword Arguments:
            timeout_ms (numeric, optional): The time in ms to wait for the
                request to complete.

        Returns:
            dict: A dict mapping :class:`~kafka.TopicPartition` to a dict
            with keys ``'replicas'``, ``'adding_replicas'``, and
            ``'removing_replicas'`` (each a list of broker IDs).
        """
        return self._manager.run(
            self._async_list_partition_reassignments, topic_partitions, timeout_ms)

    async def _async_describe_topic_partitions(self, topics, response_partition_limit, cursor):
        _Topic = DescribeTopicPartitionsRequest.TopicRequest
        _Cursor = DescribeTopicPartitionsRequest.Cursor

        if cursor is not None:
            cursor_field = _Cursor(
                topic_name=cursor['topic_name'],
                partition_index=cursor['partition_index'],
            )
        else:
            cursor_field = None

        request = DescribeTopicPartitionsRequest(
            topics=[_Topic(name=t) for t in topics],
            response_partition_limit=response_partition_limit,
            cursor=cursor_field,
        )
        response = await self._manager.send(request)

        result = []
        for topic in response.topics:
            topic_dict = {
                'error_code': topic.error_code,
                'name': topic.name,
                'topic_id': topic.topic_id,
                'is_internal': topic.is_internal,
                'partitions': [
                    {
                        'error_code': p.error_code,
                        'partition_index': p.partition_index,
                        'leader_id': p.leader_id,
                        'leader_epoch': p.leader_epoch,
                        'replica_nodes': list(p.replica_nodes),
                        'isr_nodes': list(p.isr_nodes),
                        'eligible_leader_replicas': list(p.eligible_leader_replicas) if p.eligible_leader_replicas else None,
                        'last_known_elr': list(p.last_known_elr) if p.last_known_elr else None,
                        'offline_replicas': list(p.offline_replicas),
                    }
                    for p in topic.partitions
                ],
                'topic_authorized_operations': topic.topic_authorized_operations,
            }
            result.append(topic_dict)

        next_cursor = None
        if response.next_cursor is not None:
            next_cursor = {
                'topic_name': response.next_cursor.topic_name,
                'partition_index': response.next_cursor.partition_index,
            }

        return {'topics': result, 'next_cursor': next_cursor}

    def describe_topic_partitions(self, topics, response_partition_limit=2000, cursor=None):
        """Describe topics with fine-grained partition-level control (KIP-966).

        Unlike :meth:`describe_topics`, this uses the DescribeTopicPartitions
        API (apiKey 75, broker 3.7+) which supports pagination via a cursor
        and partition-level ELR (Eligible Leader Replicas) information.

        Arguments:
            topics ([str]): A list of topic names.

        Keyword Arguments:
            response_partition_limit (int, optional): Maximum number of
                partitions to include in the response. Default: 2000.
            cursor (dict, optional): Dict with ``'topic_name'`` and
                ``'partition_index'`` keys to start pagination from. Default:
                None.

        Returns:
            dict: ``{'topics': [...], 'next_cursor': None | {...}}``.
            ``topics`` is a list of dicts (one per topic) with keys
            ``error_code``, ``name``, ``topic_id``, ``is_internal``,
            ``partitions``, and ``topic_authorized_operations``.
            ``next_cursor`` is None if pagination is complete, otherwise a
            dict with the next page's ``topic_name`` and ``partition_index``.
        """
        return self._manager.run(
            self._async_describe_topic_partitions, topics, response_partition_limit, cursor)

    # -- List partition offsets --------------------------------------------

    @staticmethod
    def _list_partition_offsets_request(partition_timestamps, isolation_level_int):
        min_version = max(ListOffsetsRequest.min_version_for_isolation_level(isolation_level_int), 0)
        _Topic = ListOffsetsRequest.ListOffsetsTopic
        _Partition = _Topic.ListOffsetsPartition
        topic2partitions = defaultdict(list)
        for tp, ts in partition_timestamps.items():
            if not isinstance(ts, (int, OffsetSpec)):
                raise TypeError(f'Unsupported ts type {type(ts)}, expected int or OffsetSpec')
            elif int(ts) < 0:
                min_version = max(ListOffsetsRequest.min_version_for_timestamp(ts), min_version)
            topic2partitions[tp.topic].append(
                _Partition(partition_index=tp.partition, timestamp=ts))
        return ListOffsetsRequest(
            replica_id=-1,
            isolation_level=isolation_level_int,
            topics=[_Topic(name=name, partitions=parts)
                    for name, parts in topic2partitions.items()],
            min_version=min_version,
        )

    @staticmethod
    def _list_partition_offsets_process_response(response):
        results = {}
        for topic in response.topics:
            for partition in topic.partitions:
                tp = TopicPartition(topic.name, partition.partition_index)
                err = Errors.for_code(partition.error_code)
                if err is not Errors.NoError:
                    raise err(
                        "ListOffsetsRequest failed for %s: %s" % (tp, err.__name__))
                leader_epoch = getattr(partition, 'leader_epoch', -1)
                results[tp] = OffsetAndTimestamp(
                    offset=partition.offset,
                    timestamp=partition.timestamp,
                    leader_epoch=leader_epoch if leader_epoch >= 0 else None,
                )
        return results

    async def _async_list_partition_offsets(self, topic_partition_specs, isolation_level='read_uncommitted'):
        isolation_level = IsolationLevel.build_from(isolation_level)
        results = {}
        topic_partitions = set(topic_partition_specs.keys())
        while topic_partitions:
            leader2partitions = await self._async_get_leader_for_partitions(topic_partitions)

            for leader, partitions in leader2partitions.items():
                request = self._list_partition_offsets_request(
                    {tp: spec for tp, spec in topic_partition_specs.items() if tp in partitions},
                    isolation_level.value)
                try:
                    response = await self._manager.send(request, node_id=leader)
                    results.update(self._list_partition_offsets_process_response(response))
                    topic_partitions -= partitions
                except Errors.NotLeaderForPartitionError:
                    continue
        return results

    def list_partition_offsets(self, topic_partition_specs, isolation_level='read_uncommitted'):
        """Look up offsets for the given partitions by spec.

        Partitions are routed to their respective leader brokers via cluster
        metadata; one ``ListOffsetsRequest`` is sent per leader.

        Arguments:
            topic_partition_specs: dict mapping :class:`~kafka.TopicPartition` to
                :class:`OffsetSpec` (or a raw integer timestamp /
                wire-level sentinel).

        Keyword Arguments:
            isolation_level (str, optional): One of ``'read_uncommitted'``
                (default) or ``'read_committed'``. ``read_committed`` requires
                broker support for ListOffsets v2+.

        Returns:
            dict: A dict mapping :class:`~kafka.TopicPartition` to
            :class:`~kafka.structs.OffsetAndTimestamp`

        Raises:
            KafkaError: If any partition response carries an error code.
            UnknownTopicOrPartitionError: If a requested partition is not
                known to the cluster.
            UnsupportedVersionError: If the broker does not support a version
                of ListOffsetsRequest compatible with the requested specs.
        """
        return self._manager.run(
            self._async_list_partition_offsets, topic_partition_specs, isolation_level)


class NewPartitions:
    """DEPRECATED: A class for new partition creation on existing topics.

    Note that the length of new_assignments, if specified, must be the
    difference between the new total number of partitions and the existing
    number of partitions.

    Arguments:
        total_count (int): the total number of partitions that should exist
            on the topic
        new_assignments ([[int]]): an array of arrays of replica assignments
            for new partitions. If not set, broker assigns replicas per an
            internal algorithm.
    """
    def __init__(self, total_count, new_assignments=None):
        self.total_count = total_count
        self.new_assignments = new_assignments
