"""Partition management mixin for KafkaAdminClient.

Also defines NewPartitions data class.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import TYPE_CHECKING

import kafka.errors as Errors
from kafka.errors import UnknownTopicOrPartitionError
from kafka.protocol.admin import DeleteRecordsRequest, ElectLeadersRequest, ElectionType
from kafka.structs import TopicPartition

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

        leader2partitions = defaultdict(list)
        valid_partitions = set()
        for topic in metadata.get("topics", ()):
            for partition in topic.get("partitions", ()):
                t2p = TopicPartition(topic=topic["name"], partition=partition["partition_index"])
                if t2p in partitions:
                    leader2partitions[partition["leader_id"]].append(t2p)
                    valid_partitions.add(t2p)

        if len(partitions) != len(valid_partitions):
            unknown = set(partitions) - valid_partitions
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

    def perform_leader_election(self, election_type, topic_partitions=None, timeout_ms=None, raise_errors=True):
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
