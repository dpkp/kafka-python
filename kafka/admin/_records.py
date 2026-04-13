"""Record deletion and cluster operation mixin for KafkaAdminClient."""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import TYPE_CHECKING

import kafka.errors as Errors
from kafka.errors import UnknownTopicOrPartitionError
from kafka.protocol.admin import DeleteRecordsRequest, DescribeLogDirsRequest, ElectLeadersRequest, ElectionType
from kafka.structs import TopicPartition

if TYPE_CHECKING:
    from kafka.net.manager import KafkaConnectionManager

log = logging.getLogger(__name__)


class RecordAdminMixin:
    """Mixin providing record deletion and cluster operation methods."""
    _manager: KafkaConnectionManager
    _client: object
    config: dict

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
        version = self._client.api_version(DeleteRecordsRequest, max_version=0)

        if partition_leader_id is None:
            leader2partitions = await self._async_get_leader_for_partitions(set(records_to_delete))
        else:
            leader2partitions = {partition_leader_id: set(records_to_delete)}

        responses = []
        for leader, partitions in leader2partitions.items():
            topic2partitions = defaultdict(list)
            for partition in partitions:
                topic2partitions[partition.topic].append(partition)

            request = DeleteRecordsRequest[version](
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

    @staticmethod
    def _convert_topic_partitions(topic_partitions):
        return [
            (
                topic,
                partitions
            )
            for topic, partitions in topic_partitions.items()
        ]

    def _get_all_topic_partitions(self):
        return [
            (
                topic['name'],
                [p['partition_index'] for p in topic['partitions']]
            )
            for topic in self.describe_topics()
        ]

    def _get_topic_partitions(self, topic_partitions):
        if topic_partitions is None:
            return self._get_all_topic_partitions()
        return self._convert_topic_partitions(topic_partitions)

    def perform_leader_election(self, election_type, topic_partitions=None, timeout_ms=None, raise_errors=True):
        """Trigger leader election for the specified topic partitions.

        Arguments:
            election_type: Type of election to attempt. 0 for Preferred, 1 for Unclean

        Keyword Arguments:
            topic_partitions (dict): A map of topic name strings to partition ids list.
                By default, will run on all topic partitions
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

    async def _async_describe_log_dirs(self, topic_partitions=(), brokers=None):
        request = DescribeLogDirsRequest(topics=topic_partitions)
        responses = []
        if brokers is None:
            brokers = [broker.node_id for broker in self._manager.cluster.brokers()]
        for node_id in brokers:
            response = await self._manager.send(request, node_id=node_id)
            responses.append((node_id, response.to_dict()))
        for node_id, result in responses:
            result['broker'] = self._manager.cluster.broker_metadata(node_id).to_dict()
        return dict(responses)

    def describe_log_dirs(self, topic_partitions=None, brokers=None):
        """Send a DescribeLogDirsRequest request to a broker.

        Keyword Arguments:
            topic_partitions (dict, list, optional):
                Either: dict of {topic_name: [partition ids]}.
                Or:     None, to query all topics / all partitions.
                Default: None
            brokers (list, optional): List of [node_id] for brokers to query.
                If None, query is sent to all brokers. Default: None

        Returns:
            DescribeLogDirsResponse object
        """
        topic_partitions = self._get_topic_partitions(topic_partitions)
        return self._manager.run(self._async_describe_log_dirs, topic_partitions, brokers)
