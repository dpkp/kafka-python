"""Group management mixin for KafkaAdminClient."""

from __future__ import annotations

import itertools
import logging
from collections import defaultdict
from typing import TYPE_CHECKING

import kafka.errors as Errors
from kafka.admin._acls import valid_acl_operations
from kafka.protocol.admin import DeleteGroupsRequest, DescribeGroupsRequest, ListGroupsRequest
from kafka.protocol.consumer import (
    OffsetCommitRequest, OffsetDeleteRequest, OffsetFetchRequest,
)
from kafka.protocol.consumer.group import DEFAULT_GENERATION_ID, UNKNOWN_MEMBER_ID
from kafka.protocol.consumer.metadata import (
    ConsumerProtocolAssignment, ConsumerProtocolSubscription, ConsumerProtocolType,
)
from kafka.structs import OffsetAndMetadata, TopicPartition

if TYPE_CHECKING:
    from kafka.net.manager import KafkaConnectionManager

log = logging.getLogger(__name__)


class GroupAdminMixin:
    """Mixin providing consumer group management methods for KafkaAdminClient."""
    _manager: KafkaConnectionManager
    _coordinator_cache: dict
    config: dict

    # -- Describe groups ----------------------------------------------

    def _describe_groups_request(self, group_id):
        request = DescribeGroupsRequest(
            groups=[group_id],
            include_authorized_operations=True
        )
        return request

    def _describe_groups_process_response(self, response):
        """Process a DescribeGroupsResponse into a group description."""
        assert len(response.groups) == 1
        for group in response.groups:
            for member in group.members:
                member.member_metadata = ConsumerProtocolSubscription.decode(member.member_metadata)
                member.member_assignment = ConsumerProtocolAssignment.decode(member.member_assignment)
        # Return dict (key, val) tuples
        return [(group.group_id, self._process_acl_operations(group.to_dict())) for group in response.groups]

    async def _async_describe_groups(self, group_ids, group_coordinator_id=None):
        results = []
        for group_id in group_ids:
            coordinator_id = group_coordinator_id or await self._find_coordinator_id(group_id)
            request = self._describe_groups_request(group_id)
            response = await self._manager.send(request, node_id=coordinator_id)
            results.append(self._describe_groups_process_response(response))
        # Combine key/vals from multiple requests into single dict
        return dict(itertools.chain(*results))

    def describe_groups(self, group_ids, group_coordinator_id=None, include_authorized_operations=False):
        """Describe a set of consumer groups.

        Any errors are immediately raised.

        Arguments:
            group_ids: A list of consumer group IDs. These are typically the
                group names as strings.

        Keyword Arguments:
            group_coordinator_id (int, optional): The node_id of the groups' coordinator
                broker. If set to None, it will query the cluster for each group to
                find that group's coordinator. Explicitly specifying this can be
                useful for avoiding extra network round trips if you already know
                the group coordinator. This is only useful when all the group_ids
                have the same coordinator, otherwise it will error. Default: None.

        Returns:
            A dict of {group_id: {key: val}}. key/vals are simple to_dict translations
                of the raw results from DescribeGroupsResponse (with inline decoding
                of ConsumerSubscription and ConsumerAssignment metadata, and conversion
                of acl set ints to semantic enums).
        """
        return self._manager.run(self._async_describe_groups, group_ids, group_coordinator_id)

    # -- List groups --------------------------------------------------

    def _list_groups_request(self):
        # TODO: KIP-518: StatesFilter
        # TODO: KIP-848: TypesFilter
        return ListGroupsRequest()

    def _list_groups_process_response(self, response):
        """Process a ListGroupsResponse into a list of groups."""
        error_type = Errors.for_code(response.error_code)
        if error_type is not Errors.NoError:
            raise error_type(
                "ListGroupsRequest failed with response '{}'."
                .format(response))
        return [group.to_dict() for group in response.groups]

    async def _async_list_groups(self, broker_ids=None):
        if broker_ids is None:
            broker_ids = [broker.node_id for broker in self._manager.cluster.brokers()]
        groups = []
        for broker_id in broker_ids:
            request = self._list_groups_request()
            response = await self._manager.send(request, node_id=broker_id)
            groups.extend(self._list_groups_process_response(response))
        return groups

    def list_groups(self, broker_ids=None):
        """List all consumer groups known to the cluster.

        This returns a list of Group dicts. The tuples are
        composed of the consumer group name and the consumer group protocol
        type.

        Only consumer groups that store their offsets in Kafka are returned.
        The protocol type will be an empty string for groups created using
        Kafka < 0.9 APIs because, although they store their offsets in Kafka,
        they don't use Kafka for group coordination. For groups created using
        Kafka >= 0.9, the protocol type will typically be "consumer".

        As soon as any error is encountered, it is immediately raised.

        Keyword Arguments:
            broker_ids ([int], optional): A list of broker node_ids to query for consumer
                groups. If set to None, will query all brokers in the cluster.
                Explicitly specifying broker(s) can be useful for determining which
                consumer groups are coordinated by those broker(s). Default: None

        Returns:
            List of group data dicts, with key/vals from ListGroupsRequest
        """
        return self._manager.run(self._async_list_groups, broker_ids)

    # -- List group offsets -------------------------------------------

    def _list_group_offsets_request(self, group_id, partitions=None):
        _Topic = OffsetFetchRequest.OffsetFetchRequestTopic
        if partitions is None:
            min_version = 1
            topics = None
        else:
            min_version = 0
            topics_partitions_dict = defaultdict(set)
            for topic, partition in partitions:
                topics_partitions_dict[topic].add(partition)
            topics = [
                _Topic(name=name, partition_indexes=list(partitions))
                for name, partitions in topics_partitions_dict.items()
            ]
        return OffsetFetchRequest(group_id=group_id, topics=topics,
                                  min_version=min_version, max_version=6)

    def _list_group_offsets_process_response(self, response):
        """Process an OffsetFetchResponse."""
        if response.API_VERSION > 1:
            error_type = Errors.for_code(response.error_code)
            if error_type is not Errors.NoError:
                raise error_type(
                    "OffsetFetchResponse failed with response '{}'."
                    .format(response))
        def _partitions_to_dict(partitions):
            d = {}
            for p in partitions:
                d[p.partition_index] = p.to_dict()
                d[p.partition_index].pop('partition_index')
            return d
        return {topic.name: _partitions_to_dict(topic.partitions)
                for topic in response.topics}

    async def _async_list_group_offsets(self, group_id, group_coordinator_id=None, partitions=None):
        if group_coordinator_id is None:
            group_coordinator_id = await self._find_coordinator_id(group_id)
        request = self._list_group_offsets_request(group_id, partitions)
        response = await self._manager.send(request, node_id=group_coordinator_id)
        return self._list_group_offsets_process_response(response)

    def list_group_offsets(self, group_id, group_coordinator_id=None,
                                    partitions=None):
        """Fetch committed offsets for a single consumer group.

        Note:
        This does not verify that the group_id or partitions actually exist
        in the cluster.

        As soon as any error is encountered, it is immediately raised.

        Arguments:
            group_id (str): The consumer group id name for which to fetch offsets.

        Keyword Arguments:
            group_coordinator_id (int, optional): The node_id of the group's coordinator
                broker. If set to None, will query the cluster to find the group
                coordinator. Default: None.
            partitions: A list of TopicPartitions for which to fetch
                offsets. On brokers >= 0.10.2, this can be set to None to fetch all
                known offsets for the consumer group. Default: None.

        Returns:
            dict: {topic: [{partition data}]} key/vals from OffsetCommitResponse}]}
        """
        return self._manager.run(self._async_list_group_offsets, group_id, group_coordinator_id, partitions)

    # -- Delete groups ------------------------------------------------

    def _delete_groups_request(self, group_ids):
        return DeleteGroupsRequest(groups_names=group_ids)

    def _convert_delete_groups_response(self, response):
        """Parse a DeleteGroupsResponse."""
        results = []
        for group_id, error_code in response.results:
            res = 'OK' if error_code == 0 else Errors.for_code(error_code).__name__
            results.append((group_id, res))
        return results

    async def _async_delete_groups(self, group_ids, group_coordinator_id=None):
        coordinators_groups = defaultdict(list)
        if group_coordinator_id is not None:
            coordinators_groups[group_coordinator_id] = group_ids
        else:
            for group_id in group_ids:
                coordinator_id = await self._find_coordinator_id(group_id)
                coordinators_groups[coordinator_id].append(group_id)

        results = []
        for coordinator_id, coordinator_group_ids in coordinators_groups.items():
            request = self._delete_groups_request(coordinator_group_ids)
            response = await self._manager.send(request, node_id=coordinator_id)
            results.extend(self._convert_delete_groups_response(response))
        return dict(results)

    def delete_groups(self, group_ids, group_coordinator_id=None):
        """Delete Group Offsets for given consumer groups.

        Note:
        This does not verify that the group ids actually exist and
        group_coordinator_id is the correct coordinator for all these groups.

        The result needs checking for potential errors.

        Arguments:
            group_ids ([str]): The consumer group ids of the groups which are to be deleted.

        Keyword Arguments:
            group_coordinator_id (int, optional): The node_id of the broker which is
                the coordinator for all the groups. Default: None.

        Returns:
            A list of tuples (group_id, KafkaError)
        """
        return self._manager.run(self._async_delete_groups, group_ids, group_coordinator_id)

    # -- Alter group offsets -----------------------------------------------

    @staticmethod
    def _alter_group_offsets_request(group_id, offsets):
        _Topic = OffsetCommitRequest.OffsetCommitRequestTopic
        _Partition = _Topic.OffsetCommitRequestPartition
        topic2partitions = defaultdict(list)
        for tp, oam in offsets.items():
            topic2partitions[tp.topic].append(_Partition(
                partition_index=tp.partition,
                committed_offset=oam.offset,
                committed_leader_epoch=-1 if oam.leader_epoch is None else oam.leader_epoch,
                committed_metadata=oam.metadata,
            ))
        return OffsetCommitRequest(
            group_id=group_id,
            generation_id_or_member_epoch=DEFAULT_GENERATION_ID,
            member_id=UNKNOWN_MEMBER_ID,
            group_instance_id=None,
            retention_time_ms=-1,
            topics=[_Topic(name=name, partitions=parts)
                    for name, parts in topic2partitions.items()],
        )

    @staticmethod
    def _alter_group_offsets_process_response(response):
        results = {}
        for topic in response.topics:
            for partition in topic.partitions:
                results[TopicPartition(topic.name, partition.partition_index)] = \
                    Errors.for_code(partition.error_code)
        return results

    async def _async_alter_group_offsets(self, group_id, offsets, group_coordinator_id=None):
        if not offsets:
            return {}
        if group_coordinator_id is None:
            group_coordinator_id = await self._find_coordinator_id(group_id)
        request = self._alter_group_offsets_request(group_id, offsets)
        response = await self._manager.send(request, node_id=group_coordinator_id)
        return self._alter_group_offsets_process_response(response)

    def alter_group_offsets(self, group_id, offsets, group_coordinator_id=None):
        """Alter committed offsets for a consumer group.

        The group must have no active members (i.e. be empty or dead) for
        the commit to succeed; otherwise individual partitions may return
        ``UNKNOWN_MEMBER_ID`` or similar errors.

        Arguments:
            group_id (str): The consumer group id.
            offsets (dict): A dict mapping :class:`~kafka.TopicPartition` to
                :class:`~kafka.structs.OffsetAndMetadata`.

        Keyword Arguments:
            group_coordinator_id (int, optional): The node_id of the group's
                coordinator broker. If None, the cluster will be queried to
                locate the coordinator. Default: None.

        Returns:
            dict: A dict mapping :class:`~kafka.TopicPartition` to the
            partition-level :class:`~kafka.errors.KafkaError` class
            (``NoError`` on success).
        """
        return self._manager.run(
            self._async_alter_group_offsets, group_id, offsets, group_coordinator_id)

    # -- Delete group offsets ----------------------------------------------

    @staticmethod
    def _delete_group_offsets_request(group_id, partitions):
        _Topic = OffsetDeleteRequest.OffsetDeleteRequestTopic
        _Partition = _Topic.OffsetDeleteRequestPartition
        topic2partitions = defaultdict(list)
        for tp in partitions:
            topic2partitions[tp.topic].append(
                _Partition(partition_index=tp.partition))
        return OffsetDeleteRequest(
            group_id=group_id,
            topics=[_Topic(name=name, partitions=parts)
                    for name, parts in topic2partitions.items()],
        )

    @staticmethod
    def _delete_group_offsets_process_response(response):
        top_level = Errors.for_code(response.error_code)
        if top_level is not Errors.NoError:
            raise top_level(
                "OffsetDeleteRequest failed with response '{}'.".format(response))
        results = {}
        for topic in response.topics:
            for partition in topic.partitions:
                results[TopicPartition(topic.name, partition.partition_index)] = \
                    Errors.for_code(partition.error_code)
        return results

    async def _async_delete_group_offsets(self, group_id, partitions, group_coordinator_id=None):
        if not partitions:
            return {}
        if group_coordinator_id is None:
            group_coordinator_id = await self._find_coordinator_id(group_id)
        request = self._delete_group_offsets_request(group_id, partitions)
        response = await self._manager.send(request, node_id=group_coordinator_id)
        return self._delete_group_offsets_process_response(response)

    def delete_group_offsets(self, group_id, partitions, group_coordinator_id=None):
        """Delete committed offsets for a consumer group.

        The group must have no active members subscribed to the given topics;
        otherwise partitions may fail with ``GROUP_SUBSCRIBED_TO_TOPIC``.

        Arguments:
            group_id (str): The consumer group id.
            partitions: An iterable of :class:`~kafka.TopicPartition` whose
                committed offsets should be deleted.

        Keyword Arguments:
            group_coordinator_id (int, optional): The node_id of the group's
                coordinator broker. If None, the cluster will be queried to
                locate the coordinator. Default: None.

        Returns:
            dict: A dict mapping :class:`~kafka.TopicPartition` to the
            partition-level :class:`~kafka.errors.KafkaError` class
            (``NoError`` on success).

        Raises:
            KafkaError: If the response contains a top-level error (e.g.
                ``GroupIdNotFoundError``, ``NonEmptyGroupError``).
        """
        return self._manager.run(
            self._async_delete_group_offsets, group_id, partitions, group_coordinator_id)
