"""Consumer group management mixin for KafkaAdminClient."""

from __future__ import annotations

import itertools
import logging
from collections import defaultdict
from typing import TYPE_CHECKING

import kafka.errors as Errors
from kafka.admin._acls import valid_acl_operations
from kafka.protocol.admin import DeleteGroupsRequest, DescribeGroupsRequest, ListGroupsRequest
from kafka.protocol.consumer import OffsetFetchRequest
from kafka.protocol.consumer.metadata import (
    ConsumerProtocolAssignment, ConsumerProtocolSubscription, ConsumerProtocolType,
)
from kafka.structs import GroupInformation, MemberInformation, OffsetAndMetadata, TopicPartition

if TYPE_CHECKING:
    from kafka.net.manager import KafkaConnectionManager

log = logging.getLogger(__name__)


class GroupAdminMixin:
    """Mixin providing consumer group management methods for KafkaAdminClient."""
    _manager: KafkaConnectionManager
    _client: object
    _coordinator_cache: dict
    config: dict

    # -- Describe consumer groups ----------------------------------------------

    def _describe_consumer_groups_request(self, group_id):
        version = self._client.api_version(DescribeGroupsRequest, max_version=3)
        if version <= 2:
            request = DescribeGroupsRequest[version](groups=(group_id,))
        else:
            request = DescribeGroupsRequest[version](
                groups=(group_id,),
                include_authorized_operations=True
            )
        return request

    def _describe_consumer_groups_process_response(self, response):
        """Process a DescribeGroupsResponse into a group description."""
        if response.API_VERSION > 3:
            raise NotImplementedError(
                "Support for DescribeGroupsResponse_v{} has not yet been added to KafkaAdminClient."
                .format(response.API_VERSION))

        assert len(response.groups) == 1
        for response_name, response_field in response.fields.items():
            if response_name == 'groups':
                described_group = getattr(response, response_name)[0]
                described_group_information_list = []
                protocol_type_is_consumer = False
                for group_information_name, group_information_field in response_field.fields.items():
                    if not group_information_field.for_version_q(response.API_VERSION):
                        continue
                    described_group_information = getattr(described_group, group_information_name)
                    if group_information_name == 'protocol_type':
                        protocol_type = described_group_information
                        protocol_type_is_consumer = (protocol_type == ConsumerProtocolType or not protocol_type)
                    if group_information_name == 'members':
                        member_information_list = []
                        for member in described_group_information:
                            member_information = []
                            for attr_name, attr_field in group_information_field.fields.items():
                                if not attr_field.for_version_q(response.API_VERSION):
                                    continue
                                attr_val = getattr(member, attr_name)
                                if protocol_type_is_consumer:
                                    if attr_name == 'member_metadata' and attr_val:
                                        member_information.append(ConsumerProtocolSubscription.decode(attr_val))
                                    elif attr_name == 'member_assignment' and attr_val:
                                        member_information.append(ConsumerProtocolAssignment.decode(attr_val))
                                    else:
                                        member_information.append(attr_val)
                            member_info_tuple = MemberInformation._make(member_information)
                            member_information_list.append(member_info_tuple)
                        described_group_information_list.append(member_information_list)
                    else:
                        described_group_information_list.append(described_group_information)
                if response.API_VERSION >= 3:
                    described_group_information_list[-1] = list(map(lambda acl: acl.name, valid_acl_operations(described_group_information_list[-1])))
                else:
                    described_group_information_list.append([])
                group_description = GroupInformation._make(described_group_information_list)
                error_code = group_description.error_code
                error_type = Errors.for_code(error_code)
                if error_type is not Errors.NoError:
                    raise error_type(
                        "DescribeGroupsResponse failed with response '{}'."
                        .format(response))
                return group_description
        assert False, "DescribeGroupsResponse parsing failed"

    async def _async_describe_consumer_groups(self, group_ids, group_coordinator_id=None):
        results = []
        for group_id in group_ids:
            coordinator_id = group_coordinator_id or await self._find_coordinator_id(group_id)
            request = self._describe_consumer_groups_request(group_id)
            response = await self._manager.send(request, node_id=coordinator_id)
            results.append(self._describe_consumer_groups_process_response(response))
        return results

    def describe_consumer_groups(self, group_ids, group_coordinator_id=None, include_authorized_operations=False):
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
            A list of group descriptions. For now the group descriptions
            are the raw results from the DescribeGroupsResponse. Long-term, we
            plan to change this to return namedtuples as well as decoding the
            partition assignments.
        """
        return self._manager.run(self._async_describe_consumer_groups, group_ids, group_coordinator_id)

    # -- List consumer groups --------------------------------------------------

    def _list_consumer_groups_request(self):
        version = self._client.api_version(ListGroupsRequest, max_version=2)
        return ListGroupsRequest[version]()

    def _list_consumer_groups_process_response(self, response):
        """Process a ListGroupsResponse into a list of groups."""
        if response.API_VERSION <= 2:
            error_type = Errors.for_code(response.error_code)
            if error_type is not Errors.NoError:
                raise error_type(
                    "ListGroupsRequest failed with response '{}'."
                    .format(response))
        else:
            raise NotImplementedError(
                "Support for ListGroupsResponse_v{} has not yet been added to KafkaAdminClient."
                .format(response.API_VERSION))
        return [(group.group_id, group.protocol_type) for group in response.groups]

    async def _async_list_consumer_groups(self, broker_ids=None):
        if broker_ids is None:
            broker_ids = [broker.node_id for broker in self._manager.cluster.brokers()]
        consumer_groups = set()
        for broker_id in broker_ids:
            request = self._list_consumer_groups_request()
            response = await self._manager.send(request, node_id=broker_id)
            consumer_groups.update(self._list_consumer_groups_process_response(response))
        return list(consumer_groups)

    def list_consumer_groups(self, broker_ids=None):
        """List all consumer groups known to the cluster.

        This returns a list of Consumer Group tuples. The tuples are
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
            list: List of tuples of Consumer Groups.
        """
        return self._manager.run(self._async_list_consumer_groups, broker_ids)

    # -- List consumer group offsets -------------------------------------------

    def _list_consumer_group_offsets_request(self, group_id, partitions=None):
        version = self._client.api_version(OffsetFetchRequest, max_version=5)
        if partitions is None:
            if version <= 1:
                raise ValueError(
                    """OffsetFetchRequest_v{} requires specifying the
                    partitions for which to fetch offsets. Omitting the
                    partitions is only supported on brokers >= 0.10.2.
                    For details, see KIP-88.""".format(version))
            topics_partitions = None
        else:
            topics_partitions_dict = defaultdict(set)
            for topic, partition in partitions:
                topics_partitions_dict[topic].add(partition)
            topics_partitions = list(topics_partitions_dict.items())
        return OffsetFetchRequest[version](group_id, topics_partitions)

    def _list_consumer_group_offsets_process_response(self, response):
        """Process an OffsetFetchResponse."""
        if response.API_VERSION <= 5:
            if response.API_VERSION > 1:
                error_type = Errors.for_code(response.error_code)
                if error_type is not Errors.NoError:
                    raise error_type(
                        "OffsetFetchResponse failed with response '{}'."
                        .format(response))
            offsets = {}
            for topic, partitions in response.topics:
                for partition_data in partitions:
                    if response.API_VERSION <= 4:
                        partition, offset, metadata, error_code = partition_data
                        leader_epoch = -1
                    else:
                        partition, offset, leader_epoch, metadata, error_code = partition_data
                    error_type = Errors.for_code(error_code)
                    if error_type is not Errors.NoError:
                        raise error_type(
                            "Unable to fetch consumer group offsets for topic {}, partition {}"
                            .format(topic, partition))
                    offsets[TopicPartition(topic, partition)] = OffsetAndMetadata(offset, metadata, leader_epoch)
        else:
            raise NotImplementedError(
                "Support for OffsetFetchResponse_v{} has not yet been added to KafkaAdminClient."
                .format(response.API_VERSION))
        return offsets

    async def _async_list_consumer_group_offsets(self, group_id, group_coordinator_id=None, partitions=None):
        if group_coordinator_id is None:
            group_coordinator_id = await self._find_coordinator_id(group_id)
        request = self._list_consumer_group_offsets_request(group_id, partitions)
        response = await self._manager.send(request, node_id=group_coordinator_id)
        return self._list_consumer_group_offsets_process_response(response)

    def list_consumer_group_offsets(self, group_id, group_coordinator_id=None,
                                    partitions=None):
        """Fetch Consumer Offsets for a single consumer group.

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
            dictionary: A dictionary with TopicPartition keys and
            OffsetAndMetadata values.
        """
        return self._manager.run(self._async_list_consumer_group_offsets, group_id, group_coordinator_id, partitions)

    # -- Delete consumer groups ------------------------------------------------

    def _delete_consumer_groups_request(self, group_ids):
        version = self._client.api_version(DeleteGroupsRequest, max_version=1)
        return DeleteGroupsRequest[version](group_ids)

    def _convert_delete_groups_response(self, response):
        """Parse a DeleteGroupsResponse."""
        if response.API_VERSION <= 1:
            results = []
            for group_id, error_code in response.results:
                results.append((group_id, Errors.for_code(error_code)))
            return results
        else:
            raise NotImplementedError(
                "Support for DeleteGroupsResponse_v{} has not yet been added to KafkaAdminClient."
                    .format(response.API_VERSION))

    async def _async_delete_consumer_groups(self, group_ids, group_coordinator_id=None):
        coordinators_groups = defaultdict(list)
        if group_coordinator_id is not None:
            coordinators_groups[group_coordinator_id] = group_ids
        else:
            for group_id in group_ids:
                coordinator_id = await self._find_coordinator_id(group_id)
                coordinators_groups[coordinator_id].append(group_id)

        results = []
        for coordinator_id, coordinator_group_ids in coordinators_groups.items():
            request = self._delete_consumer_groups_request(coordinator_group_ids)
            response = await self._manager.send(request, node_id=coordinator_id)
            results.extend(self._convert_delete_groups_response(response))
        return results

    def delete_consumer_groups(self, group_ids, group_coordinator_id=None):
        """Delete Consumer Group Offsets for given consumer groups.

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
        return self._manager.run(self._async_delete_consumer_groups, group_ids, group_coordinator_id)
