"""Cluster metadata mixin for KafkaAdminClient."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from kafka.protocol.metadata import MetadataRequest
from kafka.protocol.admin import DescribeLogDirsRequest

if TYPE_CHECKING:
    from kafka.net.manager import KafkaConnectionManager

log = logging.getLogger(__name__)


class ClusterAdminMixin:
    """Mixin providing cluster management methods for KafkaAdminClient."""
    _manager: KafkaConnectionManager

    async def _get_cluster_metadata(self, topics):
        """topics = [] for no topics, None for all."""
        request = MetadataRequest(
            topics=[
                MetadataRequest.MetadataRequestTopic(name=topic)
                for topic in topics] if topics is not None else None,
            allow_auto_topic_creation=False,
            include_cluster_authorized_operations=True,
            include_topic_authorized_operations=True,
        )
        response = await self._manager.send(request)
        metadata = response.to_dict()
        self._process_acl_operations(metadata)
        for topic in metadata['topics']:
            self._process_acl_operations(topic)
        return metadata

    def describe_cluster(self):
        """Fetch cluster-wide metadata such as the list of brokers, the controller ID,
        and the cluster ID.

        Returns:
            A dict with cluster-wide metadata, excluding topic details.
        """
        metadata = self._manager.run(self._get_cluster_metadata, [])
        metadata.pop('topics')
        metadata.pop('throttle_time_ms', None)
        return metadata

    async def _async_describe_log_dirs(self, topic_partitions=(), brokers=None):
        request = DescribeLogDirsRequest(topics=topic_partitions)
        responses = []
        if brokers is None:
            brokers = [broker.node_id for broker in self._manager.cluster.brokers()]
        for node_id in brokers:
            response = await self._manager.send(request, node_id=node_id)
            responses.append({"broker": node_id, "log_dirs": [result.to_dict() for result in response.results]})
        return responses

    def describe_log_dirs(self, topic_partitions=None, brokers=None):
        """Fetch broker log directory and topic/partition stats

        Keyword Arguments:
            topic_partitions (dict, list, optional):
                Either: dict of {topic_name: [partition ids]}.
                Or:     list of [topic_name], to query all partitions for topic.
                Or:     None, to query all topics / all partitions.
                Default: None
            brokers (list, optional): List of [node_id] for brokers to query.
                If None, query is sent to all brokers. Default: None

        Returns:
            list of dicts, containing per-broker log-dir data
        """
        topic_partitions = self._get_topic_partitions(topic_partitions)
        return self._manager.run(self._async_describe_log_dirs, topic_partitions, brokers)
