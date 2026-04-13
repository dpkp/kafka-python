"""Cluster metadata mixin for KafkaAdminClient."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from kafka.protocol.metadata import MetadataRequest

if TYPE_CHECKING:
    from kafka.net.manager import KafkaConnectionManager

log = logging.getLogger(__name__)


class MetadataAdminMixin:
    """Mixin providing cluster metadata methods for KafkaAdminClient."""
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

    def list_topics(self):
        """Retrieve a list of all topic names in the cluster.

        Returns:
            A list of topic name strings.
        """
        metadata = self._manager.run(self._get_cluster_metadata, None)
        return [t['name'] for t in metadata['topics']]

    def describe_topics(self, topics=None):
        """Fetch metadata for the specified topics or all topics if None.

        Keyword Arguments:
            topics ([str], optional) A list of topic names. If None, metadata for all
                topics is retrieved.

        Returns:
            A list of dicts describing each topic (including partition info).
        """
        metadata = self._manager.run(self._get_cluster_metadata, topics)
        return metadata['topics']

    def describe_cluster(self):
        """Fetch cluster-wide metadata such as the list of brokers, the controller ID,
        and the cluster ID.

        Returns:
            A dict with cluster-wide metadata, excluding topic details.
        """
        metadata = self._manager.run(self._get_cluster_metadata, [])
        metadata.pop('topics')
        return metadata
