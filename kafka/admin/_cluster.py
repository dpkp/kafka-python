"""Cluster metadata mixin for KafkaAdminClient."""

from __future__ import annotations

from enum import IntEnum
import logging
from typing import TYPE_CHECKING

import kafka.errors as Errors
from kafka.protocol.api_key import ApiKey
from kafka.protocol.metadata import ApiVersionsRequest, MetadataRequest
from kafka.protocol.admin import DescribeLogDirsRequest, UpdateFeaturesRequest
from kafka.util import EnumHelper

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

    async def _async_get_broker_version_data(self, broker_id):
        conn = await self._manager.get_connection(broker_id)
        return conn.broker_version_data

    def get_broker_version_data(self, broker_id):
        """Return BrokerVersionData for a specific broker"""
        return self._manager.run(self._async_get_broker_version_data, broker_id)

    def api_versions(self):
        api_versions = self._manager.broker_version_data.api_versions
        return {ApiKey(k): v for k, v in api_versions.items()}

    async def _async_describe_features(self, send_request_to_controller=False):
        request = ApiVersionsRequest(
            client_software_name=self._manager.config['client_software_name'],
            client_software_version=self._manager.config['client_software_version'],
            min_version=3,
        )
        if send_request_to_controller:
            response = await self._send_request_to_controller(request)
        else:
            response = await self._manager.send(request)
        error_type = Errors.for_code(response.error_code)
        if error_type is not Errors.NoError:
            raise error_type(f"ApiVersionsRequest failed: {response}")
        supported = {feature.name: (feature.min_version, feature.max_version)
                     for feature in (response.supported_features or [])}
        finalized = {feature.name: (feature.min_version_level, feature.max_version_level)
                     for feature in (response.finalized_features or [])}
        epoch = response.finalized_features_epoch
        if epoch is None or epoch < 0:
            epoch = None
        return {
            'supported_features': supported,
            'finalized_features': finalized,
            'finalized_features_epoch': epoch,
        }

    def describe_features(self, send_request_to_controller=False):
        """Fetch the cluster's supported and finalized feature flags.

        Features are broker-level capabilities (e.g. ``metadata.version``)
        that can be finalized cluster-wide via ``update_features`` (KIP-584).
        Requires broker >= 2.4.

        Keyword Arguments:
            send_request_to_controller (bool, optional): If True, route the
                request to the active controller. By default the request is
                sent to any available broker. Default: False.

        Returns:
            dict with keys:
                - ``supported_features``: dict of
                  ``{feature_name: (min_version, max_version)}``
                - ``finalized_features``: dict of
                  ``{feature_name: (min_version_level, max_version_level)}``
                - ``finalized_features_epoch``: int, or None if unknown
                  (broker did not report an epoch, or reported -1)
        """
        return self._manager.run(self._async_describe_features, send_request_to_controller)

    @staticmethod
    def _build_feature_updates(feature_updates):
        if not isinstance(feature_updates, dict):
            raise TypeError('feature_updates must be a dict of '
                            '{feature_name: (max_version_level, upgrade_type)} '
                            'or {feature_name: max_version_level}')
        _FeatureUpdateKey = UpdateFeaturesRequest.FeatureUpdateKey
        updates = []
        for feature, spec in feature_updates.items():
            if isinstance(spec, tuple):
                upgrade_type, max_version_level = spec
            else:
                upgrade_type = UpdateFeatureType.UPGRADE
                max_version_level = spec
            upgrade_code = UpdateFeatureType.value_for(upgrade_type)
            downgrade = upgrade_code in (
                UpdateFeatureType.SAFE_DOWNGRADE.value,
                UpdateFeatureType.UNSAFE_DOWNGRADE.value)
            updates.append(_FeatureUpdateKey(
                feature=feature,
                max_version_level=int(max_version_level),
                allow_downgrade=downgrade,
                upgrade_type=upgrade_code))
        return updates

    async def _async_update_features(self, feature_updates, validate_only=False, timeout_ms=60000):
        min_version = 1 if validate_only else 0
        request = UpdateFeaturesRequest(
            timeout_ms=timeout_ms,
            feature_updates=self._build_feature_updates(feature_updates),
            validate_only=validate_only,
            min_version=min_version,
        )
        response = await self._send_request_to_controller(
            request,
            get_errors_fn=lambda r: [Errors.for_code(r.error_code)],
        )
        ret = {}
        for result in response.results or []:
            if result.error_code == 0:
                ret[result.feature] = 'OK'
            else:
                ret[result.feature] = str(Errors.for_code(result.error_code)(result.error_message))
        # v2+ responses omit per-feature results; top-level error is already
        # raised by _send_request_to_controller, so any feature we asked about
        # succeeded.
        for feature in feature_updates:
            ret.setdefault(feature, 'OK')
        return ret

    def update_features(self, feature_updates, validate_only=False, timeout_ms=60000):
        """Update cluster-wide finalized feature flags.

        Finalize cluster-wide feature capabilities (e.g. ``metadata.version``).
        The request is always routed to the active controller. See KIP-584.
        Requires broker >= 2.7.

        Arguments:
            feature_updates: A dict of
                ``{feature_name: (upgrade_type, max_version_level)}`` or
                ``{feature_name: max_version_level}`` (implicit UPGRADE).
                ``upgrade_type`` may be a :class:`UpdateFeatureType`,
                its name, or int value. A ``max_version_level < 1`` requests
                deletion of the finalized feature.

        Keyword Arguments:
            validate_only (bool, optional): If True, validate the request but
                do not apply it. Default: False.
            timeout_ms (int, optional): Broker-side timeout in milliseconds.
                Default: 60000.

        Returns:
            dict of {feature_name: 'OK' | error message}
        """
        return self._manager.run(self._async_update_features,
                                 feature_updates, validate_only, timeout_ms)


class UpdateFeatureType(EnumHelper, IntEnum):
    UNKNOWN = 0
    UPGRADE = 1
    SAFE_DOWNGRADE = 2
    UNSAFE_DOWNGRADE = 3
