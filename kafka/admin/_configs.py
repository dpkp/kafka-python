"""Configuration management mixin for KafkaAdminClient.

Also defines ConfigResource and ConfigResourceType data classes.
"""

from __future__ import annotations

import logging
from enum import IntEnum
from typing import TYPE_CHECKING

from kafka.errors import IncompatibleBrokerVersion
from kafka.protocol.admin import AlterConfigsRequest, DescribeConfigsRequest

if TYPE_CHECKING:
    from kafka.net.manager import KafkaConnectionManager

log = logging.getLogger(__name__)


class ConfigAdminMixin:
    """Mixin providing configuration management methods for KafkaAdminClient."""
    _manager: KafkaConnectionManager
    _client: object
    config: dict

    @staticmethod
    def _convert_describe_config_resource_request(config_resource):
        return (
            config_resource.resource_type,
            config_resource.name,
            [config_key for config_key, config_value in config_resource.configs.items()] if config_resource.configs else None
        )

    async def _async_describe_configs(self, config_resources, include_synonyms=False):
        broker_resources = []
        topic_resources = []

        for config_resource in config_resources:
            if config_resource.resource_type == ConfigResourceType.BROKER:
                broker_resources.append(self._convert_describe_config_resource_request(config_resource))
            else:
                topic_resources.append(self._convert_describe_config_resource_request(config_resource))

        version = self._client.api_version(DescribeConfigsRequest, max_version=2)
        if include_synonyms and version == 0:
            raise IncompatibleBrokerVersion(
                "include_synonyms requires DescribeConfigsRequest >= v1, which is not supported by Kafka {}."
                    .format(self._manager.broker_version))

        results = []
        for broker_resource in broker_resources:
            try:
                broker_id = int(broker_resource[1])
            except ValueError:
                raise ValueError("Broker resource names must be an integer or a string represented integer")
            if version == 0:
                request = DescribeConfigsRequest[version](resources=[broker_resource])
            else:
                request = DescribeConfigsRequest[version](
                    resources=[broker_resource],
                    include_synonyms=include_synonyms)
            results.append(await self._manager.send(request, node_id=broker_id))

        if topic_resources:
            if version == 0:
                request = DescribeConfigsRequest[version](resources=topic_resources)
            else:
                request = DescribeConfigsRequest[version](resources=topic_resources, include_synonyms=include_synonyms)
            results.append(await self._manager.send(request))

        return results

    def describe_configs(self, config_resources, include_synonyms=False):
        """Fetch configuration parameters for one or more Kafka resources.

        Arguments:
            config_resources: An list of ConfigResource objects.
                Any keys in ConfigResource.configs dict will be used to filter the
                result. Setting the configs dict to None will get all values. An
                empty dict will get zero values (as per Kafka protocol).

        Keyword Arguments:
            include_synonyms (bool, optional): If True, return synonyms in response. Not
                supported by all versions. Default: False.

        Returns:
            List of DescribeConfigsResponses.
        """
        return self._manager.run(self._async_describe_configs, config_resources, include_synonyms)

    @staticmethod
    def _convert_alter_config_resource_request(config_resource):
        return (
            config_resource.resource_type,
            config_resource.name,
            [
                (config_key, config_value) for config_key, config_value in config_resource.configs.items()
            ]
        )

    async def _async_alter_configs(self, config_resources):
        version = self._client.api_version(AlterConfigsRequest, max_version=1)
        request = AlterConfigsRequest[version](
            resources=[self._convert_alter_config_resource_request(config_resource) for config_resource in config_resources]
        )
        # TODO: BROKER resources should be sent to the specific broker
        return await self._manager.send(request)

    def alter_configs(self, config_resources):
        """Alter configuration parameters of one or more Kafka resources.

        Warning:
            This is currently broken for BROKER resources because those must be
            sent to that specific broker, versus this always picks the
            least-loaded node.

        Arguments:
            config_resources: A list of ConfigResource objects.

        Returns:
            Appropriate version of AlterConfigsResponse class.
        """
        return self._manager.run(self._async_alter_configs, config_resources)


class ConfigResourceType(IntEnum):
    """An enumerated type of config resources"""
    BROKER = 4
    TOPIC = 2


class ConfigResource:
    """A class for specifying config resources.

    Arguments:
        resource_type (ConfigResourceType): the type of kafka resource
        name (string): The name of the kafka resource
        configs ({key : value}): A maps of config keys to values.
    """
    def __init__(self, resource_type, name, configs=None):
        if not isinstance(resource_type, ConfigResourceType):
            resource_type = ConfigResourceType[str(resource_type).upper()]  # pylint: disable-msg=unsubscriptable-object
        self.resource_type = resource_type
        self.name = name
        self.configs = configs

    def __str__(self):
        return "ConfigResource %s=%s" % (self.resource_type, self.name)

    def __repr__(self):
        return "ConfigResource(%s, %s, %s)" % (self.resource_type, self.name, self.configs)
