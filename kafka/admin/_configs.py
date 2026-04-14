"""Configuration management mixin for KafkaAdminClient.

Also defines ConfigResource and ConfigResourceType data classes.
"""

from __future__ import annotations

from collections import defaultdict
from enum import IntEnum
import logging
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
            list(config_resource.configs.keys()) if isinstance(config_resource.configs, dict) else config_resource.configs
        )

    async def _async_describe_configs(self, config_resources, include_synonyms=False):
        broker_resources = defaultdict(list)
        other_resources = []

        for config_resource in config_resources:
            if config_resource.resource_type in (ConfigResourceType.BROKER, ConfigResourceType.BROKER_LOGGER):
                try:
                    broker_id = int(config_resource.name)
                except ValueError:
                    raise ValueError("Broker resource names must be an integer or a string represented integer")
                broker_resources[broker_id].append(self._convert_describe_config_resource_request(config_resource))
            else:
                other_resources.append(self._convert_describe_config_resource_request(config_resource))

        version = self._client.api_version(DescribeConfigsRequest, max_version=2)
        if include_synonyms and version == 0:
            raise IncompatibleBrokerVersion(
                "include_synonyms requires DescribeConfigsRequest >= v1, which is not supported by Kafka {}."
                    .format(self._manager.broker_version))

        responses = []
        for broker_id, resources in broker_resources.items():
            request = DescribeConfigsRequest(
                resources=resources,
                include_synonyms=include_synonyms)
            responses.append(await self._manager.send(request, node_id=broker_id))
        if other_resources:
            request = DescribeConfigsRequest(resources=other_resources, include_synonyms=include_synonyms)
            responses.append(await self._manager.send(request))

        ret = defaultdict(dict)
        for response in responses:
            for result in response.results:
                result_type = ConfigResourceType(result.resource_type).name.lower()
                ret[result_type][result.resource_name] = {}
                for config in result.configs:
                    config = config.to_dict()
                    name = config.pop('name')
                    if 'config_source' in config:
                        config['config_source'] = ConfigSourceType(config['config_source']).name
                    if 'synonyms' in config:
                        for synonym in config['synonyms']:
                            synonym['source'] = ConfigSourceType(synonym['source']).name

                    if 'config_type' in config:
                        config['config_type'] = ConfigType(config['config_type']).name
                    ret[result_type][result.resource_name][name] = config
        return dict(ret)

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
    UNKNOWN = 0
    TOPIC = 2
    BROKER = 4
    BROKER_LOGGER = 8
    CLIENT_METRICS = 16
    GROUP = 32


class ConfigResource:
    """A class for specifying config resources.

    Arguments:
        resource_type (ConfigResourceType): the type of kafka resource
        name (string): The name of the kafka resource
        configs ([key] or {key : value}): config keys (values required to alter)
    """
    def __init__(self, resource_type, name, configs=None):
        if not isinstance(resource_type, ConfigResourceType):
            resource_type = ConfigResourceType[str(resource_type).upper()]  # pylint: disable-msg=unsubscriptable-object
        self.resource_type = resource_type
        self.name = name
        self.configs = configs

    def __str__(self):
        return f"ConfigResource {self.name}={self.resource_type}"

    def __repr__(self):
        return f"ConfigResource({self.resource_type}, {self.name}, {self.configs})"


class ConfigType(IntEnum):
    UNKNOWN  = 0
    BOOLEAN  = 1
    STRING   = 2
    INT      = 3
    SHORT    = 4
    LONG     = 5
    DOUBLE   = 6
    LIST     = 7
    CLASS    = 8
    PASSWORD = 9


class ConfigSourceType(IntEnum):
    UNKNOWN = 0
    TOPIC_CONFIG = 1
    DYNAMIC_BROKER_CONFIG = 2
    DYNAMIC_DEFAULT_BROKER_CONFIG = 3
    STATIC_BROKER_CONFIG = 4
    DEFAULT_CONFIG = 5
    DYNAMIC_BROKER_LOGGER_CONFIG = 6
    CLIENT_METRICS_CONFIG = 7
    GROUP_CONFIG = 8
