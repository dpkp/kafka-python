"""Configuration management mixin for KafkaAdminClient.

Also defines ConfigResource and ConfigResourceType data classes.
"""

from __future__ import annotations

from collections import defaultdict
from enum import IntEnum
import logging
from typing import TYPE_CHECKING

import kafka.errors as Errors
from kafka.protocol.admin import (
    AlterConfigsRequest,
    DescribeConfigsRequest,
)

if TYPE_CHECKING:
    from kafka.net.manager import KafkaConnectionManager

log = logging.getLogger(__name__)


class ConfigAdminMixin:
    """Mixin providing configuration management methods for KafkaAdminClient."""
    _manager: KafkaConnectionManager
    config: dict

    @staticmethod
    def _convert_config_resource(config_resource, key_only=True):
        if key_only:
            values = list(config_resource.configs.keys()) if isinstance(config_resource.configs, dict) else config_resource.configs
        elif not config_resource.configs:
            values = []
        else:
            assert isinstance(config_resource.configs, dict)
            values = list(config_resource.configs.items())
        return (config_resource.resource_type, config_resource.name, values)

    def _group_config_resources(self, config_resources, key_only=True):
        broker_resources = defaultdict(list)
        other_resources = []
        for config_resource in config_resources:
            if config_resource.resource_type in (ConfigResourceType.BROKER, ConfigResourceType.BROKER_LOGGER):
                try:
                    broker_id = int(config_resource.name)
                except ValueError:
                    raise ValueError("Broker resource names must be an integer or a string represented integer")
                broker_resources[broker_id].append(self._convert_config_resource(config_resource, key_only=key_only))
            else:
                other_resources.append(self._convert_config_resource(config_resource, key_only=key_only))
        return broker_resources, other_resources

    async def _async_describe_configs(self, config_resources, include_synonyms=False, config_filter='modified', flat=False):
        if isinstance(config_filter, str):
            try:
                config_filter = ConfigFilterType[config_filter.upper()]
            except KeyError:
                raise ValueError(f'{config_filter} is not a valid ConfigFilterType')
        min_version = 1 if include_synonyms else 0
        broker_resources, other_resources = self._group_config_resources(config_resources, key_only=True)
        responses = []
        for broker_id, resources in broker_resources.items():
            request = DescribeConfigsRequest(
                resources=resources,
                include_synonyms=include_synonyms,
                min_version=min_version)
            responses.append(await self._manager.send(request, node_id=broker_id))
        if other_resources:
            request = DescribeConfigsRequest(
                resources=other_resources,
                include_synonyms=include_synonyms,
                min_version=min_version)
            responses.append(await self._manager.send(request))

        ret = defaultdict(dict)
        for response in responses:
            for result in response.results:
                resource_type = ConfigResourceType(result.resource_type)
                resource_configs = {}
                for config in result.configs:
                    config = config.to_dict()
                    name = config.pop('name')
                    if config_filter == ConfigFilterType.DYNAMIC and config['read_only']:
                        continue
                    if 'config_source' in config:
                        config_source = ConfigSourceType(config['config_source'])
                    elif config['read_only'] and resource_type is ConfigResourceType.BROKER:
                        config_source = ConfigSourceType.STATIC_BROKER_CONFIG
                    elif config['is_default']:
                        config_source = ConfigSourceType.DEFAULT_CONFIG
                    else:
                        config_source = ConfigSourceType.dynamic_for_resource_type(resource_type)
                    if config_filter.should_skip(config_source):
                        continue
                    config['config_source'] = config_source.name
                    if 'synonyms' in config:
                        for synonym in config['synonyms']:
                            synonym['source'] = ConfigSourceType(synonym['source']).name

                    if 'config_type' in config:
                        config['config_type'] = ConfigType(config['config_type']).name
                    resource_configs[name] = config
                ret[resource_type.name.lower()][result.resource_name] = resource_configs
        if flat:
            return [ret[resource.resource_type.name.lower()][resource.name]
                    for resource in config_resources]
        else:
            return dict(ret)

    def describe_configs(self, config_resources, include_synonyms=False, config_filter='modified'):
        """Fetch configuration parameters for one or more Kafka resources.

        Arguments:
            config_resources: An list of ConfigResource objects.
                Any keys in ConfigResource.configs dict will be used to filter the
                result. Setting the configs dict to None will get all values. An
                empty dict will get zero values (as per Kafka protocol).

        Keyword Arguments:
            include_synonyms (bool, optional): If True, return synonyms in response. Not
                supported by all versions. Default: False.
            config_filter (ConfigFilterType or str): Modified returns only keys that have
                non-default values; Dynamic returns all keys that can be modified with
                alter_configs; All returns all available keys. Default: Modified.

        Returns:
            dict of {resource_type (str): {resource_name (str): {config_key: {config data}}}}
        """
        return self._manager.run(self._async_describe_configs, config_resources,
                                 include_synonyms, config_filter)

    async def _get_missing_dynamic_configs(self, config_resources):
        resource_lookups = [ConfigResource(resource.resource_type, resource.name) for resource in config_resources]
        dynamic_configs = await self._async_describe_configs(resource_lookups, config_filter='modified', flat=True)
        missing_resource_configs = []
        for resource, describe in zip(config_resources, dynamic_configs):
            missing = {}
            for config_key in describe:
                if config_key not in resource.configs:
                    config_value = describe[config_key]['value']
                    if config_value is None:
                        continue
                    missing[config_key] = config_value
            missing_resource_configs.append(missing)
        return missing_resource_configs

    async def _add_missing_dynamic_configs(self, config_resources):
        # Add missing dynamic config values to resource list to avoid accidental resets
        missing_resource_configs = await self._get_missing_dynamic_configs(config_resources)
        for resource, missing in zip(config_resources, missing_resource_configs):
            resource.configs.update(missing)

    async def _validate_dynamic_configs(self, config_resources):
        resource_lookups = [ConfigResource(resource.resource_type, resource.name) for resource in config_resources]
        dynamic_configs = await self._async_describe_configs(resource_lookups, config_filter='dynamic', flat=True)
        for resource, describe in zip(config_resources, dynamic_configs):
            unknown = set(resource.configs) - set(describe)
            if unknown:
                raise ValueError(f'Unrecognized configs: {unknown}')

    async def _async_alter_configs(self, config_resources, validate_only=False, raise_on_unknown=True):
        # Broker Version <  (2, 3): use alter configs
        # Broker Version >= (2, 3): use incremental alter configs
        if raise_on_unknown:
            await self._validate_dynamic_configs(config_resources)
        await self._add_missing_dynamic_configs(config_resources)
        return await self._send_alter_configs_requests(config_resources, validate_only=validate_only)

    async def _send_alter_configs_requests(self, config_resources, validate_only=False):
        broker_resources, other_resources = self._group_config_resources(config_resources, key_only=False)
        responses = []
        for broker_id, resources in broker_resources.items():
            request = AlterConfigsRequest(
                resources=resources,
                validate_only=validate_only)
            response = await self._manager.send(request, node_id=broker_id)
            responses.extend(response.responses)
        if other_resources:
            request = AlterConfigsRequest(
                resources=other_resources,
                validate_only=validate_only)
            response = await self._manager.send(request)
            responses.extend(response.responses)
        ret = defaultdict(dict)
        for response in responses:
            if response.error_code == 0:
                result = 'OK'
            else:
                result = str(Errors.for_code(response.error_code)(response.error_message))
            result_type = ConfigResourceType(response.resource_type).name.lower()
            ret[result_type][response.resource_name] = result
        return dict(ret)

    def alter_configs(self, config_resources, validate_only=False, raise_on_unknown=True):
        """Alter configuration parameters of one or more Kafka resources.

        Arguments:
            config_resources: A list of ConfigResource objects.
            validate_only (bool, optional): If True, changes are sent to broker for
                validation only. Changes will not be applied. Default: False
            raise_on_unknown (bool, optional): If True, raises ValueError if any
                config key is not recognized as a dynamic config for the resource.

        Returns:
            dict of {resource_type (str): {resource_name (str): Error/Result}}
        """
        return self._manager.run(self._async_alter_configs, config_resources, validate_only, raise_on_unknown)

    async def _async_reset_configs(self, config_resources, validate_only=False, raise_on_unknown=True):
        if raise_on_unknown:
            await self._validate_dynamic_configs(config_resources)
        # if no keys provided, submit as-is -- full reset
        # if keys are provided, replace with missing -- partial reset
        partial_resets = [resource for resource in config_resources if resource.configs]
        missing_resource_configs = await self._get_missing_dynamic_configs(partial_resets)
        for resource, missing in zip(partial_resets, missing_resource_configs):
            resource.configs = missing
        return await self._send_alter_configs_requests(config_resources, validate_only=validate_only)

    def reset_configs(self, config_resources, validate_only=False, raise_on_unknown=True):
        """Reset configuration parameters of one or more Kafka resources.

        Arguments:
            config_resources: A list of ConfigResource objects.

        Returns:
            dict of {resource_type (str): {resource_name (str): Error/Result}}
        """
        return self._manager.run(self._async_reset_configs, config_resources, validate_only, raise_on_unknown)


class ConfigFilterType(IntEnum):
    ALL = 0
    DYNAMIC = 1
    MODIFIED = 2
    DEFAULT = 3
    STATIC = 4

    def should_skip(self, config_source):
        if self is ConfigFilterType.MODIFIED:
            return not config_source.is_modified()
        elif self is ConfigFilterType.DEFAULT:
            return config_source.is_modified()
        elif self is ConfigFilterType.STATIC:
            return config_source is not ConfigSourceType.STATIC_BROKER_CONFIG
        return False


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
    DYNAMIC_TOPIC_CONFIG = 1
    DYNAMIC_BROKER_CONFIG = 2
    DYNAMIC_DEFAULT_BROKER_CONFIG = 3
    STATIC_BROKER_CONFIG = 4
    DEFAULT_CONFIG = 5
    DYNAMIC_BROKER_LOGGER_CONFIG = 6
    DYNAMIC_CLIENT_METRICS_CONFIG = 7
    DYNAMIC_GROUP_CONFIG = 8

    def is_modified(self):
        return self.value not in (3, 4, 5)

    @classmethod
    def dynamic_for_resource_type(cls, resource_type):
        if resource_type is ConfigResourceType.UNKNOWN:
            return ConfigSourceType.UNKNOWN
        elif resource_type is ConfigResourceType.TOPIC:
            return ConfigSourceType.DYNAMIC_TOPIC_CONFIG
        elif resource_type is ConfigResourceType.BROKER:
            return ConfigSourceType.DYNAMIC_BROKER_CONFIG
        elif resource_type is ConfigResourceType.BROKER_LOGGER:
            return ConfigSourceType.DYNAMIC_BROKER_LOGGER_CONFIG
        elif resource_type is ConfigResourceType.CLIENT_METRICS:
            return ConfigSourceType.DYNAMIC_CLIENT_METRICS_CONFIG
        elif resource_type is ConfigResourceType.GROUP:
            return ConfigSourceType.DYNAMIC_GROUP_CONFIG
        else:
            raise RuntimeError(f'Unrecognized resource type {resource_type}')
