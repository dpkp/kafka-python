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
    IncrementalAlterConfigsRequest,
    ListConfigResourcesRequest,
)

if TYPE_CHECKING:
    from kafka.net.manager import KafkaConnectionManager

log = logging.getLogger(__name__)


class ConfigAdminMixin:
    """Mixin providing configuration management methods for KafkaAdminClient."""
    _manager: KafkaConnectionManager
    config: dict

    def _check_incremental_alter_configs_support(self):
        # Broker Version >= (2, 3) has incremental alter configs
        try:
            self._manager.broker_version_data.api_version(IncrementalAlterConfigsRequest)
            return True
        except Errors.IncompatibleBrokerVersion:
            return False

    @staticmethod
    def _incremental_configs_entries(configs):
        if not configs:
            return []
        if not isinstance(configs, dict):
            raise TypeError('alter_configs requires configs as a dict of '
                            '{key: (op, value)} or {key: value} (interpreted as SET)')
        entries = []
        for name, op_value in configs.items():
            if isinstance(op_value, tuple):
                op, value = op_value
            else:
                op, value = AlterConfigOp.SET, op_value
            op_code = AlterConfigOp.value_for(op)
            if op_code == AlterConfigOp.DELETE.value:
                value = None
            entries.append((name, op_code, value))
        return entries

    @staticmethod
    def _describe_configs_entries(configs):
        return list(configs.keys()) if isinstance(configs, dict) else configs

    @staticmethod
    def _alter_configs_entries(configs):
        if not configs:
            return []
        elif not isinstance(configs, dict):
            raise TypeError(f'configs should be a dict of {{key: value}}, found {type(configs)}')
        entries = []
        for name, op_value in configs.items():
            if isinstance(op_value, tuple):
                op, value = op_value
            else:
                op, value = AlterConfigOp.SET, op_value
            op_code = AlterConfigOp.value_for(op)
            if op_code != AlterConfigOp.SET.value:
                raise ValueError(f'Non-incremental AlterConfigsRequest does not support operation {op} (SET only)')
            entries.append((name, value))
        return entries

    @staticmethod
    def _group_config_resources(config_resources):
        broker_resources = defaultdict(list)
        other_resources = []
        for config_resource in config_resources:
            if config_resource.resource_type in (ConfigResourceType.BROKER, ConfigResourceType.BROKER_LOGGER):
                try:
                    broker_id = int(config_resource.name)
                except ValueError:
                    raise ValueError("Broker resource names must be an integer or a string represented integer")
                broker_resources[broker_id].append(config_resource)
            else:
                other_resources.append(config_resource)
        return broker_resources, other_resources

    @classmethod
    def _describe_configs_request(cls, config_resources, include_synonyms=False):
        min_version = 1 if include_synonyms else 0
        return DescribeConfigsRequest(
            resources=[(cr.resource_type, cr.name, cls._describe_configs_entries(cr.configs))
                       for cr in config_resources],
            include_synonyms=include_synonyms,
            min_version=min_version)

    @staticmethod
    def _describe_configs_process_responses(responses, config_filter='modified'):
        if isinstance(config_filter, str):
            try:
                config_filter = ConfigFilterType[config_filter.upper()]
            except KeyError:
                raise ValueError(f'{config_filter} is not a valid ConfigFilterType')
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
        return dict(ret)

    async def _async_describe_configs(self, config_resources, include_synonyms=False, config_filter='modified', flat=False):
        broker_resources, other_resources = self._group_config_resources(config_resources)
        responses = []
        for broker_id, resources in broker_resources.items():
            request = self._describe_configs_request(resources, include_synonyms)
            responses.append(await self._manager.send(request, node_id=broker_id))
        if other_resources:
            request = self._describe_configs_request(other_resources, include_synonyms)
            responses.append(await self._manager.send(request))
        ret = self._describe_configs_process_responses(responses, config_filter)
        if flat:
            return [ret[resource.resource_type.name.lower()][resource.name]
                    for resource in config_resources]
        else:
            return ret

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

    @staticmethod
    def _list_config_resources_process_response(response):
        error_type = Errors.for_code(response.error_code)
        if error_type is not Errors.NoError:
            raise error_type(
                "ListConfigResourcesRequest failed with response '{}'.".format(response))
        ret = defaultdict(list)
        for resource in response.config_resources:
            resource_type = ConfigResourceType(resource.resource_type)
            ret[resource_type.name.lower()].append(resource.resource_name)
        return dict(ret)

    async def _async_list_config_resources(self, resource_types=None):
        wire_types = []
        for rt in resource_types or []:
            if not isinstance(rt, ConfigResourceType):
                try:
                    rt = ConfigResourceType[str(rt).upper().replace('-', '_')]
                except KeyError:
                    raise ValueError(f'Unrecognized ConfigResourceType: {rt}')
            wire_types.append(rt.value)
        request = ListConfigResourcesRequest(resource_types=wire_types)
        response = await self._manager.send(request)
        return self._list_config_resources_process_response(response)

    def list_config_resources(self, resource_types=None):
        """List config resources known to the cluster.

        Useful for discovering resource types that have no separate enumeration
        API (e.g. ``CLIENT_METRICS``, ``GROUP``). For ``TOPIC`` and ``BROKER``
        the data is also available via ``Metadata`` / cluster descriptions.

        Keyword Arguments:
            resource_types (list, optional): Filter by resource type. Each entry
                may be a :class:`ConfigResourceType` or its name (e.g. ``'TOPIC'``).
                If None or empty, the broker returns all supported types.
                Requires broker >= 4.1 for anything other than ``CLIENT_METRICS``.

        Returns:
            dict of {resource_type (str): [resource_name (str)]}
        """
        return self._manager.run(self._async_list_config_resources, resource_types)

    async def _get_missing_modified_configs(self, config_resources):
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
        # Add missing modified config values to resource list to avoid accidental resets
        missing_resource_configs = await self._get_missing_modified_configs(config_resources)
        for resource, missing in zip(config_resources, missing_resource_configs):
            if not isinstance(missing, dict):
                raise TypeError(f'missing configs: expected dict, found {type(missing)}')
            resource.configs.update(missing)

    async def _validate_dynamic_configs(self, config_resources):
        resource_lookups = [ConfigResource(resource.resource_type, resource.name) for resource in config_resources]
        dynamic_configs = await self._async_describe_configs(resource_lookups, config_filter='dynamic', flat=True)
        for resource, describe in zip(config_resources, dynamic_configs):
            unknown = set(resource.configs or []) - set(describe)
            if unknown:
                raise ValueError(f'Unrecognized configs: {unknown}')

    @classmethod
    def _alter_configs_request(cls, config_resources, validate_only=False, incremental=False):
        if incremental:
            return IncrementalAlterConfigsRequest(
                resources=[(cr.resource_type, cr.name, cls._incremental_configs_entries(cr.configs))
                           for cr in config_resources],
                validate_only=validate_only)
        else:
            return AlterConfigsRequest(
                resources=[(cr.resource_type, cr.name, cls._alter_configs_entries(cr.configs))
                           for cr in config_resources],
                validate_only=validate_only)

    @staticmethod
    def _alter_configs_process_responses(responses):
        ret = defaultdict(dict)
        for response in responses:
            if response.error_code == 0:
                result = 'OK'
            else:
                result = str(Errors.for_code(response.error_code)(response.error_message))
            result_type = ConfigResourceType(response.resource_type).name.lower()
            ret[result_type][response.resource_name] = result
        return dict(ret)

    async def _send_alter_configs_requests(self, config_resources, validate_only=False, incremental=False):
        broker_resources, other_resources = self._group_config_resources(config_resources)
        responses = []
        for broker_id, resources in broker_resources.items():
            request = self._alter_configs_request(resources, validate_only, incremental)
            response = await self._manager.send(request, node_id=broker_id)
            responses.extend(response.responses)
        if other_resources:
            request = self._alter_configs_request(other_resources, validate_only, incremental)
            response = await self._manager.send(request)
            responses.extend(response.responses)
        return self._alter_configs_process_responses(responses)

    async def _async_alter_configs(self, config_resources, validate_only=False, raise_on_unknown=True, incremental=None):
        if raise_on_unknown:
            await self._validate_dynamic_configs(config_resources)
        if incremental is None:
            incremental = self._check_incremental_alter_configs_support()
        if not incremental:
            await self._add_missing_dynamic_configs(config_resources)
        return await self._send_alter_configs_requests(config_resources, validate_only, incremental)

    def alter_configs(self, config_resources, validate_only=False, raise_on_unknown=True, incremental=None):
        """Alter configuration parameters of one or more Kafka resources.

        Arguments:
            config_resources: A list of ConfigResource objects. Each resource's
                ``configs`` must be a dict mapping config key to either
                ``(op, value)`` (where ``op`` is an :class:`AlterConfigOp`,
                its name, or its int value) or a bare value (interpreted as SET).
                For DELETE operations the value is ignored and sent as null.
                Note: if broker does not support IncrementalAlterConfigsRequest,
                AlterConfigOp APPEND/SUBTRACT are only supported on 2.3+ brokers,
                which support the IncrementalAlterConfigsRequest. For older brokers
                the client will use AlterConfigsRequest, which requires submitting
                all dynamic configs together (the client will fill in missing keys
                as required, though be wary of the inherent race with this approach).
            validate_only (bool, optional): If True, changes are sent to broker for
                validation only. Changes will not be applied. Default: False
            raise_on_unknown (bool, optional): If True, raises ValueError if any
                config key is not recognized as a dynamic config for the resource.
            incremental (bool, optional): Set to True/False to force use of
                IncrementalAlterConfigs (True) or AlterConfigs (False).
                By Default, the admin client will use IncrementalAlterConfigs
                if supported by the broker, otherwise AlterConfigs.

        Returns:
            dict of {resource_type (str): {resource_name (str): Error/Result}}
        """
        return self._manager.run(self._async_alter_configs, config_resources, validate_only, raise_on_unknown, incremental)

    async def _async_reset_configs(self, config_resources, validate_only=False, raise_on_unknown=True, incremental=None):
        if raise_on_unknown:
            await self._validate_dynamic_configs(config_resources)
        if incremental is None:
            incremental = self._check_incremental_alter_configs_support()

        if not incremental:
            # if no keys provided, submit as-is -- full reset
            # if keys are provided, replace with missing -- partial reset
            partial_resets = [resource for resource in config_resources if resource.configs]
            missing_resource_configs = await self._get_missing_modified_configs(partial_resets)
            for resource, missing in zip(partial_resets, missing_resource_configs):
                resource.configs = missing
        else:
            config_resources = [
                ConfigResource(cr.resource_type, cr.name,
                               {key: (AlterConfigOp.DELETE, None)
                                for key in cr.configs})
                for cr in config_resources
            ]
        return await self._send_alter_configs_requests(config_resources, validate_only, incremental)

    def reset_configs(self, config_resources, validate_only=False, raise_on_unknown=True, incremental=None):
        """Reset configuration parameters of one or more Kafka resources to defaults.

        On 2.3+ brokers, the client will submit an IncrementalAlterConfigsRequest
        with op DELETE for each resource/key. On older brokers, the client will
        use submit an AlterConfigsRequest and attempt to include all modified
        dynamic config values for each resource except the keys marked for reset.
        (AlterConfigsRequest will reset any missing config key to its default).

        Arguments:
            config_resources: A list of ConfigResource objects. Each resource's
                ``configs`` should be a list or dict of config keys to reset.
                (if dict, the values are ignored).

        Returns:
            dict of {resource_type (str): {resource_name (str): Error/Result}}
        """
        return self._manager.run(self._async_reset_configs, config_resources, validate_only, raise_on_unknown, incremental)


class AlterConfigOp(IntEnum):
    SET = 0
    DELETE = 1
    APPEND = 2
    SUBTRACT = 3

    @staticmethod
    def value_for(op):
        if isinstance(op, AlterConfigOp):
            return op.value
        if isinstance(op, int):
            return AlterConfigOp(op).value
        try:
            return AlterConfigOp[str(op).upper()].value
        except KeyError:
            raise ValueError(f'Unrecognized AlterConfigOp: {op}')


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
