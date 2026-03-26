import uuid
from typing import Any, Self

from enum import IntEnum
from kafka.protocol.new.api_message import ApiMessage
from kafka.protocol.new.data_container import DataContainer

__all__ = ['DescribeClusterRequest', 'DescribeClusterResponse', 'DescribeConfigsRequest', 'DescribeConfigsResponse', 'AlterConfigsRequest', 'AlterConfigsResponse', 'DescribeLogDirsRequest', 'DescribeLogDirsResponse', 'ElectLeadersRequest', 'ElectLeadersResponse', 'ElectionType']

class DescribeClusterRequest(ApiMessage):
    include_cluster_authorized_operations: bool
    endpoint_type: int
    include_fenced_brokers: bool
    def __init__(
        self,
        *args: Any,
        include_cluster_authorized_operations: bool = ...,
        endpoint_type: int = ...,
        include_fenced_brokers: bool = ...,
        version: int | None = None,
        **kwargs: Any,
    ) -> None: ...
    @property
    def version(self) -> int | None: ...
    def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...
    name: str
    type: str
    API_KEY: int
    API_VERSION: int
    valid_versions: tuple[int, int]
    min_version: int
    max_version: int
    @property
    def header(self) -> Any: ...
    @classmethod
    def is_request(cls) -> bool: ...
    def expect_response(self) -> bool: ...
    def with_header(self, correlation_id: int = 0, client_id: str = "kafka-python") -> None: ...

class DescribeClusterResponse(ApiMessage):
    class DescribeClusterBroker(DataContainer):
        broker_id: int
        host: str
        port: int
        rack: str | None
        is_fenced: bool
        def __init__(
            self,
            *args: Any,
            broker_id: int = ...,
            host: str = ...,
            port: int = ...,
            rack: str | None = ...,
            is_fenced: bool = ...,
            version: int | None = None,
            **kwargs: Any,
        ) -> None: ...
        @property
        def version(self) -> int | None: ...
        def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

    throttle_time_ms: int
    error_code: int
    error_message: str | None
    endpoint_type: int
    cluster_id: str
    controller_id: int
    brokers: list[DescribeClusterBroker]
    cluster_authorized_operations: set[int]
    def __init__(
        self,
        *args: Any,
        throttle_time_ms: int = ...,
        error_code: int = ...,
        error_message: str | None = ...,
        endpoint_type: int = ...,
        cluster_id: str = ...,
        controller_id: int = ...,
        brokers: list[DescribeClusterBroker] = ...,
        cluster_authorized_operations: set[int] = ...,
        version: int | None = None,
        **kwargs: Any,
    ) -> None: ...
    @property
    def version(self) -> int | None: ...
    def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...
    name: str
    type: str
    API_KEY: int
    API_VERSION: int
    valid_versions: tuple[int, int]
    min_version: int
    max_version: int
    @property
    def header(self) -> Any: ...
    @classmethod
    def is_request(cls) -> bool: ...
    def expect_response(self) -> bool: ...
    def with_header(self, correlation_id: int = 0, client_id: str = "kafka-python") -> None: ...

class DescribeConfigsRequest(ApiMessage):
    class DescribeConfigsResource(DataContainer):
        resource_type: int
        resource_name: str
        configuration_keys: list[str] | None
        def __init__(
            self,
            *args: Any,
            resource_type: int = ...,
            resource_name: str = ...,
            configuration_keys: list[str] | None = ...,
            version: int | None = None,
            **kwargs: Any,
        ) -> None: ...
        @property
        def version(self) -> int | None: ...
        def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

    resources: list[DescribeConfigsResource]
    include_synonyms: bool
    include_documentation: bool
    def __init__(
        self,
        *args: Any,
        resources: list[DescribeConfigsResource] = ...,
        include_synonyms: bool = ...,
        include_documentation: bool = ...,
        version: int | None = None,
        **kwargs: Any,
    ) -> None: ...
    @property
    def version(self) -> int | None: ...
    def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...
    name: str
    type: str
    API_KEY: int
    API_VERSION: int
    valid_versions: tuple[int, int]
    min_version: int
    max_version: int
    @property
    def header(self) -> Any: ...
    @classmethod
    def is_request(cls) -> bool: ...
    def expect_response(self) -> bool: ...
    def with_header(self, correlation_id: int = 0, client_id: str = "kafka-python") -> None: ...

class DescribeConfigsResponse(ApiMessage):
    class DescribeConfigsResult(DataContainer):
        class DescribeConfigsResourceResult(DataContainer):
            class DescribeConfigsSynonym(DataContainer):
                name: str
                value: str | None
                source: int
                def __init__(
                    self,
                    *args: Any,
                    name: str = ...,
                    value: str | None = ...,
                    source: int = ...,
                    version: int | None = None,
                    **kwargs: Any,
                ) -> None: ...
                @property
                def version(self) -> int | None: ...
                def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

            name: str
            value: str | None
            read_only: bool
            config_source: int
            is_default: bool
            is_sensitive: bool
            synonyms: list[DescribeConfigsSynonym]
            config_type: int
            documentation: str | None
            def __init__(
                self,
                *args: Any,
                name: str = ...,
                value: str | None = ...,
                read_only: bool = ...,
                config_source: int = ...,
                is_default: bool = ...,
                is_sensitive: bool = ...,
                synonyms: list[DescribeConfigsSynonym] = ...,
                config_type: int = ...,
                documentation: str | None = ...,
                version: int | None = None,
                **kwargs: Any,
            ) -> None: ...
            @property
            def version(self) -> int | None: ...
            def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

        error_code: int
        error_message: str | None
        resource_type: int
        resource_name: str
        configs: list[DescribeConfigsResourceResult]
        def __init__(
            self,
            *args: Any,
            error_code: int = ...,
            error_message: str | None = ...,
            resource_type: int = ...,
            resource_name: str = ...,
            configs: list[DescribeConfigsResourceResult] = ...,
            version: int | None = None,
            **kwargs: Any,
        ) -> None: ...
        @property
        def version(self) -> int | None: ...
        def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

    throttle_time_ms: int
    results: list[DescribeConfigsResult]
    def __init__(
        self,
        *args: Any,
        throttle_time_ms: int = ...,
        results: list[DescribeConfigsResult] = ...,
        version: int | None = None,
        **kwargs: Any,
    ) -> None: ...
    @property
    def version(self) -> int | None: ...
    def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...
    name: str
    type: str
    API_KEY: int
    API_VERSION: int
    valid_versions: tuple[int, int]
    min_version: int
    max_version: int
    @property
    def header(self) -> Any: ...
    @classmethod
    def is_request(cls) -> bool: ...
    def expect_response(self) -> bool: ...
    def with_header(self, correlation_id: int = 0, client_id: str = "kafka-python") -> None: ...

class AlterConfigsRequest(ApiMessage):
    class AlterConfigsResource(DataContainer):
        class AlterableConfig(DataContainer):
            name: str
            value: str | None
            def __init__(
                self,
                *args: Any,
                name: str = ...,
                value: str | None = ...,
                version: int | None = None,
                **kwargs: Any,
            ) -> None: ...
            @property
            def version(self) -> int | None: ...
            def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

        resource_type: int
        resource_name: str
        configs: list[AlterableConfig]
        def __init__(
            self,
            *args: Any,
            resource_type: int = ...,
            resource_name: str = ...,
            configs: list[AlterableConfig] = ...,
            version: int | None = None,
            **kwargs: Any,
        ) -> None: ...
        @property
        def version(self) -> int | None: ...
        def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

    resources: list[AlterConfigsResource]
    validate_only: bool
    def __init__(
        self,
        *args: Any,
        resources: list[AlterConfigsResource] = ...,
        validate_only: bool = ...,
        version: int | None = None,
        **kwargs: Any,
    ) -> None: ...
    @property
    def version(self) -> int | None: ...
    def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...
    name: str
    type: str
    API_KEY: int
    API_VERSION: int
    valid_versions: tuple[int, int]
    min_version: int
    max_version: int
    @property
    def header(self) -> Any: ...
    @classmethod
    def is_request(cls) -> bool: ...
    def expect_response(self) -> bool: ...
    def with_header(self, correlation_id: int = 0, client_id: str = "kafka-python") -> None: ...

class AlterConfigsResponse(ApiMessage):
    class AlterConfigsResourceResponse(DataContainer):
        error_code: int
        error_message: str | None
        resource_type: int
        resource_name: str
        def __init__(
            self,
            *args: Any,
            error_code: int = ...,
            error_message: str | None = ...,
            resource_type: int = ...,
            resource_name: str = ...,
            version: int | None = None,
            **kwargs: Any,
        ) -> None: ...
        @property
        def version(self) -> int | None: ...
        def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

    throttle_time_ms: int
    responses: list[AlterConfigsResourceResponse]
    def __init__(
        self,
        *args: Any,
        throttle_time_ms: int = ...,
        responses: list[AlterConfigsResourceResponse] = ...,
        version: int | None = None,
        **kwargs: Any,
    ) -> None: ...
    @property
    def version(self) -> int | None: ...
    def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...
    name: str
    type: str
    API_KEY: int
    API_VERSION: int
    valid_versions: tuple[int, int]
    min_version: int
    max_version: int
    @property
    def header(self) -> Any: ...
    @classmethod
    def is_request(cls) -> bool: ...
    def expect_response(self) -> bool: ...
    def with_header(self, correlation_id: int = 0, client_id: str = "kafka-python") -> None: ...

class DescribeLogDirsRequest(ApiMessage):
    class DescribableLogDirTopic(DataContainer):
        topic: str
        partitions: list[int]
        def __init__(
            self,
            *args: Any,
            topic: str = ...,
            partitions: list[int] = ...,
            version: int | None = None,
            **kwargs: Any,
        ) -> None: ...
        @property
        def version(self) -> int | None: ...
        def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

    topics: list[DescribableLogDirTopic] | None
    def __init__(
        self,
        *args: Any,
        topics: list[DescribableLogDirTopic] | None = ...,
        version: int | None = None,
        **kwargs: Any,
    ) -> None: ...
    @property
    def version(self) -> int | None: ...
    def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...
    name: str
    type: str
    API_KEY: int
    API_VERSION: int
    valid_versions: tuple[int, int]
    min_version: int
    max_version: int
    @property
    def header(self) -> Any: ...
    @classmethod
    def is_request(cls) -> bool: ...
    def expect_response(self) -> bool: ...
    def with_header(self, correlation_id: int = 0, client_id: str = "kafka-python") -> None: ...

class DescribeLogDirsResponse(ApiMessage):
    class DescribeLogDirsResult(DataContainer):
        class DescribeLogDirsTopic(DataContainer):
            class DescribeLogDirsPartition(DataContainer):
                partition_index: int
                partition_size: int
                offset_lag: int
                is_future_key: bool
                def __init__(
                    self,
                    *args: Any,
                    partition_index: int = ...,
                    partition_size: int = ...,
                    offset_lag: int = ...,
                    is_future_key: bool = ...,
                    version: int | None = None,
                    **kwargs: Any,
                ) -> None: ...
                @property
                def version(self) -> int | None: ...
                def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

            name: str
            partitions: list[DescribeLogDirsPartition]
            def __init__(
                self,
                *args: Any,
                name: str = ...,
                partitions: list[DescribeLogDirsPartition] = ...,
                version: int | None = None,
                **kwargs: Any,
            ) -> None: ...
            @property
            def version(self) -> int | None: ...
            def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

        error_code: int
        log_dir: str
        topics: list[DescribeLogDirsTopic]
        total_bytes: int
        usable_bytes: int
        def __init__(
            self,
            *args: Any,
            error_code: int = ...,
            log_dir: str = ...,
            topics: list[DescribeLogDirsTopic] = ...,
            total_bytes: int = ...,
            usable_bytes: int = ...,
            version: int | None = None,
            **kwargs: Any,
        ) -> None: ...
        @property
        def version(self) -> int | None: ...
        def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

    throttle_time_ms: int
    error_code: int
    results: list[DescribeLogDirsResult]
    def __init__(
        self,
        *args: Any,
        throttle_time_ms: int = ...,
        error_code: int = ...,
        results: list[DescribeLogDirsResult] = ...,
        version: int | None = None,
        **kwargs: Any,
    ) -> None: ...
    @property
    def version(self) -> int | None: ...
    def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...
    name: str
    type: str
    API_KEY: int
    API_VERSION: int
    valid_versions: tuple[int, int]
    min_version: int
    max_version: int
    @property
    def header(self) -> Any: ...
    @classmethod
    def is_request(cls) -> bool: ...
    def expect_response(self) -> bool: ...
    def with_header(self, correlation_id: int = 0, client_id: str = "kafka-python") -> None: ...

class ElectLeadersRequest(ApiMessage):
    class TopicPartitions(DataContainer):
        topic: str
        partitions: list[int]
        def __init__(
            self,
            *args: Any,
            topic: str = ...,
            partitions: list[int] = ...,
            version: int | None = None,
            **kwargs: Any,
        ) -> None: ...
        @property
        def version(self) -> int | None: ...
        def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

    election_type: int
    topic_partitions: list[TopicPartitions] | None
    timeout_ms: int
    def __init__(
        self,
        *args: Any,
        election_type: int = ...,
        topic_partitions: list[TopicPartitions] | None = ...,
        timeout_ms: int = ...,
        version: int | None = None,
        **kwargs: Any,
    ) -> None: ...
    @property
    def version(self) -> int | None: ...
    def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...
    name: str
    type: str
    API_KEY: int
    API_VERSION: int
    valid_versions: tuple[int, int]
    min_version: int
    max_version: int
    @property
    def header(self) -> Any: ...
    @classmethod
    def is_request(cls) -> bool: ...
    def expect_response(self) -> bool: ...
    def with_header(self, correlation_id: int = 0, client_id: str = "kafka-python") -> None: ...

class ElectLeadersResponse(ApiMessage):
    class ReplicaElectionResult(DataContainer):
        class PartitionResult(DataContainer):
            partition_id: int
            error_code: int
            error_message: str | None
            def __init__(
                self,
                *args: Any,
                partition_id: int = ...,
                error_code: int = ...,
                error_message: str | None = ...,
                version: int | None = None,
                **kwargs: Any,
            ) -> None: ...
            @property
            def version(self) -> int | None: ...
            def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

        topic: str
        partition_result: list[PartitionResult]
        def __init__(
            self,
            *args: Any,
            topic: str = ...,
            partition_result: list[PartitionResult] = ...,
            version: int | None = None,
            **kwargs: Any,
        ) -> None: ...
        @property
        def version(self) -> int | None: ...
        def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

    throttle_time_ms: int
    error_code: int
    replica_election_results: list[ReplicaElectionResult]
    def __init__(
        self,
        *args: Any,
        throttle_time_ms: int = ...,
        error_code: int = ...,
        replica_election_results: list[ReplicaElectionResult] = ...,
        version: int | None = None,
        **kwargs: Any,
    ) -> None: ...
    @property
    def version(self) -> int | None: ...
    def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...
    name: str
    type: str
    API_KEY: int
    API_VERSION: int
    valid_versions: tuple[int, int]
    min_version: int
    max_version: int
    @property
    def header(self) -> Any: ...
    @classmethod
    def is_request(cls) -> bool: ...
    def expect_response(self) -> bool: ...
    def with_header(self, correlation_id: int = 0, client_id: str = "kafka-python") -> None: ...

class ElectionType(IntEnum):
    PREFERRED: int
    UNCLEAN: int
