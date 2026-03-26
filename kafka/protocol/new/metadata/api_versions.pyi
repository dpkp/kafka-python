import uuid
from typing import Any, Self

from kafka.protocol.new.api_message import ApiMessage
from kafka.protocol.new.data_container import DataContainer

__all__ = ['ApiVersionsRequest', 'ApiVersionsResponse']

class ApiVersionsRequest(ApiMessage):
    client_software_name: str
    client_software_version: str
    def __init__(
        self,
        *args: Any,
        client_software_name: str = ...,
        client_software_version: str = ...,
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

class ApiVersionsResponse(ApiMessage):
    class ApiVersion(DataContainer):
        api_key: int
        min_version: int
        max_version: int
        def __init__(
            self,
            *args: Any,
            api_key: int = ...,
            min_version: int = ...,
            max_version: int = ...,
            version: int | None = None,
            **kwargs: Any,
        ) -> None: ...
        @property
        def version(self) -> int | None: ...
        def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

    class SupportedFeatureKey(DataContainer):
        name: str
        min_version: int
        max_version: int
        def __init__(
            self,
            *args: Any,
            name: str = ...,
            min_version: int = ...,
            max_version: int = ...,
            version: int | None = None,
            **kwargs: Any,
        ) -> None: ...
        @property
        def version(self) -> int | None: ...
        def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

    class FinalizedFeatureKey(DataContainer):
        name: str
        max_version_level: int
        min_version_level: int
        def __init__(
            self,
            *args: Any,
            name: str = ...,
            max_version_level: int = ...,
            min_version_level: int = ...,
            version: int | None = None,
            **kwargs: Any,
        ) -> None: ...
        @property
        def version(self) -> int | None: ...
        def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

    error_code: int
    api_keys: list[ApiVersion]
    throttle_time_ms: int
    supported_features: list[SupportedFeatureKey]
    finalized_features_epoch: int
    finalized_features: list[FinalizedFeatureKey]
    zk_migration_ready: bool
    def __init__(
        self,
        *args: Any,
        error_code: int = ...,
        api_keys: list[ApiVersion] = ...,
        throttle_time_ms: int = ...,
        supported_features: list[SupportedFeatureKey] = ...,
        finalized_features_epoch: int = ...,
        finalized_features: list[FinalizedFeatureKey] = ...,
        zk_migration_ready: bool = ...,
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
    @classmethod
    def parse_header(cls, data: Any, version: Any = ...) -> Any: ...
    def encode_header(self, flexible: Any = ...) -> Any: ...
