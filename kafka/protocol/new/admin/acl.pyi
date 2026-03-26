import uuid
from typing import Any, Self

from enum import IntEnum
from kafka.protocol.new.api_message import ApiMessage
from kafka.protocol.new.data_container import DataContainer

__all__ = ['CreateAclsRequest', 'CreateAclsResponse', 'DeleteAclsRequest', 'DeleteAclsResponse', 'DescribeAclsRequest', 'DescribeAclsResponse', 'ACLResourceType', 'ACLOperation', 'ACLPermissionType', 'ACLResourcePatternType']

class CreateAclsRequest(ApiMessage):
    class AclCreation(DataContainer):
        resource_type: int
        resource_name: str
        resource_pattern_type: int
        principal: str
        host: str
        operation: int
        permission_type: int
        def __init__(
            self,
            *args: Any,
            resource_type: int = ...,
            resource_name: str = ...,
            resource_pattern_type: int = ...,
            principal: str = ...,
            host: str = ...,
            operation: int = ...,
            permission_type: int = ...,
            version: int | None = None,
            **kwargs: Any,
        ) -> None: ...
        @property
        def version(self) -> int | None: ...
        def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

    creations: list[AclCreation]
    def __init__(
        self,
        *args: Any,
        creations: list[AclCreation] = ...,
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

class CreateAclsResponse(ApiMessage):
    class AclCreationResult(DataContainer):
        error_code: int
        error_message: str | None
        def __init__(
            self,
            *args: Any,
            error_code: int = ...,
            error_message: str | None = ...,
            version: int | None = None,
            **kwargs: Any,
        ) -> None: ...
        @property
        def version(self) -> int | None: ...
        def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

    throttle_time_ms: int
    results: list[AclCreationResult]
    def __init__(
        self,
        *args: Any,
        throttle_time_ms: int = ...,
        results: list[AclCreationResult] = ...,
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

class DeleteAclsRequest(ApiMessage):
    class DeleteAclsFilter(DataContainer):
        resource_type_filter: int
        resource_name_filter: str | None
        pattern_type_filter: int
        principal_filter: str | None
        host_filter: str | None
        operation: int
        permission_type: int
        def __init__(
            self,
            *args: Any,
            resource_type_filter: int = ...,
            resource_name_filter: str | None = ...,
            pattern_type_filter: int = ...,
            principal_filter: str | None = ...,
            host_filter: str | None = ...,
            operation: int = ...,
            permission_type: int = ...,
            version: int | None = None,
            **kwargs: Any,
        ) -> None: ...
        @property
        def version(self) -> int | None: ...
        def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

    filters: list[DeleteAclsFilter]
    def __init__(
        self,
        *args: Any,
        filters: list[DeleteAclsFilter] = ...,
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

class DeleteAclsResponse(ApiMessage):
    class DeleteAclsFilterResult(DataContainer):
        class DeleteAclsMatchingAcl(DataContainer):
            error_code: int
            error_message: str | None
            resource_type: int
            resource_name: str
            pattern_type: int
            principal: str
            host: str
            operation: int
            permission_type: int
            def __init__(
                self,
                *args: Any,
                error_code: int = ...,
                error_message: str | None = ...,
                resource_type: int = ...,
                resource_name: str = ...,
                pattern_type: int = ...,
                principal: str = ...,
                host: str = ...,
                operation: int = ...,
                permission_type: int = ...,
                version: int | None = None,
                **kwargs: Any,
            ) -> None: ...
            @property
            def version(self) -> int | None: ...
            def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

        error_code: int
        error_message: str | None
        matching_acls: list[DeleteAclsMatchingAcl]
        def __init__(
            self,
            *args: Any,
            error_code: int = ...,
            error_message: str | None = ...,
            matching_acls: list[DeleteAclsMatchingAcl] = ...,
            version: int | None = None,
            **kwargs: Any,
        ) -> None: ...
        @property
        def version(self) -> int | None: ...
        def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

    throttle_time_ms: int
    filter_results: list[DeleteAclsFilterResult]
    def __init__(
        self,
        *args: Any,
        throttle_time_ms: int = ...,
        filter_results: list[DeleteAclsFilterResult] = ...,
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

class DescribeAclsRequest(ApiMessage):
    resource_type_filter: int
    resource_name_filter: str | None
    pattern_type_filter: int
    principal_filter: str | None
    host_filter: str | None
    operation: int
    permission_type: int
    def __init__(
        self,
        *args: Any,
        resource_type_filter: int = ...,
        resource_name_filter: str | None = ...,
        pattern_type_filter: int = ...,
        principal_filter: str | None = ...,
        host_filter: str | None = ...,
        operation: int = ...,
        permission_type: int = ...,
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

class DescribeAclsResponse(ApiMessage):
    class DescribeAclsResource(DataContainer):
        class AclDescription(DataContainer):
            principal: str
            host: str
            operation: int
            permission_type: int
            def __init__(
                self,
                *args: Any,
                principal: str = ...,
                host: str = ...,
                operation: int = ...,
                permission_type: int = ...,
                version: int | None = None,
                **kwargs: Any,
            ) -> None: ...
            @property
            def version(self) -> int | None: ...
            def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

        resource_type: int
        resource_name: str
        pattern_type: int
        acls: list[AclDescription]
        def __init__(
            self,
            *args: Any,
            resource_type: int = ...,
            resource_name: str = ...,
            pattern_type: int = ...,
            acls: list[AclDescription] = ...,
            version: int | None = None,
            **kwargs: Any,
        ) -> None: ...
        @property
        def version(self) -> int | None: ...
        def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

    throttle_time_ms: int
    error_code: int
    error_message: str | None
    resources: list[DescribeAclsResource]
    def __init__(
        self,
        *args: Any,
        throttle_time_ms: int = ...,
        error_code: int = ...,
        error_message: str | None = ...,
        resources: list[DescribeAclsResource] = ...,
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

class ACLResourceType(IntEnum):
    UNKNOWN: int
    ANY: int
    CLUSTER: int
    DELEGATION_TOKEN: int
    GROUP: int
    TOPIC: int
    TRANSACTIONAL_ID: int

class ACLOperation(IntEnum):
    UNKNOWN: int
    ANY: int
    ALL: int
    READ: int
    WRITE: int
    CREATE: int
    DELETE: int
    ALTER: int
    DESCRIBE: int
    CLUSTER_ACTION: int
    DESCRIBE_CONFIGS: int
    ALTER_CONFIGS: int
    IDEMPOTENT_WRITE: int
    CREATE_TOKENS: int
    DESCRIBE_TOKENS: int

class ACLPermissionType(IntEnum):
    UNKNOWN: int
    ANY: int
    DENY: int
    ALLOW: int

class ACLResourcePatternType(IntEnum):
    UNKNOWN: int
    ANY: int
    MATCH: int
    LITERAL: int
    PREFIXED: int
