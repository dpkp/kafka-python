import uuid
from typing import Any, Self

from kafka.protocol.new.api_message import ApiMessage
from kafka.protocol.new.data_container import DataContainer

__all__ = ['DEFAULT_GENERATION_ID', 'UNKNOWN_MEMBER_ID', 'JoinGroupRequest', 'JoinGroupResponse', 'SyncGroupRequest', 'SyncGroupResponse', 'LeaveGroupRequest', 'LeaveGroupResponse', 'HeartbeatRequest', 'HeartbeatResponse', 'OffsetFetchRequest', 'OffsetFetchResponse', 'OffsetCommitRequest', 'OffsetCommitResponse']

DEFAULT_GENERATION_ID: int

UNKNOWN_MEMBER_ID: str

class JoinGroupRequest(ApiMessage):
    class JoinGroupRequestProtocol(DataContainer):
        name: str
        metadata: bytes
        def __init__(
            self,
            *args: Any,
            name: str = ...,
            metadata: bytes = ...,
            version: int | None = None,
            **kwargs: Any,
        ) -> None: ...
        @property
        def version(self) -> int | None: ...
        def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

    group_id: str
    session_timeout_ms: int
    rebalance_timeout_ms: int
    member_id: str
    group_instance_id: str | None
    protocol_type: str
    protocols: list[JoinGroupRequestProtocol]
    reason: str | None
    def __init__(
        self,
        *args: Any,
        group_id: str = ...,
        session_timeout_ms: int = ...,
        rebalance_timeout_ms: int = ...,
        member_id: str = ...,
        group_instance_id: str | None = ...,
        protocol_type: str = ...,
        protocols: list[JoinGroupRequestProtocol] = ...,
        reason: str | None = ...,
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

class JoinGroupResponse(ApiMessage):
    class JoinGroupResponseMember(DataContainer):
        member_id: str
        group_instance_id: str | None
        metadata: bytes
        def __init__(
            self,
            *args: Any,
            member_id: str = ...,
            group_instance_id: str | None = ...,
            metadata: bytes = ...,
            version: int | None = None,
            **kwargs: Any,
        ) -> None: ...
        @property
        def version(self) -> int | None: ...
        def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

    throttle_time_ms: int
    error_code: int
    generation_id: int
    protocol_type: str | None
    protocol_name: str | None
    leader: str
    skip_assignment: bool
    member_id: str
    members: list[JoinGroupResponseMember]
    def __init__(
        self,
        *args: Any,
        throttle_time_ms: int = ...,
        error_code: int = ...,
        generation_id: int = ...,
        protocol_type: str | None = ...,
        protocol_name: str | None = ...,
        leader: str = ...,
        skip_assignment: bool = ...,
        member_id: str = ...,
        members: list[JoinGroupResponseMember] = ...,
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

class SyncGroupRequest(ApiMessage):
    class SyncGroupRequestAssignment(DataContainer):
        member_id: str
        assignment: bytes
        def __init__(
            self,
            *args: Any,
            member_id: str = ...,
            assignment: bytes = ...,
            version: int | None = None,
            **kwargs: Any,
        ) -> None: ...
        @property
        def version(self) -> int | None: ...
        def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

    group_id: str
    generation_id: int
    member_id: str
    group_instance_id: str | None
    protocol_type: str | None
    protocol_name: str | None
    assignments: list[SyncGroupRequestAssignment]
    def __init__(
        self,
        *args: Any,
        group_id: str = ...,
        generation_id: int = ...,
        member_id: str = ...,
        group_instance_id: str | None = ...,
        protocol_type: str | None = ...,
        protocol_name: str | None = ...,
        assignments: list[SyncGroupRequestAssignment] = ...,
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

class SyncGroupResponse(ApiMessage):
    throttle_time_ms: int
    error_code: int
    protocol_type: str | None
    protocol_name: str | None
    assignment: bytes
    def __init__(
        self,
        *args: Any,
        throttle_time_ms: int = ...,
        error_code: int = ...,
        protocol_type: str | None = ...,
        protocol_name: str | None = ...,
        assignment: bytes = ...,
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

class LeaveGroupRequest(ApiMessage):
    class MemberIdentity(DataContainer):
        member_id: str
        group_instance_id: str | None
        reason: str | None
        def __init__(
            self,
            *args: Any,
            member_id: str = ...,
            group_instance_id: str | None = ...,
            reason: str | None = ...,
            version: int | None = None,
            **kwargs: Any,
        ) -> None: ...
        @property
        def version(self) -> int | None: ...
        def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

    group_id: str
    member_id: str
    members: list[MemberIdentity]
    def __init__(
        self,
        *args: Any,
        group_id: str = ...,
        member_id: str = ...,
        members: list[MemberIdentity] = ...,
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

class LeaveGroupResponse(ApiMessage):
    class MemberResponse(DataContainer):
        member_id: str
        group_instance_id: str | None
        error_code: int
        def __init__(
            self,
            *args: Any,
            member_id: str = ...,
            group_instance_id: str | None = ...,
            error_code: int = ...,
            version: int | None = None,
            **kwargs: Any,
        ) -> None: ...
        @property
        def version(self) -> int | None: ...
        def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

    throttle_time_ms: int
    error_code: int
    members: list[MemberResponse]
    def __init__(
        self,
        *args: Any,
        throttle_time_ms: int = ...,
        error_code: int = ...,
        members: list[MemberResponse] = ...,
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

class HeartbeatRequest(ApiMessage):
    group_id: str
    generation_id: int
    member_id: str
    group_instance_id: str | None
    def __init__(
        self,
        *args: Any,
        group_id: str = ...,
        generation_id: int = ...,
        member_id: str = ...,
        group_instance_id: str | None = ...,
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

class HeartbeatResponse(ApiMessage):
    throttle_time_ms: int
    error_code: int
    def __init__(
        self,
        *args: Any,
        throttle_time_ms: int = ...,
        error_code: int = ...,
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

class OffsetFetchRequest(ApiMessage):
    class OffsetFetchRequestTopic(DataContainer):
        name: str
        partition_indexes: list[int]
        def __init__(
            self,
            *args: Any,
            name: str = ...,
            partition_indexes: list[int] = ...,
            version: int | None = None,
            **kwargs: Any,
        ) -> None: ...
        @property
        def version(self) -> int | None: ...
        def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

    class OffsetFetchRequestGroup(DataContainer):
        class OffsetFetchRequestTopics(DataContainer):
            name: str
            partition_indexes: list[int]
            def __init__(
                self,
                *args: Any,
                name: str = ...,
                partition_indexes: list[int] = ...,
                version: int | None = None,
                **kwargs: Any,
            ) -> None: ...
            @property
            def version(self) -> int | None: ...
            def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

        group_id: str
        member_id: str | None
        member_epoch: int
        topics: list[OffsetFetchRequestTopics] | None
        def __init__(
            self,
            *args: Any,
            group_id: str = ...,
            member_id: str | None = ...,
            member_epoch: int = ...,
            topics: list[OffsetFetchRequestTopics] | None = ...,
            version: int | None = None,
            **kwargs: Any,
        ) -> None: ...
        @property
        def version(self) -> int | None: ...
        def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

    group_id: str
    topics: list[OffsetFetchRequestTopic] | None
    groups: list[OffsetFetchRequestGroup]
    require_stable: bool
    def __init__(
        self,
        *args: Any,
        group_id: str = ...,
        topics: list[OffsetFetchRequestTopic] | None = ...,
        groups: list[OffsetFetchRequestGroup] = ...,
        require_stable: bool = ...,
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

class OffsetFetchResponse(ApiMessage):
    class OffsetFetchResponseTopic(DataContainer):
        class OffsetFetchResponsePartition(DataContainer):
            partition_index: int
            committed_offset: int
            committed_leader_epoch: int
            metadata: str | None
            error_code: int
            def __init__(
                self,
                *args: Any,
                partition_index: int = ...,
                committed_offset: int = ...,
                committed_leader_epoch: int = ...,
                metadata: str | None = ...,
                error_code: int = ...,
                version: int | None = None,
                **kwargs: Any,
            ) -> None: ...
            @property
            def version(self) -> int | None: ...
            def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

        name: str
        partitions: list[OffsetFetchResponsePartition]
        def __init__(
            self,
            *args: Any,
            name: str = ...,
            partitions: list[OffsetFetchResponsePartition] = ...,
            version: int | None = None,
            **kwargs: Any,
        ) -> None: ...
        @property
        def version(self) -> int | None: ...
        def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

    class OffsetFetchResponseGroup(DataContainer):
        class OffsetFetchResponseTopics(DataContainer):
            class OffsetFetchResponsePartitions(DataContainer):
                partition_index: int
                committed_offset: int
                committed_leader_epoch: int
                metadata: str | None
                error_code: int
                def __init__(
                    self,
                    *args: Any,
                    partition_index: int = ...,
                    committed_offset: int = ...,
                    committed_leader_epoch: int = ...,
                    metadata: str | None = ...,
                    error_code: int = ...,
                    version: int | None = None,
                    **kwargs: Any,
                ) -> None: ...
                @property
                def version(self) -> int | None: ...
                def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

            name: str
            partitions: list[OffsetFetchResponsePartitions]
            def __init__(
                self,
                *args: Any,
                name: str = ...,
                partitions: list[OffsetFetchResponsePartitions] = ...,
                version: int | None = None,
                **kwargs: Any,
            ) -> None: ...
            @property
            def version(self) -> int | None: ...
            def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

        group_id: str
        topics: list[OffsetFetchResponseTopics]
        error_code: int
        def __init__(
            self,
            *args: Any,
            group_id: str = ...,
            topics: list[OffsetFetchResponseTopics] = ...,
            error_code: int = ...,
            version: int | None = None,
            **kwargs: Any,
        ) -> None: ...
        @property
        def version(self) -> int | None: ...
        def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

    throttle_time_ms: int
    topics: list[OffsetFetchResponseTopic]
    error_code: int
    groups: list[OffsetFetchResponseGroup]
    def __init__(
        self,
        *args: Any,
        throttle_time_ms: int = ...,
        topics: list[OffsetFetchResponseTopic] = ...,
        error_code: int = ...,
        groups: list[OffsetFetchResponseGroup] = ...,
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

class OffsetCommitRequest(ApiMessage):
    class OffsetCommitRequestTopic(DataContainer):
        class OffsetCommitRequestPartition(DataContainer):
            partition_index: int
            committed_offset: int
            committed_leader_epoch: int
            commit_timestamp: int
            committed_metadata: str | None
            def __init__(
                self,
                *args: Any,
                partition_index: int = ...,
                committed_offset: int = ...,
                committed_leader_epoch: int = ...,
                commit_timestamp: int = ...,
                committed_metadata: str | None = ...,
                version: int | None = None,
                **kwargs: Any,
            ) -> None: ...
            @property
            def version(self) -> int | None: ...
            def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

        name: str
        partitions: list[OffsetCommitRequestPartition]
        def __init__(
            self,
            *args: Any,
            name: str = ...,
            partitions: list[OffsetCommitRequestPartition] = ...,
            version: int | None = None,
            **kwargs: Any,
        ) -> None: ...
        @property
        def version(self) -> int | None: ...
        def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

    group_id: str
    generation_id_or_member_epoch: int
    member_id: str
    group_instance_id: str | None
    retention_time_ms: int
    topics: list[OffsetCommitRequestTopic]
    def __init__(
        self,
        *args: Any,
        group_id: str = ...,
        generation_id_or_member_epoch: int = ...,
        member_id: str = ...,
        group_instance_id: str | None = ...,
        retention_time_ms: int = ...,
        topics: list[OffsetCommitRequestTopic] = ...,
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

class OffsetCommitResponse(ApiMessage):
    class OffsetCommitResponseTopic(DataContainer):
        class OffsetCommitResponsePartition(DataContainer):
            partition_index: int
            error_code: int
            def __init__(
                self,
                *args: Any,
                partition_index: int = ...,
                error_code: int = ...,
                version: int | None = None,
                **kwargs: Any,
            ) -> None: ...
            @property
            def version(self) -> int | None: ...
            def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

        name: str
        partitions: list[OffsetCommitResponsePartition]
        def __init__(
            self,
            *args: Any,
            name: str = ...,
            partitions: list[OffsetCommitResponsePartition] = ...,
            version: int | None = None,
            **kwargs: Any,
        ) -> None: ...
        @property
        def version(self) -> int | None: ...
        def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

    throttle_time_ms: int
    topics: list[OffsetCommitResponseTopic]
    def __init__(
        self,
        *args: Any,
        throttle_time_ms: int = ...,
        topics: list[OffsetCommitResponseTopic] = ...,
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
