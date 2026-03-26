import uuid
from typing import Any, Self

from kafka.protocol.new.api_message import ApiMessage
from kafka.protocol.new.data_container import DataContainer

__all__ = ['UNKNOWN_OFFSET', 'OffsetResetStrategy', 'ListOffsetsRequest', 'ListOffsetsResponse', 'OffsetForLeaderEpochRequest', 'OffsetForLeaderEpochResponse']

UNKNOWN_OFFSET: int

class OffsetResetStrategy:
    LATEST: int
    EARLIEST: int
    NONE: int

class ListOffsetsRequest(ApiMessage):
    class ListOffsetsTopic(DataContainer):
        class ListOffsetsPartition(DataContainer):
            partition_index: int
            current_leader_epoch: int
            timestamp: int
            max_num_offsets: int
            def __init__(
                self,
                *args: Any,
                partition_index: int = ...,
                current_leader_epoch: int = ...,
                timestamp: int = ...,
                max_num_offsets: int = ...,
                version: int | None = None,
                **kwargs: Any,
            ) -> None: ...
            @property
            def version(self) -> int | None: ...
            def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

        name: str
        partitions: list[ListOffsetsPartition]
        def __init__(
            self,
            *args: Any,
            name: str = ...,
            partitions: list[ListOffsetsPartition] = ...,
            version: int | None = None,
            **kwargs: Any,
        ) -> None: ...
        @property
        def version(self) -> int | None: ...
        def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

    replica_id: int
    isolation_level: int
    topics: list[ListOffsetsTopic]
    timeout_ms: int
    def __init__(
        self,
        *args: Any,
        replica_id: int = ...,
        isolation_level: int = ...,
        topics: list[ListOffsetsTopic] = ...,
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

class ListOffsetsResponse(ApiMessage):
    class ListOffsetsTopicResponse(DataContainer):
        class ListOffsetsPartitionResponse(DataContainer):
            partition_index: int
            error_code: int
            old_style_offsets: list[int]
            timestamp: int
            offset: int
            leader_epoch: int
            def __init__(
                self,
                *args: Any,
                partition_index: int = ...,
                error_code: int = ...,
                old_style_offsets: list[int] = ...,
                timestamp: int = ...,
                offset: int = ...,
                leader_epoch: int = ...,
                version: int | None = None,
                **kwargs: Any,
            ) -> None: ...
            @property
            def version(self) -> int | None: ...
            def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

        name: str
        partitions: list[ListOffsetsPartitionResponse]
        def __init__(
            self,
            *args: Any,
            name: str = ...,
            partitions: list[ListOffsetsPartitionResponse] = ...,
            version: int | None = None,
            **kwargs: Any,
        ) -> None: ...
        @property
        def version(self) -> int | None: ...
        def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

    throttle_time_ms: int
    topics: list[ListOffsetsTopicResponse]
    def __init__(
        self,
        *args: Any,
        throttle_time_ms: int = ...,
        topics: list[ListOffsetsTopicResponse] = ...,
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

class OffsetForLeaderEpochRequest(ApiMessage):
    class OffsetForLeaderTopic(DataContainer):
        class OffsetForLeaderPartition(DataContainer):
            partition: int
            current_leader_epoch: int
            leader_epoch: int
            def __init__(
                self,
                *args: Any,
                partition: int = ...,
                current_leader_epoch: int = ...,
                leader_epoch: int = ...,
                version: int | None = None,
                **kwargs: Any,
            ) -> None: ...
            @property
            def version(self) -> int | None: ...
            def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

        topic: str
        partitions: list[OffsetForLeaderPartition]
        def __init__(
            self,
            *args: Any,
            topic: str = ...,
            partitions: list[OffsetForLeaderPartition] = ...,
            version: int | None = None,
            **kwargs: Any,
        ) -> None: ...
        @property
        def version(self) -> int | None: ...
        def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

    replica_id: int
    topics: list[OffsetForLeaderTopic]
    def __init__(
        self,
        *args: Any,
        replica_id: int = ...,
        topics: list[OffsetForLeaderTopic] = ...,
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

class OffsetForLeaderEpochResponse(ApiMessage):
    class OffsetForLeaderTopicResult(DataContainer):
        class EpochEndOffset(DataContainer):
            error_code: int
            partition: int
            leader_epoch: int
            end_offset: int
            def __init__(
                self,
                *args: Any,
                error_code: int = ...,
                partition: int = ...,
                leader_epoch: int = ...,
                end_offset: int = ...,
                version: int | None = None,
                **kwargs: Any,
            ) -> None: ...
            @property
            def version(self) -> int | None: ...
            def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

        topic: str
        partitions: list[EpochEndOffset]
        def __init__(
            self,
            *args: Any,
            topic: str = ...,
            partitions: list[EpochEndOffset] = ...,
            version: int | None = None,
            **kwargs: Any,
        ) -> None: ...
        @property
        def version(self) -> int | None: ...
        def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

    throttle_time_ms: int
    topics: list[OffsetForLeaderTopicResult]
    def __init__(
        self,
        *args: Any,
        throttle_time_ms: int = ...,
        topics: list[OffsetForLeaderTopicResult] = ...,
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
