import uuid
from typing import Any, Self

from kafka.protocol.new.api_message import ApiMessage
from kafka.protocol.new.api_data import ApiData
from kafka.protocol.new.data_container import DataContainer

__all__ = ['ProduceRequest', 'ProduceResponse']

class ProduceRequest(ApiMessage):
    class TopicProduceData(DataContainer):
        class PartitionProduceData(DataContainer):
            index: int
            records: bytes | ApiData | None
            def __init__(
                self,
                *args: Any,
                index: int = ...,
                records: bytes | ApiData | None = ...,
                version: int | None = None,
                **kwargs: Any,
            ) -> None: ...
            @property
            def version(self) -> int | None: ...
            def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

        name: str
        partition_data: list[PartitionProduceData]
        def __init__(
            self,
            *args: Any,
            name: str = ...,
            partition_data: list[PartitionProduceData] = ...,
            version: int | None = None,
            **kwargs: Any,
        ) -> None: ...
        @property
        def version(self) -> int | None: ...
        def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

    transactional_id: str | None
    acks: int
    timeout_ms: int
    topic_data: list[TopicProduceData]
    def __init__(
        self,
        *args: Any,
        transactional_id: str | None = ...,
        acks: int = ...,
        timeout_ms: int = ...,
        topic_data: list[TopicProduceData] = ...,
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

class ProduceResponse(ApiMessage):
    class TopicProduceResponse(DataContainer):
        class PartitionProduceResponse(DataContainer):
            class BatchIndexAndErrorMessage(DataContainer):
                batch_index: int
                batch_index_error_message: str | None
                def __init__(
                    self,
                    *args: Any,
                    batch_index: int = ...,
                    batch_index_error_message: str | None = ...,
                    version: int | None = None,
                    **kwargs: Any,
                ) -> None: ...
                @property
                def version(self) -> int | None: ...
                def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

            class LeaderIdAndEpoch(DataContainer):
                leader_id: int
                leader_epoch: int
                def __init__(
                    self,
                    *args: Any,
                    leader_id: int = ...,
                    leader_epoch: int = ...,
                    version: int | None = None,
                    **kwargs: Any,
                ) -> None: ...
                @property
                def version(self) -> int | None: ...
                def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

            index: int
            error_code: int
            base_offset: int
            log_append_time_ms: int
            log_start_offset: int
            record_errors: list[BatchIndexAndErrorMessage]
            error_message: str | None
            current_leader: LeaderIdAndEpoch
            def __init__(
                self,
                *args: Any,
                index: int = ...,
                error_code: int = ...,
                base_offset: int = ...,
                log_append_time_ms: int = ...,
                log_start_offset: int = ...,
                record_errors: list[BatchIndexAndErrorMessage] = ...,
                error_message: str | None = ...,
                current_leader: LeaderIdAndEpoch = ...,
                version: int | None = None,
                **kwargs: Any,
            ) -> None: ...
            @property
            def version(self) -> int | None: ...
            def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

        name: str
        partition_responses: list[PartitionProduceResponse]
        def __init__(
            self,
            *args: Any,
            name: str = ...,
            partition_responses: list[PartitionProduceResponse] = ...,
            version: int | None = None,
            **kwargs: Any,
        ) -> None: ...
        @property
        def version(self) -> int | None: ...
        def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

    class NodeEndpoint(DataContainer):
        node_id: int
        host: str
        port: int
        rack: str | None
        def __init__(
            self,
            *args: Any,
            node_id: int = ...,
            host: str = ...,
            port: int = ...,
            rack: str | None = ...,
            version: int | None = None,
            **kwargs: Any,
        ) -> None: ...
        @property
        def version(self) -> int | None: ...
        def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...

    responses: list[TopicProduceResponse]
    throttle_time_ms: int
    node_endpoints: list[NodeEndpoint]
    def __init__(
        self,
        *args: Any,
        responses: list[TopicProduceResponse] = ...,
        throttle_time_ms: int = ...,
        node_endpoints: list[NodeEndpoint] = ...,
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
