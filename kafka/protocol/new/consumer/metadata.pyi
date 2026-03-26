import uuid
from typing import Any, Self

from kafka.protocol.new.api_data import ApiData
from kafka.protocol.new.data_container import DataContainer

__all__ = ['ConsumerProtocolSubscription', 'ConsumerProtocolAssignment', 'ConsumerProtocolType']

class ConsumerProtocolSubscription(ApiData):
    class TopicPartition(DataContainer):
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

    topics: list[str]
    user_data: bytes | None
    owned_partitions: list[TopicPartition]
    generation_id: int
    rack_id: str | None
    def __init__(
        self,
        *args: Any,
        topics: list[str] = ...,
        user_data: bytes | None = ...,
        owned_partitions: list[TopicPartition] = ...,
        generation_id: int = ...,
        rack_id: str | None = ...,
        version: int | None = None,
        **kwargs: Any,
    ) -> None: ...
    @property
    def version(self) -> int | None: ...
    def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...
    name: str
    type: str
    valid_versions: tuple[int, int]
    min_version: int
    max_version: int

class ConsumerProtocolAssignment(ApiData):
    class TopicPartition(DataContainer):
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

    assigned_partitions: list[TopicPartition]
    user_data: bytes | None
    def __init__(
        self,
        *args: Any,
        assigned_partitions: list[TopicPartition] = ...,
        user_data: bytes | None = ...,
        version: int | None = None,
        **kwargs: Any,
    ) -> None: ...
    @property
    def version(self) -> int | None: ...
    def to_dict(self, meta: bool = False, json: bool = True) -> dict: ...
    name: str
    type: str
    valid_versions: tuple[int, int]
    min_version: int
    max_version: int
    @property
    def assignment(self) -> Any: ...

ConsumerProtocolType: str
