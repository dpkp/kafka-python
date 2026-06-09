from abc import ABC, abstractmethod
from typing import List, Tuple, Any


class Serializer(ABC):
    @abstractmethod
    def serialize(self, topic: str, headers: List[Tuple[str, bytes]], data: Any):
        pass

    def close(self):
        pass


class Deserializer(ABC):
    @abstractmethod
    def deserialize(self, topic: str, headers: List[Tuple[str, bytes]], data: bytes):
        pass

    def close(self):
        pass
