from abc import ABC, abstractmethod


class Partitioner(ABC):
    @abstractmethod
    def partition(self, topic, key, serialized_key, value, serialized_value, cluster):
        pass

