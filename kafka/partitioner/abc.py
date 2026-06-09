import abc


class Partitioner(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def partition(self, topic, key, serialized_key, value, serialized_value, cluster):
        pass

