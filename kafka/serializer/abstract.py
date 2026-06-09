import abc


class Serializer(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def serialize(self, topic, data):
        pass

    def close(self):
        pass


class Deserializer(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def deserialize(self, topic, data):
        pass

    def close(self):
        pass
