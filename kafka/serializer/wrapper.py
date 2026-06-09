from .abstract import Deserializer


class DeserializeWrapper(Deserializer):
    def __init__(self, fn):
        self.fn = fn

    def deserialize(self, topic, headers, data):
        if self.fn is None:
            return data
        return self.fn(data)
