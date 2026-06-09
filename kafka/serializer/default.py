from .abstract import Serializer, Deserializer


class DefaultSerializer(Serializer, Deserializer):
    def __init__(self, encoding='utf-8'):
        self.encoding = encoding

    def serialize(self, topic, data):
        if type(data) in (bytes, bytearray, memoryview, type(None)):
            return data
        return data.encode(self.encoding)

    def deserialize(self, topic, data):
        if data is None:
            return None
        return data.decode(self.encoding)
