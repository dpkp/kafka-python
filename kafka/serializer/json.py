from .abstract import Serializer, Deserializer


class JsonSerializer(Serializer, Deserializer):
    def serialize(self, topic, data):
        if data is None:
            return None
        s = json.dumps(data)
        return super().serialize(topic, s)

    def deserialize(self, topic, data):
        if data is None:
            return None
        s = super().deserialize(data)
        return json.loads(s)
