import json

from .default import DefaultSerializer


class JsonSerializer(DefaultSerializer):
    def serialize(self, topic, headers, data):
        if data is None:
            return None
        s = json.dumps(data)
        return super().serialize(topic, headers, s)

    def deserialize(self, topic, headers, data):
        if data is None:
            return None
        s = super().deserialize(topic, headers, data)
        return json.loads(s)
