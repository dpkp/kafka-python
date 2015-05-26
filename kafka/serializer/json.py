from __future__ import absolute_import

import json

from kafka.serializer.abstract import AbstractSerializer


class JsonSerializer(AbstractSerializer):
    """
    Serialize / Deserialize using json protocol
    """
    def __init__(self, is_key=False, **configs):
        self.is_key = is_key

    def serialize(self, topic, data):
        return json.dumps(data)

    def deserialize(self, topic, data):
        return json.loads(data)
