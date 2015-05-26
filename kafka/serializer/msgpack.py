from __future__ import absolute_import

import logging

from kafka.serializer.abstract import AbstractSerializer

logger = logging.getLogger(__name__)

try:
    import msgpack
except ImportError:
    # Msgpack support not enabled
    logger.warning('msgpack module not found -- MsgpackSerializer disabled')
    pass


class MsgpackSerializer(AbstractSerializer):
    """
    Serialize / Deserialize using msgpack protocol
    """
    def __init__(self, is_key=False, **configs):
        self.is_key = is_key

        # Instead of a local import, we check whether global import succeeded
        # by checking the namespace
        if globals().get('msgpack') is None:
            raise ImportError('msgpack module not found')

    def serialize(self, topic, data):
        return msgpack.dumps(data)

    def deserialize(self, topic, data):
        return msgpack.loads(data)
