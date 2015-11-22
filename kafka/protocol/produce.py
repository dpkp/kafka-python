from .api import AbstractRequest, AbstractResponse, MessageSet
from .types import Int8, Int16, Int32, Int64, Bytes, String, Array


class ProduceRequest(AbstractRequest):
    API_KEY = 0
    API_VERSION = 0
    __slots__ = ('required_acks', 'timeout', 'topic_partition_messages', 'compression')

    def __init__(self, topic_partition_messages,
                 required_acks=-1, timeout=1000, compression=None):
        """
        topic_partition_messages is a dict of dicts of lists (of messages)
        {
          "TopicFoo": {
            0: [
              Message('foo'),
              Message('bar')
            ],
            1: [
              Message('fizz'),
              Message('buzz')
            ]
          }
        }
        """
        self.required_acks = required_acks
        self.timeout = timeout
        self.topic_partition_messages = topic_partition_messages
        self.compression = compression

    @staticmethod
    def _encode_messages(partition, messages, compression):
        message_set = MessageSet.encode(messages)

        if compression:
            # compress message_set data and re-encode as single message
            # then wrap single compressed message in a new message_set
            pass

        return (Int32.encode(partition) +
                Int32.encode(len(message_set)) +
                message_set)

    def encode(self):
        request = (
            Int16.encode(self.required_acks) +
            Int32.encode(self.timeout) +
            Array.encode([(
                String.encode(topic) +
                Array.encode([
                    self._encode_messages(partition, messages, self.compression)
                    for partition, messages in partitions.iteritems()])
            ) for topic, partitions in self.topic_partition_messages.iteritems()])
        )
        return super(ProduceRequest, self).encode(request)



