import struct

from .types import (
    Int8, Int16, Int32, Int64, Bytes, String, Array
)
from ..util import crc32


class Message(object):
    MAGIC_BYTE = 0
    __slots__ = ('magic', 'attributes', 'key', 'value')

    def __init__(self, value, key=None, magic=0, attributes=0):
        self.magic = magic
        self.attributes = attributes
        self.key = key
        self.value = value

    def encode(self):
        message = (
            Int8.encode(self.magic) +
            Int8.encode(self.attributes) +
            Bytes.encode(self.key) +
            Bytes.encode(self.value)
        )
        return (
            struct.pack('>I', crc32(message)) +
            message
        )


class MessageSet(object):

    @staticmethod
    def _encode_one(message):
        encoded = message.encode()
        return (Int64.encode(0) + Int32.encode(len(encoded)) + encoded)

    @staticmethod
    def encode(messages):
        return b''.join(map(MessageSet._encode_one, messages))


class AbstractRequestResponse(object):
    @classmethod
    def encode(cls, message):
        return Int32.encode(len(message)) + message


class AbstractRequest(AbstractRequestResponse):
    @classmethod
    def encode(cls, request, correlation_id=0, client_id='kafka-python'):
        request = (Int16.encode(cls.API_KEY) +
                   Int16.encode(cls.API_VERSION) +
                   Int32.encode(correlation_id) +
                   String.encode(client_id) +
                   request)
        return super(AbstractRequest, cls).encode(request)


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


class FetchRequest(AbstractRequest):
    API_KEY = 1
    API_VERSION = 0
    __slots__ = ('replica_id', 'max_wait_time', 'min_bytes', 'topic_partition_offsets')

    def __init__(self, topic_partition_offsets,
                 max_wait_time=-1, min_bytes=0, replica_id=-1):
        """
        topic_partition_offsets is a dict of dicts of (offset, max_bytes) tuples
        {
          "TopicFoo": {
            0: (1234, 1048576),
            1: (1324, 1048576)
          }
        }
        """
        self.topic_partition_offsets = topic_partition_offsets
        self.max_wait_time = max_wait_time
        self.min_bytes = min_bytes
        self.replica_id = replica_id

    def encode(self):
        request = (
            Int32.encode(self.replica_id) +
            Int32.encode(self.max_wait_time) +
            Int32.encode(self.min_bytes) +
            Array.encode([(
                String.encode(topic) +
                Array.encode([(
                    Int32.encode(partition) +
                    Int64.encode(offset) +
                    Int32.encode(max_bytes)
                ) for partition, (offset, max_bytes) in partitions.iteritems()])
            ) for topic, partitions in self.topic_partition_offsets.iteritems()]))
        return super(FetchRequest, self).encode(request)


class OffsetRequest(AbstractRequest):
    API_KEY = 2
    API_VERSION = 0
    __slots__ = ('replica_id', 'topic_partition_times')

    def __init__(self, topic_partition_times, replica_id=-1):
        """
        topic_partition_times is a dict of dicts of (time, max_offsets) tuples
        {
          "TopicFoo": {
            0: (-1, 1),
            1: (-1, 1)
          }
        }
        """
        self.topic_partition_times = topic_partition_times
        self.replica_id = replica_id

    def encode(self):
        request = (
            Int32.encode(self.replica_id) +
            Array.encode([(
                String.encode(topic) +
                Array.encode([(
                    Int32.encode(partition) +
                    Int64.encode(time) +
                    Int32.encode(max_offsets)
                ) for partition, (time, max_offsets) in partitions.iteritems()])
            ) for topic, partitions in self.topic_partition_times.iteritems()]))
        return super(OffsetRequest, self).encode(request)


class MetadataRequest(AbstractRequest):
    API_KEY = 3
    API_VERSION = 0
    __slots__ = ('topics')

    def __init__(self, *topics):
        self.topics = topics

    def encode(self):
        request = Array.encode(map(String.encode, self.topics))
        return super(MetadataRequest, self).encode(request)


# Non-user facing control APIs 4-7


class OffsetCommitRequestV0(AbstractRequest):
    API_KEY = 8
    API_VERSION = 0
    __slots__ = ('consumer_group_id', 'offsets')

    def __init__(self, consumer_group_id, offsets):
        """
        offsets is a dict of dicts of (offset, metadata) tuples
        {
          "TopicFoo": {
            0: (1234, ""),
            1: (1243, "")
          }
        }
        """
        self.consumer_group_id = consumer_group_id
        self.offsets = offsets

    def encode(self):
        request = (
            String.encode(self.consumer_group_id) +
            Array.encode([(
                String.encode(topic) +
                Array.encode([(
                    Int32.encode(partition) +
                    Int64.encode(offset) +
                    String.encode(metadata)
                ) for partition, (offset, metadata) in partitions.iteritems()])
            ) for topic, partitions in self.offsets.iteritems()]))
        return super(OffsetCommitRequestV0, self).encode(request)


class OffsetCommitRequestV1(AbstractRequest):
    API_KEY = 8
    API_VERSION = 1
    __slots__ = ('consumer_group_id', 'consumer_group_generation_id',
                 'consumer_id', 'offsets')

    def __init__(self, consumer_group_id, consumer_group_generation_id,
                 consumer_id, offsets):
        """
        offsets is a dict of dicts of (offset, timestamp, metadata) tuples
        {
          "TopicFoo": {
            0: (1234, 1448198827, ""),
            1: (1243, 1448198827, "")
          }
        }
        """
        self.consumer_group_id = consumer_group_id
        self.consumer_group_generation_id = consumer_group_generation_id
        self.consumer_id = consumer_id
        self.offsets = offsets

    def encode(self):
        request = (
            String.encode(self.consumer_group_id) +
            Int32.encode(self.consumer_group_generation_id) +
            String.encode(self.consumer_id) +
            Array.encode([(
                String.encode(topic) +
                Array.encode([(
                    Int32.encode(partition) +
                    Int64.encode(offset) +
                    Int64.encode(timestamp) +
                    String.encode(metadata)
                ) for partition, (offset, timestamp, metadata) in partitions.iteritems()])
            ) for topic, partitions in self.offsets.iteritems()]))
        return super(OffsetCommitRequestV1, self).encode(request)


class OffsetCommitRequest(AbstractRequest):
    API_KEY = 8
    API_VERSION = 2
    __slots__ = ('consumer_group_id', 'consumer_group_generation_id',
                 'consumer_id', 'retention_time', 'offsets')

    def __init__(self, consumer_group_id, consumer_group_generation_id,
                 consumer_id, retention_time, offsets):
        """
        offsets is a dict of dicts of (offset, metadata) tuples
        {
          "TopicFoo": {
            0: (1234, ""),
            1: (1243, "")
          }
        }
        """
        self.consumer_group_id = consumer_group_id
        self.consumer_group_generation_id = consumer_group_generation_id
        self.consumer_id = consumer_id
        self.retention_time = retention_time
        self.offsets = offsets

    def encode(self):
        request = (
            String.encode(self.consumer_group_id) +
            Int32.encode(self.consumer_group_generation_id) +
            String.encode(self.consumer_id) +
            Int64.encode(self.retention_time) +
            Array.encode([(
                String.encode(topic) +
                Array.encode([(
                    Int32.encode(partition) +
                    Int64.encode(offset) +
                    String.encode(metadata)
                ) for partition, (offset, timestamp, metadata) in partitions.iteritems()])
            ) for topic, partitions in self.offsets.iteritems()]))
        return super(OffsetCommitRequest, self).encode(request)


class OffsetFetchRequestV0(AbstractRequest):
    API_KEY = 9
    API_VERSION = 0
    __slots__ = ('consumer_group', 'topic_partitions')

    def __init__(self, consumer_group, topic_partitions):
        """
        offsets is a dict of lists of partition ints
        {
          "TopicFoo": [0, 1, 2]
        }
        """
        self.consumer_group = consumer_group
        self.topic_partitions = topic_partitions

    def encode(self):
        request = (
            String.encode(self.consumer_group) +
            Array.encode([(
                String.encode(topic) +
                Array.encode([Int32.encode(partition) for partition in partitions])
            ) for topic, partitions in self.topic_partitions.iteritems()])
        )
        return super(OffsetFetchRequest, self).encode(request)


class OffsetFetchRequest(OffsetFetchRequestV0):
    """Identical to V0, but offsets fetched from kafka storage not zookeeper"""
    API_VERSION = 1


class GroupCoordinatorRequest(AbstractRequest):
    API_KEY = 10
    API_VERSION = 0
    __slots__ = ('group_id',)

    def __init__(self, group_id):
        self.group_id = group_id

    def encode(self):
        request = String.encode(self.group_id)
        return super(GroupCoordinatorRequest, self).encode(request)



