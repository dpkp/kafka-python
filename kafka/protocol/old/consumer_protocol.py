from .struct import Struct
from .types import Array, Bytes, Int16, Int32, Schema, String
from kafka.structs import TopicPartition


class ConsumerProtocolMemberMetadata_v0(Struct):
    SCHEMA = Schema(
        ('version', Int16),
        ('topics', Array(String('utf-8'))),
        ('user_data', Bytes))


class ConsumerProtocolMemberAssignment_v0(Struct):
    SCHEMA = Schema(
        ('version', Int16),
        ('assigned_partitions', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(Int32)))),
        ('user_data', Bytes))
    ALIASES = {
        'assignment': 'assigned_partitions',
    }

    def partitions(self):
        return [TopicPartition(topic, partition)
                for topic, partitions in self.assigned_partitions
                for partition in partitions]


class ConsumerProtocol_v0:
    PROTOCOL_TYPE = 'consumer'
    METADATA = ConsumerProtocolMemberMetadata_v0
    ASSIGNMENT = ConsumerProtocolMemberAssignment_v0


ConsumerProtocol = [ConsumerProtocol_v0]
