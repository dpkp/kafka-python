from .struct import Struct
from .types import Array, Int16, Int32, Int64, Schema, String


class OffsetCommitResponse_v0(Struct):
    API_KEY = 8
    API_VERSION = 0
    SCHEMA = Schema(
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('error_code', Int16)))))
    )


class OffsetCommitResponse_v1(Struct):
    API_KEY = 8
    API_VERSION = 1
    SCHEMA = Schema(
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('error_code', Int16)))))
    )


class OffsetCommitResponse_v2(Struct):
    API_KEY = 8
    API_VERSION = 2
    SCHEMA = Schema(
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('error_code', Int16)))))
    )


class OffsetCommitRequest_v0(Struct):
    API_KEY = 8
    API_VERSION = 0 # Zookeeper-backed storage
    RESPONSE_TYPE = OffsetCommitResponse_v0
    SCHEMA = Schema(
        ('consumer_group', String('utf-8')),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('offset', Int64),
                ('metadata', String('utf-8'))))))
    )


class OffsetCommitRequest_v1(Struct):
    API_KEY = 8
    API_VERSION = 1 # Kafka-backed storage
    RESPONSE_TYPE = OffsetCommitResponse_v1
    SCHEMA = Schema(
        ('consumer_group', String('utf-8')),
        ('consumer_group_generation_id', Int32),
        ('consumer_id', String('utf-8')),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('offset', Int64),
                ('timestamp', Int64),
                ('metadata', String('utf-8'))))))
    )


class OffsetCommitRequest_v2(Struct):
    API_KEY = 8
    API_VERSION = 2 # added retention_time, dropped timestamp
    RESPONSE_TYPE = OffsetCommitResponse_v2
    SCHEMA = Schema(
        ('consumer_group', String('utf-8')),
        ('consumer_group_generation_id', Int32),
        ('consumer_id', String('utf-8')),
        ('retention_time', Int64),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('offset', Int64),
                ('metadata', String('utf-8'))))))
    )
    DEFAULT_GENERATION_ID = -1
    DEFAULT_RETENTION_TIME = -1


OffsetCommitRequest = [OffsetCommitRequest_v0, OffsetCommitRequest_v1,
                       OffsetCommitRequest_v2]
OffsetCommitResponse = [OffsetCommitResponse_v0, OffsetCommitResponse_v1,
                        OffsetCommitResponse_v2]


class OffsetFetchResponse_v0(Struct):
    API_KEY = 9
    API_VERSION = 0
    SCHEMA = Schema(
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('offset', Int64),
                ('metadata', String('utf-8')),
                ('error_code', Int16)))))
    )


class OffsetFetchResponse_v1(Struct):
    API_KEY = 9
    API_VERSION = 1
    SCHEMA = Schema(
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('offset', Int64),
                ('metadata', String('utf-8')),
                ('error_code', Int16)))))
    )


class OffsetFetchRequest_v0(Struct):
    API_KEY = 9
    API_VERSION = 0 # zookeeper-backed storage
    RESPONSE_TYPE = OffsetFetchResponse_v0
    SCHEMA = Schema(
        ('consumer_group', String('utf-8')),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(Int32))))
    )


class OffsetFetchRequest_v1(Struct):
    API_KEY = 9
    API_VERSION = 1 # kafka-backed storage
    RESPONSE_TYPE = OffsetFetchResponse_v1
    SCHEMA = Schema(
        ('consumer_group', String('utf-8')),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(Int32))))
    )


OffsetFetchRequest = [OffsetFetchRequest_v0, OffsetFetchRequest_v1]
OffsetFetchResponse = [OffsetFetchResponse_v0, OffsetFetchResponse_v1]


class GroupCoordinatorResponse_v0(Struct):
    API_KEY = 10
    API_VERSION = 0
    SCHEMA = Schema(
        ('error_code', Int16),
        ('coordinator_id', Int32),
        ('host', String('utf-8')),
        ('port', Int32)
    )


class GroupCoordinatorRequest_v0(Struct):
    API_KEY = 10
    API_VERSION = 0
    RESPONSE_TYPE = GroupCoordinatorResponse_v0
    SCHEMA = Schema(
        ('consumer_group', String('utf-8'))
    )


GroupCoordinatorRequest = [GroupCoordinatorRequest_v0]
GroupCoordinatorResponse = [GroupCoordinatorResponse_v0]
