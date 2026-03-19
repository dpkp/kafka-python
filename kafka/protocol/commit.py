from kafka.protocol.api import Request, Response
from kafka.protocol.types import Array, Int16, Int32, Int64, Schema, String


class OffsetCommitResponse_v0(Response):
    API_KEY = 8
    API_VERSION = 0
    SCHEMA = Schema(
        ('topics', Array(
            ('name', String('utf-8')),
            ('partitions', Array(
                ('partition_index', Int32),
                ('error_code', Int16)))))
    )


class OffsetCommitResponse_v1(Response):
    API_KEY = 8
    API_VERSION = 1
    SCHEMA = OffsetCommitResponse_v0.SCHEMA


class OffsetCommitResponse_v2(Response):
    API_KEY = 8
    API_VERSION = 2
    SCHEMA = OffsetCommitResponse_v1.SCHEMA


class OffsetCommitResponse_v3(Response):
    API_KEY = 8
    API_VERSION = 3
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('topics', Array(
            ('name', String('utf-8')),
            ('partitions', Array(
                ('partition_index', Int32),
                ('error_code', Int16)))))
    )


class OffsetCommitResponse_v4(Response):
    API_KEY = 8
    API_VERSION = 4
    SCHEMA = OffsetCommitResponse_v3.SCHEMA


class OffsetCommitResponse_v5(Response):
    API_KEY = 8
    API_VERSION = 5
    SCHEMA = OffsetCommitResponse_v4.SCHEMA


class OffsetCommitResponse_v6(Response):
    API_KEY = 8
    API_VERSION = 6
    SCHEMA = OffsetCommitResponse_v5.SCHEMA


class OffsetCommitResponse_v7(Response):
    API_KEY = 8
    API_VERSION = 7
    SCHEMA = OffsetCommitResponse_v6.SCHEMA


class OffsetCommitRequest_v0(Request):
    API_KEY = 8
    API_VERSION = 0  # Zookeeper-backed storage
    SCHEMA = Schema(
        ('group_id', String('utf-8')),
        ('topics', Array(
            ('name', String('utf-8')),
            ('partitions', Array(
                ('partition_index', Int32),
                ('committed_offset', Int64),
                ('committed_metadata', String('utf-8'))))))
    )
    ALIASES = {
        'consumer_group': 'group_id',
    }


class OffsetCommitRequest_v1(Request):
    API_KEY = 8
    API_VERSION = 1  # Kafka-backed storage
    SCHEMA = Schema(
        ('group_id', String('utf-8')),
        ('generation_id_or_member_epoch', Int32),
        ('member_id', String('utf-8')),
        ('topics', Array(
            ('name', String('utf-8')),
            ('partitions', Array(
                ('partition_index', Int32),
                ('committed_offset', Int64),
                ('commit_timestamp', Int64),
                ('committed_metadata', String('utf-8'))))))
    )
    ALIASES = {
        'consumer_group': 'group_id',
        'consumer_group_generation_id': 'generation_id_or_member_epoch',
        'consumer_id': 'member_id',
    }


class OffsetCommitRequest_v2(Request):
    API_KEY = 8
    API_VERSION = 2
    SCHEMA = Schema(
        ('group_id', String('utf-8')),
        ('generation_id_or_member_epoch', Int32),
        ('member_id', String('utf-8')),
        ('retention_time_ms', Int64), # added retention_time_ms, dropped timestamp
        ('topics', Array(
            ('name', String('utf-8')),
            ('partitions', Array(
                ('partition_index', Int32),
                ('committed_offset', Int64),
                ('committed_metadata', String('utf-8'))))))
    )
    DEFAULT_RETENTION_TIME = -1
    ALIASES = {
        'consumer_group': 'group_id',
        'consumer_group_generation_id': 'generation_id_or_member_epoch',
        'consumer_id': 'member_id',
        'retention_time': 'retention_time_ms',
    }


class OffsetCommitRequest_v3(Request):
    API_KEY = 8
    API_VERSION = 3
    SCHEMA = OffsetCommitRequest_v2.SCHEMA
    DEFAULT_RETENTION_TIME = -1
    ALIASES = OffsetCommitRequest_v2.ALIASES


class OffsetCommitRequest_v4(Request):
    API_KEY = 8
    API_VERSION = 4
    SCHEMA = OffsetCommitRequest_v3.SCHEMA
    DEFAULT_RETENTION_TIME = -1
    ALIASES = OffsetCommitRequest_v3.ALIASES


class OffsetCommitRequest_v5(Request):
    API_KEY = 8
    API_VERSION = 5 # drops retention_time
    SCHEMA = Schema(
        ('group_id', String('utf-8')),
        ('generation_id_or_member_epoch', Int32),
        ('member_id', String('utf-8')),
        ('topics', Array(
            ('name', String('utf-8')),
            ('partitions', Array(
                ('partition_index', Int32),
                ('committed_offset', Int64),
                ('committed_metadata', String('utf-8'))))))
    )
    ALIASES = {
        'consumer_group': 'group_id',
        'consumer_group_generation_id': 'generation_id_or_member_epoch',
        'consumer_id': 'member_id',
    }


class OffsetCommitRequest_v6(Request):
    API_KEY = 8
    API_VERSION = 6
    SCHEMA = Schema(
        ('group_id', String('utf-8')),
        ('generation_id_or_member_epoch', Int32),
        ('member_id', String('utf-8')),
        ('topics', Array(
            ('name', String('utf-8')),
            ('partitions', Array(
                ('partition_index', Int32),
                ('committed_offset', Int64),
                ('committed_leader_epoch', Int32), # added for fencing / kip-320. default -1
                ('committed_metadata', String('utf-8'))))))
    )
    ALIASES = OffsetCommitRequest_v5.ALIASES


class OffsetCommitRequest_v7(Request):
    API_KEY = 8
    API_VERSION = 7
    SCHEMA = Schema(
        ('group_id', String('utf-8')),
        ('generation_id_or_member_epoch', Int32),
        ('member_id', String('utf-8')),
        ('group_instance_id', String('utf-8')), # added for static membership / kip-345
        ('topics', Array(
            ('name', String('utf-8')),
            ('partitions', Array(
                ('partition_index', Int32),
                ('committed_offset', Int64),
                ('committed_leader_epoch', Int32),
                ('committed_metadata', String('utf-8'))))))
    )
    ALIASES = {
        'consumer_group': 'group_id',
        'generation_id': 'generation_id_or_member_epoch',
        'consumer_id': 'member_id',
    }


OffsetCommitRequest = [
    OffsetCommitRequest_v0, OffsetCommitRequest_v1, OffsetCommitRequest_v2,
    OffsetCommitRequest_v3, OffsetCommitRequest_v4, OffsetCommitRequest_v5,
    OffsetCommitRequest_v6, OffsetCommitRequest_v7,
]
OffsetCommitResponse = [
    OffsetCommitResponse_v0, OffsetCommitResponse_v1, OffsetCommitResponse_v2,
    OffsetCommitResponse_v3, OffsetCommitResponse_v4, OffsetCommitResponse_v5,
    OffsetCommitResponse_v6, OffsetCommitResponse_v7,
]


class OffsetFetchResponse_v0(Response):
    API_KEY = 9
    API_VERSION = 0
    SCHEMA = Schema(
        ('topics', Array(
            ('name', String('utf-8')),
            ('partitions', Array(
                ('partition_index', Int32),
                ('committed_offset', Int64),
                ('metadata', String('utf-8')),
                ('error_code', Int16)))))
    )


class OffsetFetchResponse_v1(Response):
    API_KEY = 9
    API_VERSION = 1
    SCHEMA = OffsetFetchResponse_v0.SCHEMA


class OffsetFetchResponse_v2(Response):
    # Added in KIP-88
    API_KEY = 9
    API_VERSION = 2
    SCHEMA = Schema(
        ('topics', Array(
            ('name', String('utf-8')),
            ('partitions', Array(
                ('partition_index', Int32),
                ('committed_offset', Int64),
                ('metadata', String('utf-8')),
                ('error_code', Int16))))),
        ('error_code', Int16)
    )


class OffsetFetchResponse_v3(Response):
    API_KEY = 9
    API_VERSION = 3
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('topics', Array(
            ('name', String('utf-8')),
            ('partitions', Array(
                ('partition_index', Int32),
                ('committed_offset', Int64),
                ('metadata', String('utf-8')),
                ('error_code', Int16))))),
        ('error_code', Int16)
    )


class OffsetFetchResponse_v4(Response):
    API_KEY = 9
    API_VERSION = 4
    SCHEMA = OffsetFetchResponse_v3.SCHEMA


class OffsetFetchResponse_v5(Response):
    API_KEY = 9
    API_VERSION = 5
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('topics', Array(
            ('name', String('utf-8')),
            ('partitions', Array(
                ('partition_index', Int32),
                ('committed_offset', Int64),
                ('committed_leader_epoch', Int32),
                ('metadata', String('utf-8')),
                ('error_code', Int16))))),
        ('error_code', Int16)
    )


class OffsetFetchRequest_v0(Request):
    API_KEY = 9
    API_VERSION = 0  # zookeeper-backed storage
    SCHEMA = Schema(
        ('group_id', String('utf-8')),
        ('topics', Array(
            ('name', String('utf-8')),
            ('partition_indexes', Array(Int32))))
    )
    ALIASES = {
        'consumer_group': 'group_id',
    }


class OffsetFetchRequest_v1(Request):
    API_KEY = 9
    API_VERSION = 1  # kafka-backed storage
    SCHEMA = OffsetFetchRequest_v0.SCHEMA
    ALIASES = OffsetFetchRequest_v0.ALIASES


class OffsetFetchRequest_v2(Request):
    # KIP-88: Allows passing null topics to return offsets for all partitions
    # that the consumer group has a stored offset for, even if no consumer in
    # the group is currently consuming that partition.
    API_KEY = 9
    API_VERSION = 2
    SCHEMA = OffsetFetchRequest_v1.SCHEMA
    ALIASES = OffsetFetchRequest_v1.ALIASES


class OffsetFetchRequest_v3(Request):
    API_KEY = 9
    API_VERSION = 3
    SCHEMA = OffsetFetchRequest_v2.SCHEMA
    ALIASES = OffsetFetchRequest_v2.ALIASES


class OffsetFetchRequest_v4(Request):
    API_KEY = 9
    API_VERSION = 4
    SCHEMA = OffsetFetchRequest_v3.SCHEMA
    ALIASES = OffsetFetchRequest_v3.ALIASES


class OffsetFetchRequest_v5(Request):
    API_KEY = 9
    API_VERSION = 5
    SCHEMA = OffsetFetchRequest_v4.SCHEMA
    ALIASES = OffsetFetchRequest_v4.ALIASES


OffsetFetchRequest = [
    OffsetFetchRequest_v0, OffsetFetchRequest_v1,
    OffsetFetchRequest_v2, OffsetFetchRequest_v3,
    OffsetFetchRequest_v4, OffsetFetchRequest_v5,
]
OffsetFetchResponse = [
    OffsetFetchResponse_v0, OffsetFetchResponse_v1,
    OffsetFetchResponse_v2, OffsetFetchResponse_v3,
    OffsetFetchResponse_v4, OffsetFetchResponse_v5,
]
