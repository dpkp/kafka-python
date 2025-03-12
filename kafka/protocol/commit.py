from __future__ import absolute_import

from kafka.protocol.api import Request, Response
from kafka.protocol.types import Array, Int16, Int32, Int64, Schema, String


class OffsetCommitResponse_v0(Response):
    API_KEY = 8
    API_VERSION = 0
    SCHEMA = Schema(
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
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
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
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


class OffsetCommitRequest_v0(Request):
    API_KEY = 8
    API_VERSION = 0  # Zookeeper-backed storage
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


class OffsetCommitRequest_v1(Request):
    API_KEY = 8
    API_VERSION = 1  # Kafka-backed storage
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


class OffsetCommitRequest_v2(Request):
    API_KEY = 8
    API_VERSION = 2
    RESPONSE_TYPE = OffsetCommitResponse_v2
    SCHEMA = Schema(
        ('consumer_group', String('utf-8')),
        ('consumer_group_generation_id', Int32),
        ('consumer_id', String('utf-8')),
        ('retention_time', Int64), # added retention_time, dropped timestamp
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('offset', Int64),
                ('metadata', String('utf-8'))))))
    )
    DEFAULT_RETENTION_TIME = -1


class OffsetCommitRequest_v3(Request):
    API_KEY = 8
    API_VERSION = 3
    RESPONSE_TYPE = OffsetCommitResponse_v3
    SCHEMA = OffsetCommitRequest_v2.SCHEMA
    DEFAULT_RETENTION_TIME = -1


class OffsetCommitRequest_v4(Request):
    API_KEY = 8
    API_VERSION = 4
    RESPONSE_TYPE = OffsetCommitResponse_v4
    SCHEMA = OffsetCommitRequest_v3.SCHEMA
    DEFAULT_RETENTION_TIME = -1


class OffsetCommitRequest_v5(Request):
    API_KEY = 8
    API_VERSION = 5 # drops retention_time
    RESPONSE_TYPE = OffsetCommitResponse_v5
    SCHEMA = Schema(
        ('consumer_group', String('utf-8')),
        ('consumer_group_generation_id', Int32),
        ('consumer_id', String('utf-8')),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('offset', Int64),
                ('metadata', String('utf-8'))))))
    )


class OffsetCommitRequest_v6(Request):
    API_KEY = 8
    API_VERSION = 6
    RESPONSE_TYPE = OffsetCommitResponse_v6
    SCHEMA = Schema(
        ('consumer_group', String('utf-8')),
        ('consumer_group_generation_id', Int32),
        ('consumer_id', String('utf-8')),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('offset', Int64),
                ('leader_epoch', Int32), # added for fencing / kip-320. default -1
                ('metadata', String('utf-8'))))))
    )


OffsetCommitRequest = [
    OffsetCommitRequest_v0, OffsetCommitRequest_v1,
    OffsetCommitRequest_v2, OffsetCommitRequest_v3,
    OffsetCommitRequest_v4, OffsetCommitRequest_v5,
    OffsetCommitRequest_v6,
]
OffsetCommitResponse = [
    OffsetCommitResponse_v0, OffsetCommitResponse_v1,
    OffsetCommitResponse_v2, OffsetCommitResponse_v3,
    OffsetCommitResponse_v4, OffsetCommitResponse_v5,
    OffsetCommitResponse_v6,
]


class OffsetFetchResponse_v0(Response):
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
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('offset', Int64),
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
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('offset', Int64),
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
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('offset', Int64),
                ('leader_epoch', Int32),
                ('metadata', String('utf-8')),
                ('error_code', Int16))))),
        ('error_code', Int16)
    )


class OffsetFetchRequest_v0(Request):
    API_KEY = 9
    API_VERSION = 0  # zookeeper-backed storage
    RESPONSE_TYPE = OffsetFetchResponse_v0
    SCHEMA = Schema(
        ('consumer_group', String('utf-8')),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(Int32))))
    )


class OffsetFetchRequest_v1(Request):
    API_KEY = 9
    API_VERSION = 1  # kafka-backed storage
    RESPONSE_TYPE = OffsetFetchResponse_v1
    SCHEMA = OffsetFetchRequest_v0.SCHEMA


class OffsetFetchRequest_v2(Request):
    # KIP-88: Allows passing null topics to return offsets for all partitions
    # that the consumer group has a stored offset for, even if no consumer in
    # the group is currently consuming that partition.
    API_KEY = 9
    API_VERSION = 2
    RESPONSE_TYPE = OffsetFetchResponse_v2
    SCHEMA = OffsetFetchRequest_v1.SCHEMA


class OffsetFetchRequest_v3(Request):
    API_KEY = 9
    API_VERSION = 3
    RESPONSE_TYPE = OffsetFetchResponse_v3
    SCHEMA = OffsetFetchRequest_v2.SCHEMA


class OffsetFetchRequest_v4(Request):
    API_KEY = 9
    API_VERSION = 4
    RESPONSE_TYPE = OffsetFetchResponse_v4
    SCHEMA = OffsetFetchRequest_v3.SCHEMA


class OffsetFetchRequest_v5(Request):
    API_KEY = 9
    API_VERSION = 5
    RESPONSE_TYPE = OffsetFetchResponse_v5
    SCHEMA = OffsetFetchRequest_v4.SCHEMA


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
