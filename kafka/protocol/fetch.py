import collections

from kafka.protocol.api import Request, Response
from kafka.protocol.types import Array, Int8, Int16, Int32, Int64, Schema, String, Bytes


AbortedTransaction = collections.namedtuple("AbortedTransaction",
    ["producer_id", "first_offset"])


class FetchResponse_v0(Response):
    API_KEY = 1
    API_VERSION = 0
    SCHEMA = Schema(
        ('responses', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition_index', Int32),
                ('error_code', Int16),
                ('high_watermark', Int64),
                ('records', Bytes)))))
    )
    ALIASES = {
        'topics': 'responses',
    }


class FetchResponse_v1(Response):
    API_KEY = 1
    API_VERSION = 1
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('responses', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition_index', Int32),
                ('error_code', Int16),
                ('high_watermark', Int64),
                ('records', Bytes)))))
    )
    ALIASES = FetchResponse_v0.ALIASES


class FetchResponse_v2(Response):
    API_KEY = 1
    API_VERSION = 2
    SCHEMA = FetchResponse_v1.SCHEMA  # message format changed internally
    ALIASES = FetchResponse_v1.ALIASES


class FetchResponse_v3(Response):
    API_KEY = 1
    API_VERSION = 3
    SCHEMA = FetchResponse_v2.SCHEMA
    ALIASES = FetchResponse_v2.ALIASES


class FetchResponse_v4(Response):
    # Adds message format v2
    API_KEY = 1
    API_VERSION = 4
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('responses', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition_index', Int32),
                ('error_code', Int16),
                ('high_watermark', Int64),
                ('last_stable_offset', Int64),
                ('aborted_transactions', Array(
                    ('producer_id', Int64),
                    ('first_offset', Int64))),
                ('records', Bytes)))))
    )
    ALIASES = FetchResponse_v3.ALIASES


class FetchResponse_v5(Response):
    API_KEY = 1
    API_VERSION = 5
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('responses', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition_index', Int32),
                ('error_code', Int16),
                ('high_watermark', Int64),
                ('last_stable_offset', Int64),
                ('log_start_offset', Int64),
                ('aborted_transactions', Array(
                    ('producer_id', Int64),
                    ('first_offset', Int64))),
                ('records', Bytes)))))
    )
    ALIASES = FetchResponse_v4.ALIASES


class FetchResponse_v6(Response):
    """
    Same as FetchResponse_v5. The version number is bumped up to indicate that the client supports KafkaStorageException.
    The KafkaStorageException will be translated to NotLeaderForPartitionException in the response if version <= 5
    """
    API_KEY = 1
    API_VERSION = 6
    SCHEMA = FetchResponse_v5.SCHEMA
    ALIASES = FetchResponse_v5.ALIASES


class FetchResponse_v7(Response):
    """
    Add error_code and session_id to response
    """
    API_KEY = 1
    API_VERSION = 7
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('error_code', Int16),
        ('session_id', Int32),
        ('responses', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition_index', Int32),
                ('error_code', Int16),
                ('high_watermark', Int64),
                ('last_stable_offset', Int64),
                ('log_start_offset', Int64),
                ('aborted_transactions', Array(
                    ('producer_id', Int64),
                    ('first_offset', Int64))),
                ('records', Bytes)))))
    )
    ALIASES = FetchResponse_v6.ALIASES


class FetchResponse_v8(Response):
    API_KEY = 1
    API_VERSION = 8
    SCHEMA = FetchResponse_v7.SCHEMA
    ALIASES = FetchResponse_v7.ALIASES


class FetchResponse_v9(Response):
    API_KEY = 1
    API_VERSION = 9
    SCHEMA = FetchResponse_v8.SCHEMA
    ALIASES = FetchResponse_v8.ALIASES


class FetchResponse_v10(Response):
    API_KEY = 1
    API_VERSION = 10
    SCHEMA = FetchResponse_v9.SCHEMA
    ALIASES = FetchResponse_v9.ALIASES


class FetchResponse_v11(Response):
    API_KEY = 1
    API_VERSION = 11
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('error_code', Int16),
        ('session_id', Int32),
        ('responses', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition_index', Int32),
                ('error_code', Int16),
                ('high_watermark', Int64),
                ('last_stable_offset', Int64),
                ('log_start_offset', Int64),
                ('aborted_transactions', Array(
                    ('producer_id', Int64),
                    ('first_offset', Int64))),
                ('preferred_read_replica', Int32),
                ('records', Bytes)))))
    )
    ALIASES = FetchResponse_v10.ALIASES


class FetchRequest_v0(Request):
    API_KEY = 1
    API_VERSION = 0
    SCHEMA = Schema(
        ('replica_id', Int32),
        ('max_wait_ms', Int32),
        ('min_bytes', Int32),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('fetch_offset', Int64),
                ('partition_max_bytes', Int32)))))
    )
    ALIASES = {
        'max_wait_time': 'max_wait_ms',
    }


class FetchRequest_v1(Request):
    API_KEY = 1
    API_VERSION = 1
    SCHEMA = FetchRequest_v0.SCHEMA
    ALIASES = FetchRequest_v0.ALIASES


class FetchRequest_v2(Request):
    API_KEY = 1
    API_VERSION = 2
    SCHEMA = FetchRequest_v1.SCHEMA
    ALIASES = FetchRequest_v1.ALIASES


class FetchRequest_v3(Request):
    API_KEY = 1
    API_VERSION = 3
    SCHEMA = Schema(
        ('replica_id', Int32),
        ('max_wait_ms', Int32),
        ('min_bytes', Int32),
        ('max_bytes', Int32),  # This new field is only difference from FR_v2
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('fetch_offset', Int64),
                ('partition_max_bytes', Int32)))))
    )
    ALIASES = FetchRequest_v2.ALIASES


class FetchRequest_v4(Request):
    # Adds isolation_level field
    # Adds message format v2
    API_KEY = 1
    API_VERSION = 4
    SCHEMA = Schema(
        ('replica_id', Int32),
        ('max_wait_ms', Int32),
        ('min_bytes', Int32),
        ('max_bytes', Int32),
        ('isolation_level', Int8),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('fetch_offset', Int64),
                ('partition_max_bytes', Int32)))))
    )
    ALIASES = FetchRequest_v3.ALIASES


class FetchRequest_v5(Request):
    # This may only be used in broker-broker api calls
    API_KEY = 1
    API_VERSION = 5
    SCHEMA = Schema(
        ('replica_id', Int32),
        ('max_wait_ms', Int32),
        ('min_bytes', Int32),
        ('max_bytes', Int32),
        ('isolation_level', Int8),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('fetch_offset', Int64),
                ('log_start_offset', Int64),
                ('partition_max_bytes', Int32)))))
    )
    ALIASES = FetchRequest_v4.ALIASES


class FetchRequest_v6(Request):
    """
    The body of FETCH_REQUEST_V6 is the same as FETCH_REQUEST_V5.
    The version number is bumped up to indicate that the client supports KafkaStorageException.
    The KafkaStorageException will be translated to NotLeaderForPartitionException in the response if version <= 5
    """
    API_KEY = 1
    API_VERSION = 6
    SCHEMA = FetchRequest_v5.SCHEMA
    ALIASES = FetchRequest_v5.ALIASES


class FetchRequest_v7(Request):
    """
    Add incremental fetch requests (see KIP-227)
    """
    API_KEY = 1
    API_VERSION = 7
    SCHEMA = Schema(
        ('replica_id', Int32),
        ('max_wait_ms', Int32),
        ('min_bytes', Int32),
        ('max_bytes', Int32),
        ('isolation_level', Int8),
        ('session_id', Int32),
        ('session_epoch', Int32),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('fetch_offset', Int64),
                ('log_start_offset', Int64),
                ('partition_max_bytes', Int32))))),
        ('forgotten_topics_data', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(Int32))
        )),
    )
    ALIASES = FetchRequest_v6.ALIASES


class FetchRequest_v8(Request):
    """
    bump used to indicate that on quota violation brokers send out responses before throttling.
    """
    API_KEY = 1
    API_VERSION = 8
    SCHEMA = FetchRequest_v7.SCHEMA
    ALIASES = FetchRequest_v7.ALIASES


class FetchRequest_v9(Request):
    """
    adds the current leader epoch (see KIP-320)
    """
    API_KEY = 1
    API_VERSION = 9
    SCHEMA = Schema(
        ('replica_id', Int32),
        ('max_wait_ms', Int32),
        ('min_bytes', Int32),
        ('max_bytes', Int32),
        ('isolation_level', Int8),
        ('session_id', Int32),
        ('session_epoch', Int32),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('current_leader_epoch', Int32),
                ('fetch_offset', Int64),
                ('log_start_offset', Int64),
                ('partition_max_bytes', Int32))))),
        ('forgotten_topics_data', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(Int32)),
        )),
    )
    ALIASES = FetchRequest_v8.ALIASES


class FetchRequest_v10(Request):
    """
    bumped up to indicate ZStandard capability. (see KIP-110)
    """
    API_KEY = 1
    API_VERSION = 10
    SCHEMA = FetchRequest_v9.SCHEMA
    ALIASES = FetchRequest_v9.ALIASES


class FetchRequest_v11(Request):
    """
    added rack ID to support read from followers (KIP-392)
    """
    API_KEY = 1
    API_VERSION = 11
    SCHEMA = Schema(
        ('replica_id', Int32),
        ('max_wait_ms', Int32),
        ('min_bytes', Int32),
        ('max_bytes', Int32),
        ('isolation_level', Int8),
        ('session_id', Int32),
        ('session_epoch', Int32),
        ('topics', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('partition', Int32),
                ('current_leader_epoch', Int32),
                ('fetch_offset', Int64),
                ('log_start_offset', Int64),
                ('partition_max_bytes', Int32))))),
        ('forgotten_topics_data', Array(
            ('topic', String('utf-8')),
            ('partitions', Array(Int32))
        )),
        ('rack_id', String('utf-8')),
    )
    ALIASES = FetchRequest_v10.ALIASES


FetchRequest = [
    FetchRequest_v0, FetchRequest_v1, FetchRequest_v2,
    FetchRequest_v3, FetchRequest_v4, FetchRequest_v5,
    FetchRequest_v6, FetchRequest_v7, FetchRequest_v8,
    FetchRequest_v9, FetchRequest_v10, FetchRequest_v11,
]
FetchResponse = [
    FetchResponse_v0, FetchResponse_v1, FetchResponse_v2,
    FetchResponse_v3, FetchResponse_v4, FetchResponse_v5,
    FetchResponse_v6, FetchResponse_v7, FetchResponse_v8,
    FetchResponse_v9, FetchResponse_v10, FetchResponse_v11,
]
