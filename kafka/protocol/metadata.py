from __future__ import absolute_import

from kafka.protocol.api import Request, Response
from kafka.protocol.types import Array, Boolean, Int16, Int32, Schema, String, BitField


class MetadataResponse_v0(Response):
    API_KEY = 3
    API_VERSION = 0
    SCHEMA = Schema(
        ('brokers', Array(
            ('node_id', Int32),
            ('host', String('utf-8')),
            ('port', Int32))),
        ('topics', Array(
            ('error_code', Int16),
            ('topic', String('utf-8')),
            ('partitions', Array(
                ('error_code', Int16),
                ('partition', Int32),
                ('leader', Int32),
                ('replicas', Array(Int32)),
                ('isr', Array(Int32))))))
    )


class MetadataResponse_v1(Response):
    API_KEY = 3
    API_VERSION = 1
    SCHEMA = Schema(
        ('brokers', Array(
            ('node_id', Int32),
            ('host', String('utf-8')),
            ('port', Int32),
            ('rack', String('utf-8')))),
        ('controller_id', Int32),
        ('topics', Array(
            ('error_code', Int16),
            ('topic', String('utf-8')),
            ('is_internal', Boolean),
            ('partitions', Array(
                ('error_code', Int16),
                ('partition', Int32),
                ('leader', Int32),
                ('replicas', Array(Int32)),
                ('isr', Array(Int32))))))
    )


class MetadataResponse_v2(Response):
    API_KEY = 3
    API_VERSION = 2
    SCHEMA = Schema(
        ('brokers', Array(
            ('node_id', Int32),
            ('host', String('utf-8')),
            ('port', Int32),
            ('rack', String('utf-8')))),
        ('cluster_id', String('utf-8')),  # <-- Added cluster_id field in v2
        ('controller_id', Int32),
        ('topics', Array(
            ('error_code', Int16),
            ('topic', String('utf-8')),
            ('is_internal', Boolean),
            ('partitions', Array(
                ('error_code', Int16),
                ('partition', Int32),
                ('leader', Int32),
                ('replicas', Array(Int32)),
                ('isr', Array(Int32))))))
    )


class MetadataResponse_v3(Response):
    API_KEY = 3
    API_VERSION = 3
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('brokers', Array(
            ('node_id', Int32),
            ('host', String('utf-8')),
            ('port', Int32),
            ('rack', String('utf-8')))),
        ('cluster_id', String('utf-8')),
        ('controller_id', Int32),
        ('topics', Array(
            ('error_code', Int16),
            ('topic', String('utf-8')),
            ('is_internal', Boolean),
            ('partitions', Array(
                ('error_code', Int16),
                ('partition', Int32),
                ('leader', Int32),
                ('replicas', Array(Int32)),
                ('isr', Array(Int32))))))
    )


class MetadataResponse_v4(Response):
    API_KEY = 3
    API_VERSION = 4
    SCHEMA = MetadataResponse_v3.SCHEMA


class MetadataResponse_v5(Response):
    API_KEY = 3
    API_VERSION = 5
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('brokers', Array(
            ('node_id', Int32),
            ('host', String('utf-8')),
            ('port', Int32),
            ('rack', String('utf-8')))),
        ('cluster_id', String('utf-8')),
        ('controller_id', Int32),
        ('topics', Array(
            ('error_code', Int16),
            ('topic', String('utf-8')),
            ('is_internal', Boolean),
            ('partitions', Array(
                ('error_code', Int16),
                ('partition', Int32),
                ('leader', Int32),
                ('replicas', Array(Int32)),
                ('isr', Array(Int32)),
                ('offline_replicas', Array(Int32))))))
    )


class MetadataResponse_v6(Response):
    """Metadata Request/Response v6 is the same as v5,
    but on quota violation, brokers send out responses before throttling."""
    API_KEY = 3
    API_VERSION = 6
    SCHEMA = MetadataResponse_v5.SCHEMA


class MetadataResponse_v7(Response):
    """v7 adds per-partition leader_epoch field"""
    API_KEY = 3
    API_VERSION = 7
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('brokers', Array(
            ('node_id', Int32),
            ('host', String('utf-8')),
            ('port', Int32),
            ('rack', String('utf-8')))),
        ('cluster_id', String('utf-8')),
        ('controller_id', Int32),
        ('topics', Array(
            ('error_code', Int16),
            ('topic', String('utf-8')),
            ('is_internal', Boolean),
            ('partitions', Array(
                ('error_code', Int16),
                ('partition', Int32),
                ('leader', Int32),
                ('leader_epoch', Int32),
                ('replicas', Array(Int32)),
                ('isr', Array(Int32)),
                ('offline_replicas', Array(Int32))))))
    )


class MetadataResponse_v8(Response):
    """v8 adds authorized_operations fields"""
    API_KEY = 3
    API_VERSION = 8
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('brokers', Array(
            ('node_id', Int32),
            ('host', String('utf-8')),
            ('port', Int32),
            ('rack', String('utf-8')))),
        ('cluster_id', String('utf-8')),
        ('controller_id', Int32),
        ('topics', Array(
            ('error_code', Int16),
            ('topic', String('utf-8')),
            ('is_internal', Boolean),
            ('partitions', Array(
                ('error_code', Int16),
                ('partition', Int32),
                ('leader', Int32),
                ('leader_epoch', Int32),
                ('replicas', Array(Int32)),
                ('isr', Array(Int32)),
                ('offline_replicas', Array(Int32)))),
            ('authorized_operations', BitField))),
        ('authorized_operations', BitField)
    )


class MetadataRequest_v0(Request):
    API_KEY = 3
    API_VERSION = 0
    RESPONSE_TYPE = MetadataResponse_v0
    SCHEMA = Schema(
        ('topics', Array(String('utf-8')))
    )
    ALL_TOPICS = [] # Empty Array (len 0) for topics returns all topics
    NO_TOPICS = [] # v0 does not support a 'no topics' request, so we'll just ask for ALL


class MetadataRequest_v1(Request):
    API_KEY = 3
    API_VERSION = 1
    RESPONSE_TYPE = MetadataResponse_v1
    SCHEMA = MetadataRequest_v0.SCHEMA
    ALL_TOPICS = None # Null Array (len -1) for topics returns all topics
    NO_TOPICS = [] # Empty array (len 0) for topics returns no topics


class MetadataRequest_v2(Request):
    API_KEY = 3
    API_VERSION = 2
    RESPONSE_TYPE = MetadataResponse_v2
    SCHEMA = MetadataRequest_v1.SCHEMA
    ALL_TOPICS = None
    NO_TOPICS = []


class MetadataRequest_v3(Request):
    API_KEY = 3
    API_VERSION = 3
    RESPONSE_TYPE = MetadataResponse_v3
    SCHEMA = MetadataRequest_v1.SCHEMA
    ALL_TOPICS = None
    NO_TOPICS = []


class MetadataRequest_v4(Request):
    API_KEY = 3
    API_VERSION = 4
    RESPONSE_TYPE = MetadataResponse_v4
    SCHEMA = Schema(
        ('topics', Array(String('utf-8'))),
        ('allow_auto_topic_creation', Boolean)
    )
    ALL_TOPICS = None
    NO_TOPICS = []


class MetadataRequest_v5(Request):
    """
    The v5 metadata request is the same as v4.
    An additional field for offline_replicas has been added to the v5 metadata response
    """
    API_KEY = 3
    API_VERSION = 5
    RESPONSE_TYPE = MetadataResponse_v5
    SCHEMA = MetadataRequest_v4.SCHEMA
    ALL_TOPICS = None
    NO_TOPICS = []


class MetadataRequest_v6(Request):
    API_KEY = 3
    API_VERSION = 6
    RESPONSE_TYPE = MetadataResponse_v6
    SCHEMA = MetadataRequest_v5.SCHEMA
    ALL_TOPICS = None
    NO_TOPICS = []


class MetadataRequest_v7(Request):
    API_KEY = 3
    API_VERSION = 7
    RESPONSE_TYPE = MetadataResponse_v7
    SCHEMA = MetadataRequest_v6.SCHEMA
    ALL_TOPICS = None
    NO_TOPICS = []


class MetadataRequest_v8(Request):
    API_KEY = 3
    API_VERSION = 8
    RESPONSE_TYPE = MetadataResponse_v8
    SCHEMA = Schema(
        ('topics', Array(String('utf-8'))),
        ('allow_auto_topic_creation', Boolean),
        ('include_cluster_authorized_operations', Boolean),
        ('include_topic_authorized_operations', Boolean)
    )
    ALL_TOPICS = None
    NO_TOPICS = []


MetadataRequest = [
    MetadataRequest_v0, MetadataRequest_v1, MetadataRequest_v2,
    MetadataRequest_v3, MetadataRequest_v4, MetadataRequest_v5,
    MetadataRequest_v6, MetadataRequest_v7, MetadataRequest_v8,
]
MetadataResponse = [
    MetadataResponse_v0, MetadataResponse_v1, MetadataResponse_v2,
    MetadataResponse_v3, MetadataResponse_v4, MetadataResponse_v5,
    MetadataResponse_v6, MetadataResponse_v7, MetadataResponse_v8,
]
