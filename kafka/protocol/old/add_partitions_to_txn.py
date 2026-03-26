from .api import Request, Response
from .types import Array, Int16, Int32, Int64, Schema, String


class AddPartitionsToTxnResponse_v0(Response):
    API_KEY = 24
    API_VERSION = 0
    SCHEMA = Schema(
        ('throttle_time_ms', Int32),
        ('results_by_topic_v3_and_below', Array(
            ('name', String('utf-8')),
            ('results_by_partition', Array(
                ('partition_index', Int32),
                ('partition_error_code', Int16))))))
    ALIASES = {
        'results': 'results_by_topic_v3_and_below',
    }


class AddPartitionsToTxnResponse_v1(Response):
    API_KEY = 24
    API_VERSION = 1
    SCHEMA = AddPartitionsToTxnResponse_v0.SCHEMA
    ALIASES = AddPartitionsToTxnResponse_v0.ALIASES


class AddPartitionsToTxnResponse_v2(Response):
    API_KEY = 24
    API_VERSION = 2
    SCHEMA = AddPartitionsToTxnResponse_v1.SCHEMA
    ALIASES = AddPartitionsToTxnResponse_v1.ALIASES


class AddPartitionsToTxnRequest_v0(Request):
    API_KEY = 24
    API_VERSION = 0
    SCHEMA = Schema(
        ('v3_and_below_transactional_id', String('utf-8')),
        ('v3_and_below_producer_id', Int64),
        ('v3_and_below_producer_epoch', Int16),
        ('v3_and_below_topics', Array(
            ('name', String('utf-8')),
            ('partitions', Array(Int32)))))
    ALIASES = {
        'transactional_id': 'v3_and_below_transactional_id',
        'producer_id': 'v3_and_below_producer_id',
        'producer_epoch': 'v3_and_below_producer_epoch',
        'topics': 'v3_and_below_topics',
    }


class AddPartitionsToTxnRequest_v1(Request):
    API_KEY = 24
    API_VERSION = 1
    SCHEMA = AddPartitionsToTxnRequest_v0.SCHEMA
    ALIASES = AddPartitionsToTxnRequest_v0.ALIASES


class AddPartitionsToTxnRequest_v2(Request):
    API_KEY = 24
    API_VERSION = 2
    SCHEMA = AddPartitionsToTxnRequest_v1.SCHEMA
    ALIASES = AddPartitionsToTxnRequest_v1.ALIASES


AddPartitionsToTxnRequest = [
    AddPartitionsToTxnRequest_v0, AddPartitionsToTxnRequest_v1, AddPartitionsToTxnRequest_v2,
]
AddPartitionsToTxnResponse = [
    AddPartitionsToTxnResponse_v0, AddPartitionsToTxnResponse_v1, AddPartitionsToTxnResponse_v2,
]
