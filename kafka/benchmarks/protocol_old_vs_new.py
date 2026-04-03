#!/usr/bin/env python3
"""Benchmark old (Struct-based) vs new (JSON/ApiMessage-based) protocol encode/decode.

Benchmarks focus on realistic client operations:
  - Encode: Request objects (what the client sends)
  - Decode: Response objects (what the client receives)

Usage:
    python kafka/benchmarks/protocol_old_vs_new.py [--fast] [--quiet]

    --fast      Use pyperf fast mode (fewer iterations, less stable)
    --quiet     Suppress per-benchmark warnings

Comparison tables are printed after all benchmarks complete.
"""

import io
import sys

import pyperf

# === Old system imports ===
from kafka.protocol.old.api_versions import (
    ApiVersionsRequest_v0 as OldApiVersionsReq_v0,
    ApiVersionsRequest_v3 as OldApiVersionsReq_v3,
    ApiVersionsResponse_v0 as OldApiVersionsResp_v0,
)
from kafka.protocol.old.fetch import (
    FetchRequest_v4 as OldFetchReq_v4,
    FetchResponse_v4 as OldFetchResp_v4,
)
from kafka.protocol.old.produce import (
    ProduceRequest_v3 as OldProduceReq_v3,
    ProduceResponse_v3 as OldProduceResp_v3,
)

# === New system imports ===
from kafka.protocol.metadata.api_versions import (
    ApiVersionsRequest as NewApiVersionsReq,
    ApiVersionsResponse as NewApiVersionsResp,
)
from kafka.protocol.consumer.fetch import (
    FetchRequest as NewFetchReq,
    FetchResponse as NewFetchResp,
)
from kafka.protocol.producer.produce import (
    ProduceRequest as NewProduceReq,
    ProduceResponse as NewProduceResp,
)


# ---------------------------------------------------------------------------
# Test data
# ---------------------------------------------------------------------------

_RECORDS_BYTES = b'\x00' * 1024

# FetchRequest v4: 5 topics, 3 partitions each
_FETCH_REQ_TOPICS_V4 = [
    ('test-topic-%d' % t, [
        (p, p * 1000, 1048576)  # partition, fetch_offset, partition_max_bytes
        for p in range(3)
    ])
    for t in range(5)
]

# FetchRequest v12 (flexible): 5 topics, 3 partitions each
_FetchPartition = NewFetchReq.FetchTopic.FetchPartition
_FETCH_REQ_TOPICS_V12 = [
    NewFetchReq.FetchTopic(version=12, topic='test-topic-%d' % t, partitions=[
        _FetchPartition(version=12, partition=p, current_leader_epoch=-1,
                        fetch_offset=p * 1000, last_fetched_epoch=-1,
                        log_start_offset=0, partition_max_bytes=1048576)
        for p in range(3)
    ])
    for t in range(5)
]

# ProduceRequest v3: 5 topics, 3 partitions each, 1KB records per partition
_PRODUCE_REQ_TOPICS = [
    ('test-topic-%d' % t, [
        (p, _RECORDS_BYTES)  # index, records
        for p in range(3)
    ])
    for t in range(5)
]

# FetchResponse v4: 5 topics, 3 partitions each, 1KB records per partition
_FETCH_RESP_TOPICS_V4 = [
    ('test-topic-%d' % t, [
        (p, 0, 1000 + p, 999 + p, None, _RECORDS_BYTES)
        for p in range(3)
    ])
    for t in range(5)
]

# FetchResponse v12 (flexible): 5 topics, 3 partitions each, 1KB records per partition
_FetchRespTopic = NewFetchResp.FetchableTopicResponse
_FetchRespPart = _FetchRespTopic.PartitionData
_FETCH_RESP_TOPICS_V12 = [
    _FetchRespTopic(version=12, topic='test-topic-%d' % t, partitions=[
        _FetchRespPart(version=12, partition_index=p, error_code=0,
                       high_watermark=1000 + p, last_stable_offset=999 + p,
                       log_start_offset=0, aborted_transactions=[],
                       records=_RECORDS_BYTES, preferred_read_replica=-1)
        for p in range(3)
    ])
    for t in range(5)
]

# ProduceResponse v3: 5 topics, 3 partitions each
_PRODUCE_RESP_TOPICS_V3 = [
    ('test-topic-%d' % t, [
        (p, 0, p * 1000, -1)  # index, error_code, base_offset, log_append_time_ms
        for p in range(3)
    ])
    for t in range(5)
]

# ProduceResponse v9 (flexible): 5 topics, 3 partitions each
_ProduceRespTopic = NewProduceResp.TopicProduceResponse
_ProduceRespPart = _ProduceRespTopic.PartitionProduceResponse
_PRODUCE_RESP_TOPICS_V9 = [
    _ProduceRespTopic(version=9, name='test-topic-%d' % t, partition_responses=[
        _ProduceRespPart(version=9, index=p, error_code=0, base_offset=p * 1000,
                         log_append_time_ms=-1, log_start_offset=p * 500,
                         record_errors=[], error_message=None)
        for p in range(3)
    ])
    for t in range(5)
]

# ApiVersions: 50 api keys
_API_KEYS_DATA = [(i, 0, i + 3) for i in range(50)]


# ---------------------------------------------------------------------------
# Encode objects (Requests)
# ---------------------------------------------------------------------------

OLD_APIVER_REQ_V0 = OldApiVersionsReq_v0()
NEW_APIVER_REQ_V0 = NewApiVersionsReq(version=0)

OLD_APIVER_REQ_V3 = OldApiVersionsReq_v3(
    client_software_name='kafka-python',
    client_software_version='3.0.0',
)
NEW_APIVER_REQ_V3 = NewApiVersionsReq(
    version=3,
    client_software_name='kafka-python',
    client_software_version='3.0.0',
)

OLD_FETCH_REQ_V4 = OldFetchReq_v4(
    replica_id=-1,
    max_wait_ms=500,
    min_bytes=1,
    max_bytes=10485760,
    isolation_level=0,
    topics=_FETCH_REQ_TOPICS_V4,
)
NEW_FETCH_REQ_V4 = NewFetchReq(
    version=4,
    replica_id=-1,
    max_wait_ms=500,
    min_bytes=1,
    max_bytes=10485760,
    isolation_level=0,
    topics=_FETCH_REQ_TOPICS_V4,
)
NEW_FETCH_REQ_V12 = NewFetchReq(
    version=12,
    max_wait_ms=500,
    min_bytes=1,
    max_bytes=10485760,
    isolation_level=0,
    session_id=0,
    session_epoch=-1,
    topics=_FETCH_REQ_TOPICS_V12,
    forgotten_topics_data=[],
    rack_id='',
)

OLD_PRODUCE_REQ_V3 = OldProduceReq_v3(
    transactional_id=None,
    acks=-1,
    timeout_ms=30000,
    topic_data=_PRODUCE_REQ_TOPICS,
)
NEW_PRODUCE_REQ_V3 = NewProduceReq(
    version=3,
    transactional_id=None,
    acks=-1,
    timeout_ms=30000,
    topic_data=_PRODUCE_REQ_TOPICS,
)
NEW_PRODUCE_REQ_V9 = NewProduceReq(
    version=9,
    transactional_id=None,
    acks=-1,
    timeout_ms=30000,
    topic_data=_PRODUCE_REQ_TOPICS,
)


# ---------------------------------------------------------------------------
# Decode objects (Responses) — pre-encode bytes from old system
# ---------------------------------------------------------------------------

OLD_APIVER_RESP_V0 = OldApiVersionsResp_v0(
    error_code=0,
    api_keys=_API_KEYS_DATA,
)
NEW_APIVER_RESP_V0 = NewApiVersionsResp(
    version=0,
    error_code=0,
    api_keys=_API_KEYS_DATA,
)

OLD_FETCH_RESP_V4 = OldFetchResp_v4(
    throttle_time_ms=0,
    responses=_FETCH_RESP_TOPICS_V4,
)
NEW_FETCH_RESP_V4 = NewFetchResp(
    version=4,
    throttle_time_ms=0,
    responses=_FETCH_RESP_TOPICS_V4,
)
NEW_FETCH_RESP_V12 = NewFetchResp(
    version=12,
    throttle_time_ms=0,
    error_code=0,
    session_id=0,
    responses=_FETCH_RESP_TOPICS_V12,
)

OLD_PRODUCE_RESP_V3 = OldProduceResp_v3(
    responses=_PRODUCE_RESP_TOPICS_V3,
    throttle_time_ms=0,
)
NEW_PRODUCE_RESP_V3 = NewProduceResp(
    version=3,
    responses=_PRODUCE_RESP_TOPICS_V3,
    throttle_time_ms=0,
)
NEW_PRODUCE_RESP_V9 = NewProduceResp(
    version=9,
    responses=_PRODUCE_RESP_TOPICS_V9,
    throttle_time_ms=0,
)

ENCODED_APIVER_RESP_V0 = OLD_APIVER_RESP_V0.encode()
ENCODED_FETCH_RESP_V4 = OLD_FETCH_RESP_V4.encode()
ENCODED_PRODUCE_RESP_V3 = OLD_PRODUCE_RESP_V3.encode()
ENCODED_FETCH_RESP_V12 = NEW_FETCH_RESP_V12.encode(version=12)
ENCODED_PRODUCE_RESP_V9 = NEW_PRODUCE_RESP_V9.encode(version=9)


# ---------------------------------------------------------------------------
# Correctness assertions
# ---------------------------------------------------------------------------

assert OLD_APIVER_REQ_V0.encode() == NEW_APIVER_REQ_V0.encode(version=0), \
    "ApiVersionsRequest v0 encode mismatch!"
assert OLD_APIVER_REQ_V3.encode() == NEW_APIVER_REQ_V3.encode(version=3), \
    "ApiVersionsRequest v3 encode mismatch!"
assert OLD_FETCH_REQ_V4.encode() == NEW_FETCH_REQ_V4.encode(version=4), \
    "FetchRequest v4 encode mismatch!"
assert OLD_PRODUCE_REQ_V3.encode() == NEW_PRODUCE_REQ_V3.encode(version=3), \
    "ProduceRequest v3 encode mismatch!"
assert OLD_APIVER_RESP_V0.encode() == NEW_APIVER_RESP_V0.encode(version=0), \
    "ApiVersionsResponse v0 encode mismatch!"
assert OLD_FETCH_RESP_V4.encode() == NEW_FETCH_RESP_V4.encode(version=4), \
    "FetchResponse v4 encode mismatch!"
assert OLD_PRODUCE_RESP_V3.encode() == NEW_PRODUCE_RESP_V3.encode(version=3), \
    "ProduceResponse v3 encode mismatch!"


# ---------------------------------------------------------------------------
# bench_time_func helpers
# ---------------------------------------------------------------------------

def _make_bench_func(func, *args, **kwargs):
    def bench(loops):
        t0 = pyperf.perf_counter()
        for _ in range(loops):
            func(*args, **kwargs)
        return pyperf.perf_counter() - t0
    return bench


def _make_decode_func(cls, encoded, **kwargs):
    def bench(loops):
        t0 = pyperf.perf_counter()
        for _ in range(loops):
            cls.decode(io.BytesIO(encoded), **kwargs)
        return pyperf.perf_counter() - t0
    return bench


# ---------------------------------------------------------------------------
# Benchmark definitions
# ---------------------------------------------------------------------------

BENCHMARKS = {
    'encode (requests)': [
        ('ApiVersionsReq v0 (simple)',
         'encode_ApiVersionsReq_v0_old', _make_bench_func(OLD_APIVER_REQ_V0.encode),
         'encode_ApiVersionsReq_v0_new', _make_bench_func(NEW_APIVER_REQ_V0.encode, version=0)),
        ('ApiVersionsReq v3 (flexible)',
         'encode_ApiVersionsReq_v3_old', _make_bench_func(OLD_APIVER_REQ_V3.encode),
         'encode_ApiVersionsReq_v3_new', _make_bench_func(NEW_APIVER_REQ_V3.encode, version=3)),
        ('FetchReq v4',
         'encode_FetchReq_v4_old', _make_bench_func(OLD_FETCH_REQ_V4.encode),
         'encode_FetchReq_v4_new', _make_bench_func(NEW_FETCH_REQ_V4.encode, version=4)),
        ('FetchReq v12 (flexible)',
         None, None,
         'encode_FetchReq_v12_new', _make_bench_func(NEW_FETCH_REQ_V12.encode, version=12)),
        ('ProduceReq v3',
         'encode_ProduceReq_v3_old', _make_bench_func(OLD_PRODUCE_REQ_V3.encode),
         'encode_ProduceReq_v3_new', _make_bench_func(NEW_PRODUCE_REQ_V3.encode, version=3)),
        ('ProduceReq v9 (flexible)',
         None, None,
         'encode_ProduceReq_v9_new', _make_bench_func(NEW_PRODUCE_REQ_V9.encode, version=9)),
    ],
    'decode (responses)': [
        ('ApiVersionsResp v0 (simple)',
         'decode_ApiVersionsResp_v0_old', _make_decode_func(OldApiVersionsResp_v0, ENCODED_APIVER_RESP_V0),
         'decode_ApiVersionsResp_v0_new', _make_decode_func(NewApiVersionsResp, ENCODED_APIVER_RESP_V0, version=0)),
        ('ProduceResp v3',
         'decode_ProduceResp_v3_old', _make_decode_func(OldProduceResp_v3, ENCODED_PRODUCE_RESP_V3),
         'decode_ProduceResp_v3_new', _make_decode_func(NewProduceResp, ENCODED_PRODUCE_RESP_V3, version=3)),
        ('ProduceResp v9 (flexible)',
         None, None,
         'decode_ProduceResp_v9_new', _make_decode_func(NewProduceResp, ENCODED_PRODUCE_RESP_V9, version=9)),
        ('FetchResp v4',
         'decode_FetchResp_v4_old', _make_decode_func(OldFetchResp_v4, ENCODED_FETCH_RESP_V4),
         'decode_FetchResp_v4_new', _make_decode_func(NewFetchResp, ENCODED_FETCH_RESP_V4, version=4)),
        ('FetchResp v12 (flexible)',
         None, None,
         'decode_FetchResp_v12_new', _make_decode_func(NewFetchResp, ENCODED_FETCH_RESP_V12, version=12)),
    ],
}


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _format_time(seconds):
    """Format seconds into a human-readable string with appropriate unit."""
    us = seconds * 1e6
    if us >= 1000:
        return '%.0f us' % us
    elif us >= 100:
        return '%.0f us' % us
    elif us >= 10:
        return '%.1f us' % us
    else:
        return '%.2f us' % us


def _print_table(title, rows):
    """Print a markdown-style table."""
    print()
    print('### %s' % title)
    print()
    col1 = max(len(r[0]) for r in rows)
    col1 = max(col1, len('Message'))
    header = '| %-*s | %10s | %10s | %5s |' % (col1, 'Message', 'Old', 'New', 'Ratio')
    sep = '|%s|%s|%s|%s|' % ('-' * (col1 + 2), '-' * 12, '-' * 12, '-' * 7)
    print(header)
    print(sep)
    for desc, old_mean, new_mean in rows:
        if old_mean is not None:
            ratio = new_mean / old_mean if old_mean > 0 else float('inf')
            print('| %-*s | %10s | %10s | %5.1fx |' % (
                col1, desc,
                _format_time(old_mean),
                _format_time(new_mean),
                ratio))
        else:
            print('| %-*s | %10s | %10s | %5s |' % (
                col1, desc, 'n/a', _format_time(new_mean), 'n/a'))


def _print_summary(results):
    """Print comparison tables from collected benchmark results."""
    print()
    print('=' * 70)
    print('Protocol Benchmark: Old (Struct) vs New (ApiMessage)')
    print('=' * 70)

    for category, bench_defs in BENCHMARKS.items():
        rows = []
        for entry in bench_defs:
            desc, old_name, _, new_name, _ = entry
            if new_name in results:
                old_val = results.get(old_name) if old_name else None
                rows.append((desc, old_val, results[new_name]))
        if rows:
            _print_table(category.capitalize(), rows)

    print()
    print('### Summary')
    print()
    for category in BENCHMARKS:
        ratios = []
        for entry in BENCHMARKS[category]:
            desc, old_name, _, new_name, _ = entry
            if old_name and old_name in results and new_name in results:
                old_val = results[old_name]
                new_val = results[new_name]
                ratios.append(new_val / old_val if old_val > 0 else float('inf'))
        if ratios:
            min_r = min(ratios)
            max_r = max(ratios)
            print('- **%s**: %.1fx - %.1fx (new / old)' % (
                category.capitalize(), min_r, max_r))
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    runner = pyperf.Runner()
    results = {}

    def _record(name, bench):
        if bench is not None:
            try:
                results[name] = bench.mean()
            except Exception:
                pass

    for category, bench_defs in BENCHMARKS.items():
        for entry in bench_defs:
            desc, old_name, old_func, new_name, new_func = entry
            if old_name is not None:
                _record(old_name, runner.bench_time_func(old_name, old_func))
            _record(new_name, runner.bench_time_func(new_name, new_func))

    # Only print summary in the manager process (workers have --worker in argv)
    if results and '--worker' not in sys.argv:
        _print_summary(results)
