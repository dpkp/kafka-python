#!/usr/bin/env python3
"""Per-function and per-line profiling of new protocol encode/decode hot methods.

Uses cProfile for call-level breakdown and a targeted manual timer
for line-level insight into the key hot methods.

Usage:
    python kafka/benchmarks/profile_protocol.py [encode|decode|create] [N]

    encode  - profile encode path (default)
    decode  - profile decode path
    create  - profile object creation path
    N       - number of iterations (default: 1000)
"""

import cProfile
import io
import pstats
import sys
import time

from kafka.protocol.metadata.metadata import MetadataResponse as NewMetadataResp
from kafka.protocol.consumer.fetch import FetchResponse as NewFetchResp

# === Test data (same as benchmark script) ===
_BROKERS = [(i, 'broker-%d.example.com' % i, 9092 + i) for i in range(3)]
_PARTITIONS = [(0, p, p % 3, [0, 1, 2], [0, 1]) for p in range(3)]
_TOPICS = [(0, 'test-topic-%d' % t, _PARTITIONS) for t in range(5)]

_RECORDS_BYTES = b'\x00' * 1024
_FETCH_TOPICS = [
    ('test-topic-%d' % t, [
        (p, 0, 1000 + p, 999 + p, None, _RECORDS_BYTES)
        for p in range(3)
    ])
    for t in range(2)
]


def profile_encode(n):
    """Profile the encode path for MetadataResponse and FetchResponse."""
    obj_meta = NewMetadataResp(version=0, brokers=_BROKERS, topics=_TOPICS)
    obj_fetch = NewFetchResp(version=4, throttle_time_ms=0, responses=_FETCH_TOPICS)

    # Warmup
    obj_meta.encode(version=0)
    obj_fetch.encode(version=4)

    pr = cProfile.Profile()
    pr.enable()
    for _ in range(n):
        obj_meta.encode(version=0)
        obj_fetch.encode(version=4)
    pr.disable()

    print("=" * 80)
    print("ENCODE PROFILE (%d iterations: MetadataResponse v0 + FetchResponse v4)" % n)
    print("=" * 80)
    stats = pstats.Stats(pr)
    stats.sort_stats('cumulative')
    stats.print_stats(40)
    print()
    stats.sort_stats('tottime')
    stats.print_stats(40)


def profile_decode(n):
    """Profile the decode path for MetadataResponse and FetchResponse."""
    obj_meta = NewMetadataResp(version=0, brokers=_BROKERS, topics=_TOPICS)
    obj_fetch = NewFetchResp(version=4, throttle_time_ms=0, responses=_FETCH_TOPICS)
    encoded_meta = obj_meta.encode(version=0)
    encoded_fetch = obj_fetch.encode(version=4)

    # Warmup
    NewMetadataResp.decode(io.BytesIO(encoded_meta), version=0)
    NewFetchResp.decode(io.BytesIO(encoded_fetch), version=4)

    pr = cProfile.Profile()
    pr.enable()
    for _ in range(n):
        NewMetadataResp.decode(io.BytesIO(encoded_meta), version=0)
        NewFetchResp.decode(io.BytesIO(encoded_fetch), version=4)
    pr.disable()

    print("=" * 80)
    print("DECODE PROFILE (%d iterations: MetadataResponse v0 + FetchResponse v4)" % n)
    print("=" * 80)
    stats = pstats.Stats(pr)
    stats.sort_stats('cumulative')
    stats.print_stats(40)
    print()
    stats.sort_stats('tottime')
    stats.print_stats(40)


def profile_create(n):
    """Profile the object creation path."""
    # Warmup
    NewMetadataResp(version=0, brokers=_BROKERS, topics=_TOPICS)
    NewFetchResp(version=4, throttle_time_ms=0, responses=_FETCH_TOPICS)

    pr = cProfile.Profile()
    pr.enable()
    for _ in range(n):
        NewMetadataResp(version=0, brokers=_BROKERS, topics=_TOPICS)
        NewFetchResp(version=4, throttle_time_ms=0, responses=_FETCH_TOPICS)
    pr.disable()

    print("=" * 80)
    print("CREATE PROFILE (%d iterations: MetadataResponse v0 + FetchResponse v4)" % n)
    print("=" * 80)
    stats = pstats.Stats(pr)
    stats.sort_stats('cumulative')
    stats.print_stats(40)
    print()
    stats.sort_stats('tottime')
    stats.print_stats(40)


if __name__ == '__main__':
    mode = sys.argv[1] if len(sys.argv) > 1 else 'encode'
    iterations = int(sys.argv[2]) if len(sys.argv) > 2 else 1000

    if mode == 'encode':
        profile_encode(iterations)
    elif mode == 'decode':
        profile_decode(iterations)
    elif mode == 'create':
        profile_create(iterations)
    elif mode == 'all':
        profile_encode(iterations)
        print("\n\n")
        profile_decode(iterations)
        print("\n\n")
        profile_create(iterations)
    else:
        print("Usage: python profile_protocol.py [encode|decode|create|all] [N]")
        sys.exit(1)
