#!/usr/bin/env python3
"""Benchmarks for the producer encode hot path.

Measures the cost of finalizing a record batch and encoding it into a
ProduceRequest — the pipeline that runs on every send to the broker.

To compare two implementations (e.g. before/after a change) run this
script twice and diff the output:

    # baseline
    git stash
    python -m kafka.benchmarks.producer_encode_path -o baseline.json
    git stash pop

    # new
    python -m kafka.benchmarks.producer_encode_path -o new.json

    pyperf compare_to baseline.json new.json

For an allocation count (tracemalloc) run with --allocations, which is
a separate mode (not a timing benchmark).
"""
import argparse
import sys
import tracemalloc

import pyperf

from kafka.producer.producer_batch import ProducerBatch
from kafka.record.memory_records import MemoryRecordsBuilder
from kafka.structs import TopicPartition
from kafka.protocol.producer import ProduceRequest


DEFAULT_RECORDS_PER_BATCH = 100
DEFAULT_VALUE_SIZE = 128
DEFAULT_BATCH_SIZE = 1 << 20  # 1 MiB — big enough to hold the largest case


def _build_unclosed_batch(num_records, value_size):
    tp = TopicPartition('bench', 0)
    records = MemoryRecordsBuilder(
        magic=2, compression_type=0, batch_size=DEFAULT_BATCH_SIZE)
    batch = ProducerBatch(tp, records)
    value = b'x' * value_size
    for i in range(num_records):
        future = batch.try_append(i, None, value, [])
        assert future is not None, 'batch too small for %d records' % num_records
    return batch


def _build_closed_batch(num_records, value_size):
    batch = _build_unclosed_batch(num_records, value_size)
    batch.records.close()
    return batch


def bench_build_and_close(loops, num_records, value_size):
    """Time build + close — isolates the batch-finalization path.

    close() is fast enough that pyperf would need many thousands of loops
    to measure it directly, so we include the build cost and rely on the
    per-run diff to expose the close() delta.
    """
    value = b'x' * value_size
    tp = TopicPartition('bench', 0)

    t0 = pyperf.perf_counter()
    for _ in range(loops):
        records = MemoryRecordsBuilder(
            magic=2, compression_type=0, batch_size=DEFAULT_BATCH_SIZE)
        batch = ProducerBatch(tp, records)
        for i in range(num_records):
            batch.try_append(i, None, value, [])
        batch.records.close()
    return pyperf.perf_counter() - t0


def bench_encode(loops, num_records, value_size):
    """Time ProduceRequest.encode() with a pre-closed batch.

    Isolates the protocol encoding cost from batch finalization.
    """
    batch = _build_closed_batch(num_records, value_size)
    buf = batch.records.buffer()
    topic_data = [('bench', [(0, buf)])]

    t0 = pyperf.perf_counter()
    for _ in range(loops):
        req = ProduceRequest[8](
            transactional_id=None,
            acks=1,
            timeout_ms=30000,
            topic_data=topic_data,
        )
        req.with_header(correlation_id=1, client_id='bench')
        req.encode(framed=True, header=True)
    return pyperf.perf_counter() - t0


def bench_full_pipeline(loops, num_records, value_size):
    """Time the full producer hot path: append -> close -> encode.

    Mirrors what the Sender thread does for each drained batch.
    """
    value = b'x' * value_size
    tp = TopicPartition('bench', 0)

    t0 = pyperf.perf_counter()
    for _ in range(loops):
        records = MemoryRecordsBuilder(
            magic=2, compression_type=0, batch_size=DEFAULT_BATCH_SIZE)
        batch = ProducerBatch(tp, records)
        for i in range(num_records):
            batch.try_append(i, None, value, [])
        batch.records.close()
        req = ProduceRequest[8](
            transactional_id=None,
            acks=1,
            timeout_ms=30000,
            topic_data=[('bench', [(0, batch.records.buffer())])],
        )
        req.with_header(correlation_id=1, client_id='bench')
        req.encode(framed=True, header=True)
    return pyperf.perf_counter() - t0


def report_allocations(num_records, value_size):
    """Use tracemalloc to count bytes allocated during a single encode."""
    # Pre-build so we only measure the encode path.
    batch = _build_closed_batch(num_records, value_size)
    buf = batch.records.buffer()
    topic_data = [('bench', [(0, buf)])]
    req = ProduceRequest[8](
        transactional_id=None,
        acks=1,
        timeout_ms=30000,
        topic_data=topic_data,
    )
    req.with_header(correlation_id=1, client_id='bench')

    # Warmup — populate the compiled-encoder cache so we don't count its
    # one-time allocations.
    req.encode(framed=True, header=True)

    tracemalloc.start()
    snap_before = tracemalloc.take_snapshot()
    data = req.encode(framed=True, header=True)
    snap_after = tracemalloc.take_snapshot()
    tracemalloc.stop()

    stats = snap_after.compare_to(snap_before, 'lineno')
    total_bytes = sum(max(0, s.size_diff) for s in stats)
    total_allocs = sum(max(0, s.count_diff) for s in stats)

    print('Config        : %d records x %d bytes' % (num_records, value_size))
    print('Wire bytes    : %d' % len(data))
    print('Allocated     : %d bytes across %d allocations' % (total_bytes, total_allocs))
    print()
    print('Top allocation sites:')
    for s in sorted(stats, key=lambda s: s.size_diff, reverse=True)[:10]:
        if s.size_diff <= 0:
            continue
        frame = s.traceback[0]
        loc = '%s:%d' % (frame.filename.split('/workspace/')[-1], frame.lineno)
        print('  %8d bytes / %3d allocs  %s' % (s.size_diff, s.count_diff, loc))


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--records', type=int, default=DEFAULT_RECORDS_PER_BATCH,
                   help='records per batch (default: %(default)d)')
    p.add_argument('--value-size', type=int, default=DEFAULT_VALUE_SIZE,
                   help='record value size in bytes (default: %(default)d)')
    p.add_argument('--allocations', action='store_true',
                   help='report tracemalloc allocations for one encode (not a timing run)')

    # Let pyperf parse its own arguments (--output, --rigorous, etc).
    args, pyperf_args = p.parse_known_args()

    if args.allocations:
        report_allocations(args.records, args.value_size)
        return

    # Rebuild sys.argv so pyperf sees only its own arguments.
    sys.argv = [sys.argv[0]] + pyperf_args

    runner = pyperf.Runner()
    runner.metadata['records_per_batch'] = args.records
    runner.metadata['value_size_bytes'] = args.value_size
    runner.bench_time_func(
        'build_and_close', bench_build_and_close, args.records, args.value_size)
    runner.bench_time_func(
        'encode', bench_encode, args.records, args.value_size)
    runner.bench_time_func(
        'full_pipeline', bench_full_pipeline, args.records, args.value_size)


if __name__ == '__main__':
    main()
