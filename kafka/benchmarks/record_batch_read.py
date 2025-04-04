#!/usr/bin/env python
from __future__ import print_function
import hashlib
import itertools
import os
import random

import pyperf

from kafka.record.memory_records import MemoryRecords, MemoryRecordsBuilder


DEFAULT_BATCH_SIZE = 1600 * 1024
KEY_SIZE = 6
VALUE_SIZE = 60
TIMESTAMP_RANGE = [1505824130000, 1505824140000]

BATCH_SAMPLES = 5
MESSAGES_PER_BATCH = 100


def random_bytes(length):
    buffer = bytearray(length)
    for i in range(length):
        buffer[i] = random.randint(0, 255)
    return bytes(buffer)


def prepare(magic):
    samples = []
    for _ in range(BATCH_SAMPLES):
        batch = MemoryRecordsBuilder(
            magic, batch_size=DEFAULT_BATCH_SIZE, compression_type=0)
        for _ in range(MESSAGES_PER_BATCH):
            size = batch.append(
                random.randint(*TIMESTAMP_RANGE),
                random_bytes(KEY_SIZE),
                random_bytes(VALUE_SIZE),
                headers=[])
            assert size
        batch.close()
        samples.append(bytes(batch.buffer()))

    return iter(itertools.cycle(samples))


def finalize(results):
    # Just some strange code to make sure PyPy does execute the code above
    # properly
    hash_val = hashlib.md5()
    for buf in results:
        hash_val.update(buf)
    print(hash_val, file=open(os.devnull, "w"))


def func(loops, magic):
    # Jit can optimize out the whole function if the result is the same each
    # time, so we need some randomized input data )
    precomputed_samples = prepare(magic)
    results = []

    # Main benchmark code.
    batch_data = next(precomputed_samples)
    t0 = pyperf.perf_counter()
    for _ in range(loops):
        records = MemoryRecords(batch_data)
        while records.has_next():
            batch = records.next_batch()
            batch.validate_crc()
            for record in batch:
                results.append(record.value)

    res = pyperf.perf_counter() - t0
    finalize(results)

    return res


if __name__ == '__main__':
    runner = pyperf.Runner()
    runner.bench_time_func('batch_read_v0', func, 0)
    runner.bench_time_func('batch_read_v1', func, 1)
    runner.bench_time_func('batch_read_v2', func, 2)
