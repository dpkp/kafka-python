#!/usr/bin/env python3
from __future__ import print_function
import hashlib
import itertools
import os
import random

import pyperf

from kafka.record.memory_records import MemoryRecordsBuilder


DEFAULT_BATCH_SIZE = 1600 * 1024
KEY_SIZE = 6
VALUE_SIZE = 60
TIMESTAMP_RANGE = [1505824130000, 1505824140000]

# With values above v1 record is 100 bytes, so 10 000 bytes for 100 messages
MESSAGES_PER_BATCH = 100


def random_bytes(length):
    buffer = bytearray(length)
    for i in range(length):
        buffer[i] = random.randint(0, 255)
    return bytes(buffer)


def prepare():
    return iter(itertools.cycle([
        (random_bytes(KEY_SIZE),
         random_bytes(VALUE_SIZE),
         random.randint(*TIMESTAMP_RANGE)
         )
        for _ in range(int(MESSAGES_PER_BATCH * 1.94))
    ]))


def finalize(results):
    # Just some strange code to make sure PyPy does execute the main code
    # properly, without optimizing it away
    hash_val = hashlib.md5()
    for buf in results:
        hash_val.update(buf)
    print(hash_val, file=open(os.devnull, "w"))


def func(loops, magic):
    # Jit can optimize out the whole function if the result is the same each
    # time, so we need some randomized input data )
    precomputed_samples = prepare()
    results = []

    # Main benchmark code.
    t0 = pyperf.perf_counter()
    for _ in range(loops):
        batch = MemoryRecordsBuilder(
            magic, batch_size=DEFAULT_BATCH_SIZE, compression_type=0)
        for _ in range(MESSAGES_PER_BATCH):
            key, value, timestamp = next(precomputed_samples)
            size = batch.append(
                timestamp=timestamp, key=key, value=value)
            assert size
        batch.close()
        results.append(batch.buffer())

    res = pyperf.perf_counter() - t0

    finalize(results)

    return res


if __name__ == '__main__':
    runner = pyperf.Runner()
    runner.bench_time_func('batch_append_v0', func, 0)
    runner.bench_time_func('batch_append_v1', func, 1)
    runner.bench_time_func('batch_append_v2', func, 2)
