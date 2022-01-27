from __future__ import absolute_import

import random

from kafka.vendor import six


class FNV1a32Partitioner(object):
    """Partitioner with FNV1a 32-bit hash algorithm.

    Hashes key to partition using FNV1a 32-bit hashing.
    If key is None, selects partition randomly from available,
    or from all partitions if none are currently available.
    """
    @classmethod
    def __call__(cls, key, all_partitions, available):
        """
        Partitioner implementation using FNV1a 32-bit hashing function and
        decimal conversion with two's complement. If key is passed with None
        value, the selection of the partition is random.

        The implementation details are selected to make sure the same key
        is mapped to the same partition in Goka/Sarama. It is confirmed
        that this implementation works the same as the partitioner of
        github.com/lovoo/goka v1.0.5 with Go version 1.16.

        Algorithm details:
        http://www.isthe.com/chongo/tech/comp/fnv/#FNV-param

        :param key: partitioning key
        :param all_partitions: list of all partitions sorted by partition ID
        :param available: list of available partitions in no particular order
        :return: one of the values from all_partitions or available
        """
        if key is None:
            if available:
                return random.choice(available)
            return random.choice(all_partitions)

        key_hash = _get_fnv1a_32(key)
        key_hash = _get_twos_complement_32bit(key_hash)
        key_hash = abs(key_hash)
        idx = key_hash % len(all_partitions)
        return all_partitions[idx]
    

def _get_twos_complement_32bit(value: int) -> int:
    """
    Returns the signed two's complement decimal conversion.

    Algorithm details:
    http://sandbox.mc.edu/~bennet/cs110/tc/tctod.html

    Taken from:
    https://stackoverflow.com/questions/1604464/twos-complement-in-python
    """
    bit_base = 32
    if (value & (1 << (bit_base - 1))) != 0:
        value = value - (1 << bit_base)
    return value


def _get_fnv1a_32(key: bytes) -> int:
    """
    Returns the FNV1a 32bit hash of the given key.

    Algorithm details:
    http://www.isthe.com/chongo/tech/comp/fnv/#FNV-param

    Taken from:
    https://github.com/znerol/py-fnvhash/blob/master/fnvhash/__init__.py
    """
    # We set the same init_offset and prime for the FNV hasher as
    # defined in Golang FNV package. The Go FNV is the package Sarama
    # uses for its hashing calculations under the hood.
    # References:
    # https://cs.opensource.google/go/go/+/refs/tags/go1.17.3:src/hash/fnv/fnv.go;l=31
    # https://cs.opensource.google/go/go/+/refs/tags/go1.17.3:src/hash/fnv/fnv.go;l=35
    init_offset = 0x811c9dc5
    prime = 0x01000193
    hash_size = 2 ** 32

    # Python2 bytes is really a str, causing the bitwise operations below to fail
    # so convert to bytearray.
    if six.PY2:
        key = bytearray(bytes(key))

    key_hash = init_offset
    for byte in key:
        key_hash = key_hash ^ byte
        key_hash = (key_hash * prime) % hash_size
    return key_hash
