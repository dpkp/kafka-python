from __future__ import absolute_import

import random

from .hashed import murmur2


class DefaultPartitioner(object):
    """Default partitioner.

    Hashes key to partition using murmur2 hashing (from java client)
    If key is None, selects partition randomly from available,
    or from all partitions if none are currently available
    """
    @classmethod
    def __call__(cls, key, all_partitions, available):
        """
        Get the partition corresponding to key
        :param key: partitioning key
        :param all_partitions: list of all partitions sorted by partition ID
        :param available: list of available partitions in no particular order
        :return: one of the values from all_partitions or available
        """
        if key is None:
            if available:
                return random.choice(available)
            return random.choice(all_partitions)

        idx = murmur2(key)
        idx &= 0x7fffffff
        idx %= len(all_partitions)
        return all_partitions[idx]
