from __future__ import absolute_import

import random

from kafka.partitioner import RoundRobinPartitioner
from kafka.partitioner.roundrobin import CachedPartitionCycler


class RandStartRoundRobinPartitioner(RoundRobinPartitioner):
    """Random start round robin partitioner.
     Selects first partition randomly and starts a round robin cycle
     """
    def __init__(self, partitions=None):
        self.partitions_iterable = CachedRandomPartitionCycler(partitions)
        if partitions:
            self._set_partitions(partitions)
        else:
            self.partitions = None


class CachedRandomPartitionCycler(CachedPartitionCycler):

    def next(self):
        assert self.partitions is not None
        if self.cur_pos is None:
            self.cur_pos = random.choice(self.partitions)
        if not self._index_available(self.cur_pos, self.partitions):
            self.cur_pos = 0
        cur_item = self.partitions[self.cur_pos]
        self.cur_pos += 1
        return cur_item
