from itertools import cycle

from .base import Partitioner

class RoundRobinPartitioner(Partitioner):
    """
    Implements a round robin partitioner which sends data to partitions
    in a round robin fashion
    """
    def __init__(self, partitions):
        super(RoundRobinPartitioner, self).__init__(partitions)
        self.iterpart = cycle(partitions)

    def _set_partitions(self, partitions):
        self.partitions = partitions
        self.iterpart = cycle(partitions)

    def partition(self, key, partitions):
        # Refresh the partition list if necessary
        if self.partitions != partitions:
            self._set_partitions(partitions)

        return next(self.iterpart)
