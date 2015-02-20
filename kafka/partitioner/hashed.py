from .base import Partitioner

class HashedPartitioner(Partitioner):
    """
    Implements a partitioner which selects the target partition based on
    the hash of the key
    """
    def partition(self, key, partitions=None):
        if not partitions:
            partitions = self.partitions
        size = len(partitions)
        idx = hash(key) % size

        return partitions[idx]
