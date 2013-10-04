from itertools import cycle


class Partitioner(object):
    """
    Base class for a partitioner
    """
    def __init__(self, partitions):
        """
        Initialize the partitioner

        partitions - A list of available partitions (during startup)
        """
        self.partitions = partitions

    def partition(self, key, partitions):
        """
        Takes a string key and num_partitions as argument and returns
        a partition to be used for the message

        partitions - The list of partitions is passed in every call. This
                     may look like an overhead, but it will be useful
                     (in future) when we handle cases like rebalancing
        """
        raise NotImplementedError('partition function has to be implemented')


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

        return self.iterpart.next()


class HashedPartitioner(Partitioner):
    """
    Implements a partitioner which selects the target partition based on
    the hash of the key
    """
    def partition(self, key, partitions):
        size = len(partitions)
        idx = hash(key) % size
        return partitions[idx]
