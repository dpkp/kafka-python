
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
