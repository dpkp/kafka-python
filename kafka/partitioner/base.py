
class Partitioner(object):
    """
    Base class for a partitioner
    """
    def __init__(self, partitions):
        """
        Initialize the partitioner

        Arguments:
            partitions: A list of available partitions (during startup)
        """
        self.partitions = partitions

    def partition(self, key, partitions=None):
        """
        Takes a string key and num_partitions as argument and returns
        a partition to be used for the message

        Arguments:
            key: the key to use for partitioning
            partitions: (optional) a list of partitions.
        """
        raise NotImplementedError('partition function has to be implemented')
