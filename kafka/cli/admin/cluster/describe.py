class DescribeCluster:
    COMMAND = 'describe'
    HELP = 'Describe Kafka Cluster'

    @classmethod
    def add_arguments(cls, parser):
        pass

    @classmethod
    def command(cls, client, args):
        return client.describe_cluster()
