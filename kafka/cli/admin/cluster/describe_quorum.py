class DescribeQuorum:
    COMMAND = 'describe-quorum'
    HELP = 'Describe the KRaft metadata quorum'

    @classmethod
    def add_arguments(cls, parser):
        pass

    @classmethod
    def command(cls, client, args):
        return client.describe_metadata_quorum()
