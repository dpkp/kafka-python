class DescribeFeatures:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('describe-features', help='Describe Features of Kafka Cluster')
        parser.set_defaults(command=cls.command)

    @classmethod
    def command(cls, client, args):
        return client.describe_features()
