from __future__ import absolute_import


class DescribeCluster:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('describe', help='Describe Kafka Cluster')
        parser.set_defaults(command=lambda cli, _args: cli.describe_cluster())
