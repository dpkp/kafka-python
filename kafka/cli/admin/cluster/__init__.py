import sys

from .describe import DescribeCluster
from .log_dirs import DescribeLogDirs


class ClusterSubCommand:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('cluster', help='Manage Kafka Cluster')
        commands = parser.add_subparsers()
        for cmd in [DescribeCluster, DescribeLogDirs]:
            cmd.add_subparser(commands)
        parser.set_defaults(command=lambda *_args: parser.print_help() or sys.exit(2))
