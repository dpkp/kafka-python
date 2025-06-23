from __future__ import absolute_import


class DescribeLogDirs:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('describe', help='Get topic log directories for brokers')
        parser.set_defaults(command=lambda cli, _args: cli.describe_log_dirs())
