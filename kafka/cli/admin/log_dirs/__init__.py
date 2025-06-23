from __future__ import absolute_import

import sys

from .describe import DescribeLogDirs


class LogDirsSubCommand:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('log-dirs', help='Manage Kafka Topic/Partition Log Directories')
        commands = parser.add_subparsers()
        for cmd in [DescribeLogDirs]:
            cmd.add_subparser(commands)
        parser.set_defaults(command=lambda *_args: parser.print_help() or sys.exit(2))
