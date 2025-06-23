from __future__ import absolute_import

import sys

from .describe import DescribeConfigs


class ConfigsSubCommand:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('configs', help='Manage Kafka Configuration')
        commands = parser.add_subparsers()
        for cmd in [DescribeConfigs]:
            cmd.add_subparser(commands)
        parser.set_defaults(command=lambda *_args: parser.print_help() or sys.exit(2))
