import sys

from .alter import AlterConfigs
from .describe import DescribeConfigs
from .list import ListConfigResources
from .reset import ResetConfigs


class ConfigsSubCommand:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('configs', help='Manage Kafka Configuration')
        commands = parser.add_subparsers()
        for cmd in [DescribeConfigs, AlterConfigs, ListConfigResources, ResetConfigs]:
            cmd.add_subparser(commands)
        parser.set_defaults(command=lambda *_args: parser.print_help() or sys.exit(2))
