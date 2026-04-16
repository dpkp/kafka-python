import sys

from .create import CreateACLs
from .delete import DeleteACLs
from .describe import DescribeACLs


class ACLsSubCommand:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('acls', help='Manage Kafka ACLs')
        commands = parser.add_subparsers()
        for cmd in [DescribeACLs, CreateACLs, DeleteACLs]:
            cmd.add_subparser(commands)
        parser.set_defaults(command=lambda *_args: parser.print_help() or sys.exit(2))
