import sys

from .delete import DeleteGroups
from .describe import DescribeGroups
from .list import ListGroups
from .list_offsets import ListGroupOffsets


class GroupsSubCommand:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('groups', help='Manage Kafka Groups')
        commands = parser.add_subparsers()
        for cmd in [ListGroups, DescribeGroups, ListGroupOffsets, DeleteGroups]:
            cmd.add_subparser(commands)
        parser.set_defaults(command=lambda *_args: parser.print_help() or sys.exit(2))
