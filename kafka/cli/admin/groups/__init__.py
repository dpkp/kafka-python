import sys

from .alter_offsets import AlterGroupOffsets
from .delete import DeleteGroups
from .delete_offsets import DeleteGroupOffsets
from .describe import DescribeGroups
from .list import ListGroups
from .list_offsets import ListGroupOffsets
from .remove_members import RemoveGroupMembers
from .reset_offsets import ResetGroupOffsets


class GroupsSubCommand:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('groups', help='Manage Kafka Groups')
        commands = parser.add_subparsers()
        for cmd in [ListGroups, DescribeGroups, DeleteGroups,
                    ListGroupOffsets, AlterGroupOffsets, ResetGroupOffsets, DeleteGroupOffsets,
                    RemoveGroupMembers]:
            cmd.add_subparser(commands)
        parser.set_defaults(command=lambda *_args: parser.print_help() or sys.exit(2))
