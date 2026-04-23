from .alter_offsets import AlterGroupOffsets
from .delete import DeleteGroups
from .delete_offsets import DeleteGroupOffsets
from .describe import DescribeGroups
from .list import ListGroups
from .list_offsets import ListGroupOffsets
from .remove_members import RemoveGroupMembers
from .reset_offsets import ResetGroupOffsets


class GroupsCommandGroup:
    GROUP = 'groups'
    HELP = 'Manage Kafka Groups'
    COMMANDS = [ListGroups, DescribeGroups, DeleteGroups,
                ListGroupOffsets, AlterGroupOffsets, ResetGroupOffsets, DeleteGroupOffsets,
                RemoveGroupMembers]
