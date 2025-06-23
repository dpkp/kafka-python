from __future__ import absolute_import

import sys

from .delete import DeleteConsumerGroups
from .describe import DescribeConsumerGroups
from .list import ListConsumerGroups
from .list_offsets import ListConsumerGroupOffsets


class ConsumerGroupsSubCommand:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('consumer-groups', help='Manage Kafka Consumer Groups')
        commands = parser.add_subparsers()
        for cmd in [ListConsumerGroups, DescribeConsumerGroups, ListConsumerGroupOffsets, DeleteConsumerGroups]:
            cmd.add_subparser(commands)
        parser.set_defaults(command=lambda *_args: parser.print_help() or sys.exit(2))
