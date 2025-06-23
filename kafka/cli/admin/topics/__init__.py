from __future__ import absolute_import

import sys

from .create import CreateTopic
from .delete import DeleteTopic
from .describe import DescribeTopics
from .list import ListTopics


class TopicsSubCommand:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('topics', help='List/Describe/Create/Delete Kafka Topics')
        commands = parser.add_subparsers()
        for cmd in [ListTopics, DescribeTopics, CreateTopic, DeleteTopic]:
            cmd.add_subparser(commands)
        parser.set_defaults(command=lambda *_args: parser.print_help() or sys.exit(2))
