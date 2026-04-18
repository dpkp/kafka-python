import sys

from .alter_reassignments import AlterPartitionReassignments
from .create import CreatePartitions
from .delete_records import DeleteRecords
from .describe import DescribeTopicPartitions
from .elect_leaders import ElectLeaders
from .list_reassignments import ListPartitionReassignments


class PartitionsSubCommand:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('partitions', help='Manage Kafka Partitions')
        commands = parser.add_subparsers()
        for cmd in [
            CreatePartitions,
            DeleteRecords,
            ElectLeaders,
            AlterPartitionReassignments,
            ListPartitionReassignments,
            DescribeTopicPartitions,
        ]:
            cmd.add_subparser(commands)
        parser.set_defaults(command=lambda *_args: parser.print_help() or sys.exit(2))
