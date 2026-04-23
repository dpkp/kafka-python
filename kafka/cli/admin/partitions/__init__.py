from .alter_reassignments import AlterPartitionReassignments
from .create import CreatePartitions
from .delete_records import DeleteRecords
from .describe import DescribeTopicPartitions
from .elect_leaders import ElectLeaders
from .list_offsets import ListPartitionOffsets
from .list_reassignments import ListPartitionReassignments


class PartitionsCommandGroup:
    GROUP = 'partitions'
    HELP = 'Manage Kafka Partitions'
    COMMANDS = [
        CreatePartitions,
        DescribeTopicPartitions,
        ListPartitionOffsets,
        ListPartitionReassignments,
        AlterPartitionReassignments,
        DeleteRecords,
        ElectLeaders,
    ]
