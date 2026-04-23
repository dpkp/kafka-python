from .create import CreateACLs
from .delete import DeleteACLs
from .describe import DescribeACLs


class ACLsCommandGroup:
    GROUP = 'acls'
    HELP = 'Manage Kafka ACLs'
    COMMANDS = [DescribeACLs, CreateACLs, DeleteACLs]
