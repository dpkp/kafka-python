from .alter import AlterConfigs
from .describe import DescribeConfigs
from .list import ListConfigResources
from .reset import ResetConfigs


class ConfigsCommandGroup:
    GROUP = 'configs'
    HELP = 'Manage Kafka Configuration'
    COMMANDS = [DescribeConfigs, AlterConfigs, ListConfigResources, ResetConfigs]
