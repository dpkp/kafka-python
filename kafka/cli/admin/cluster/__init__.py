from .describe import DescribeCluster
from .features import DescribeFeatures, UpdateFeatures
from .log_dirs import DescribeLogDirs, AlterLogDirs
from .versions import GetApiVersions, GetBrokerVersion


class ClusterCommandGroup:
    GROUP = 'cluster'
    HELP = 'Manage Kafka Cluster'
    COMMANDS = [DescribeCluster,
                GetApiVersions, GetBrokerVersion,
                DescribeFeatures, UpdateFeatures,
                DescribeLogDirs, AlterLogDirs]
