from .describe import DescribeCluster
from .describe_quorum import DescribeQuorum
from .features import DescribeFeatures, UpdateFeatures
from .log_dirs import DescribeLogDirs, AlterLogDirs
from .versions import GetApiVersions, GetBrokerVersion


class ClusterCommandGroup:
    GROUP = 'cluster'
    HELP = 'Manage Kafka Cluster'
    COMMANDS = [DescribeCluster, DescribeQuorum,
                GetApiVersions, GetBrokerVersion,
                DescribeFeatures, UpdateFeatures,
                DescribeLogDirs, AlterLogDirs]
