from .api_versions import GetApiVersions
from .broker_version import GetBrokerVersion
from .describe import DescribeCluster
from .log_dirs import DescribeLogDirs
from .features import DescribeFeatures, UpdateFeatures


class ClusterCommandGroup:
    GROUP = 'cluster'
    HELP = 'Manage Kafka Cluster'
    COMMANDS = [DescribeCluster, DescribeFeatures, UpdateFeatures,
                GetApiVersions, GetBrokerVersion, DescribeLogDirs]
