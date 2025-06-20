from __future__ import absolute_import

import argparse
import json
import logging
from pprint import pprint

from kafka.admin.client import KafkaAdminClient
from .describe_cluster import DescribeCluster
from .describe_log_dirs import DescribeLogDirs
from .create_topic import CreateTopic
from .delete_topic import DeleteTopic
from .describe_topics import DescribeTopics
from .list_topics import ListTopics
from .describe_configs import DescribeConfigs

    # describe_acls
    # create_acls
    # delete_acls

    # alter_configs
    # IncrementalAlterConfigs (not supported yet)

    # create_partitions
    # AlterPartitionReassignments (not supported yet)
    # ListPartitionReassignments (not supported yet)

    # delete_records
    # OffsetDelete (not supported yet)

    # describe_consumer_groups
    # list_consumer_groups
    # list_consumer_group_offsets
    # delete_consumer_groups
    # delete_consumer_group_offsets (not supported yet)
    # remove_members_from_consumer_group (not supported yet)
    # alter_consumer_group_offsets (not supported yet)

    # list_offsets (not supported yet)

    # perform_leader_election

    # describe_log_dirs (currently broken)
    # AlterReplicaLogDirs (not supported yet)

    # DescribeClientQuotas (not supported yet)
    # AlterClientQuotas (not supported yet)

    # DescribeQuorum (not supported yet)

    # DescribeProducers (not supported yet)
    # DescribeTransactions (not supported yet)
    # ListTransactions (not supported yet)
    # abort_transactin (not supported yet)

    # DescribeTopicPartitions (not supported yet)
    # DescribeFeatures (not supported yet)
    # UpdateFeatures (not supported yet)

    # api_versions


def main_parser():
    parser = argparse.ArgumentParser(
        prog='kafka-admin-client',
        description='Kafka admin client', 
    )
    parser.add_argument(
        '-b', '--bootstrap-servers', type=str, action='append', required=True,
        help='host:port for cluster bootstrap servers')
    parser.add_argument(
        '-l', '--log-level', type=str,
        help='logging level, passed to logging.basicConfig')
    parser.add_argument(
        '-f', '--format', type=str, default='raw',
        help='output format: raw|json')
    return parser


_LOGGING_LEVELS = {'NOTSET': 0, 'DEBUG': 10, 'INFO': 20, 'WARNING': 30, 'ERROR': 40, 'CRITICAL': 50}


def run_cli(args=None):
    parser = main_parser()
    subparsers = parser.add_subparsers(help='subcommands')
    for cmd in [DescribeCluster, DescribeConfigs, DescribeLogDirs, ListTopics, DescribeTopics, CreateTopic, DeleteTopic]:
        cmd.add_subparser(subparsers)

    config = parser.parse_args(args)
    if config.log_level:
        logging.basicConfig(level=_LOGGING_LEVELS[config.log_level])
    if config.format not in ('raw', 'json'):
        raise ValueError('Unrecognized format: %s' % config.format)

    client = KafkaAdminClient(bootstrap_servers=config.bootstrap_servers)
    try:
        result = config.command(client, config)
        if config.format == 'raw':
            pprint(result)
        elif config.format == 'json':
            print(json.dumps(result))
        return 0
    except Exception:
        logging.exception('Error!')
        return 1

if __name__ == '__main__':
    import sys
    sys.exit(run_cli())
