import argparse
import json
import logging
from pprint import pprint

from kafka.admin.client import KafkaAdminClient
from .acls import ACLsSubCommand
from .cluster import ClusterSubCommand
from .configs import ConfigsSubCommand
from .groups import GroupsSubCommand
from .partitions import PartitionsSubCommand
from .topics import TopicsSubCommand
from .users import UsersSubCommand
from ..common import add_common_cli_args, configure_logging, build_connect_kwargs
from kafka.errors import BrokerResponseError


def main_parser():
    parser = argparse.ArgumentParser(
        prog='python -m kafka.admin',
        description='Kafka admin client',
    )
    add_common_cli_args(parser)
    parser.add_argument(
        '-f', '--format', type=str, default='raw',
        help='output format: raw|json')
    return parser


def run_cli(args=None):
    parser = main_parser()
    subparsers = parser.add_subparsers(help='subcommands')
    for cmd in [ACLsSubCommand, ClusterSubCommand, ConfigsSubCommand,
                TopicsSubCommand, PartitionsSubCommand,
                GroupsSubCommand, UsersSubCommand]:
        cmd.add_subparser(subparsers)
    config = parser.parse_args(args)
    if config.format not in ('raw', 'json'):
        raise ValueError('Unrecognized format: %s' % config.format)

    configure_logging(config)
    logger = logging.getLogger(__name__)

    kwargs = build_connect_kwargs(config)
    client = KafkaAdminClient(**kwargs)

    try:
        result = config.command(client, config)
        if config.format == 'raw':
            pprint(result)
        elif config.format == 'json':
            if hasattr(result, 'to_dict'):
                result = result.to_dict()
            print(json.dumps(result))
        return 0
    except BrokerResponseError as exc:
        print(exc)
        return 1
    except ValueError as exc:
        print(exc.args[0])
        return 1
    except AttributeError as exc:
        logger.exception(exc)
        parser.print_help()
        return 1
    except Exception:
        logger.critical('Error!', exc_info=True)
        return 1


# Commands TODO:
    # --dry-run support
    # --trace ?

    # [producers]
    # describe (DescribeProducers)

    # [transactions]
    # describe (DescribeTransactions)
    # list (ListTransactions)
    # abort (EndTxn)

    # [cluster]
    # alter-log-dirs (AlterReplicaLogDirs)
    # describe-features (ApiVersions)
    # update-features (UpdateFeatures)
    # describe-quorum (DescribeQuorum)
    # unregister-broker (UnregisterBroker)
    # add-raft-voter (AddRaftVoter)
    # remove-raft-voter (RemoveRaftVoter)
    # describe-quotas (DescribeClientQuotas)
    # alter-quotas (AlterClientQuotas)

    # [tokens] *DelegationTokenRequest
    # create
    # describe
    # renew
    # expire
