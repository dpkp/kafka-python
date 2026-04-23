import argparse
import json
import logging
from pprint import pprint

from kafka.admin.client import KafkaAdminClient
from .acls import ACLsCommandGroup
from .cluster import ClusterCommandGroup
from .configs import ConfigsCommandGroup
from .groups import GroupsCommandGroup
from .partitions import PartitionsCommandGroup
from .topics import TopicsCommandGroup
from .users import UsersCommandGroup
from ..common import add_common_cli_args, configure_logging, build_connect_kwargs
from kafka.errors import BrokerResponseError


def build_parser(groups=()):
    parser = argparse.ArgumentParser(
        prog='python -m kafka.admin',
        description='Kafka Admin Client',
    )
    add_common_cli_args(parser, bootstrap_required=False)
    parser.add_argument_group('output').add_argument(
        '--format', type=str, default='raw',
        help='output format: raw|json')
    groups_sub = parser.add_subparsers(dest='group', metavar='GROUP', title='Available command groups')
    for group in groups:
        group_parser = groups_sub.add_parser(group.GROUP, help=group.HELP)
        group_parser.set_defaults(group=group_parser) # refcycle..
        commands_sub = group_parser.add_subparsers(dest='command', metavar='COMMAND', title='Available commands')
        for cmd in group.COMMANDS:
            command_parser = commands_sub.add_parser(cmd.COMMAND, help=cmd.HELP)
            options = command_parser.add_argument_group('command options')
            cmd.add_arguments(options)
            command_parser.set_defaults(command=cmd.command)
    return parser


def run_cli(args=None):
    parser = build_parser([
        ACLsCommandGroup, ClusterCommandGroup, ConfigsCommandGroup,
        TopicsCommandGroup, PartitionsCommandGroup, GroupsCommandGroup,
        UsersCommandGroup,
    ])
    config = parser.parse_args(args)
    if not config.group:
        parser.print_help()
        return 1
    elif not config.command:
        config.group.print_help()
        return 1
    if config.format not in ('raw', 'json'):
        print(f'Unrecognized format: {config.format}')
        return 1

    configure_logging(config)
    logger = logging.getLogger(__name__)

    try:
        kwargs = build_connect_kwargs(config)
    except ValueError as exc:
        parser.print_usage()
        print(f'{parser.prog}: {exc}')
        return 1
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
