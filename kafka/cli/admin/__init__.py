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
from ..common import add_common_cli_args
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


_LOGGING_LEVELS = {'NOTSET': 0, 'DEBUG': 10, 'INFO': 20, 'WARNING': 30, 'ERROR': 40, 'CRITICAL': 50}


def build_kwargs(props):
    kwargs = {}
    for prop in props or []:
        k, v = prop.split('=')
        try:
            v = int(v)
        except ValueError:
            pass
        if v == 'None':
            v = None
        elif v == 'False':
            v = False
        elif v == 'True':
            v = True
        kwargs[k] = v
    return kwargs


def run_cli(args=None):
    parser = main_parser()
    subparsers = parser.add_subparsers(help='subcommands')
    for cmd in [ACLsSubCommand, ClusterSubCommand, ConfigsSubCommand,
                TopicsSubCommand, PartitionsSubCommand,
                GroupsSubCommand, UsersSubCommand]:
        cmd.add_subparser(subparsers)
    config = parser.parse_args(args)

    if config.enable_logger is not None:
        log_level = _LOGGING_LEVELS[config.log_level.upper()]
        handler = logging.StreamHandler()
        handler.setLevel(log_level)
        handler.setFormatter(logging.Formatter(logging.BASIC_FORMAT))
        for name in config.enable_logger:
            logger = logging.getLogger(name)
            logger.setLevel(log_level)
            logger.addHandler(handler)
    else:
        logging.basicConfig(level=_LOGGING_LEVELS[config.log_level.upper()])
    if config.disable_logger is not None:
        for name in config.disable_logger:
            logging.getLogger(name).setLevel(logging.CRITICAL + 1)

    if config.format not in ('raw', 'json'):
        raise ValueError('Unrecognized format: %s' % config.format)
    logger = logging.getLogger(__name__)

    kwargs = build_kwargs(config.extra_config)
    client = KafkaAdminClient(
        bootstrap_servers=config.bootstrap_servers,
        security_protocol=config.security_protocol,
        sasl_mechanism=config.sasl_mechanism,
        sasl_plain_username=config.sasl_user,
        sasl_plain_password=config.sasl_password,
        **kwargs)
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

    # [client-quotas]
    # describe (DescribeClientQuotas - not supported yet)
    # alter (AlterClientQuotas - not supported yet)

    # [producers]
    # describe (DescribeProducers - not supported yet)

    # [transactions]
    # describe (DescribeTransactions - not supported yet)
    # list (ListTransactions - not supported yet)
    # abort (not supported yet)

    # [cluster]
    # describe-features (DescribeFeatures - not supported yet)
    # update-features (UpdateFeatures - not supported yet)
    # version
    # api-versions
    # alter-log-dirs (AlterReplicaLogDirs - not supported yet)
    # DescribeQuorum (not supported yet)
    # UnregisterBroker
    # AddRaftVoter
    # RemoveRaftVoter

    # [tokens] *DelegationTokenRequest
    # create
    # describe
    # renew
    # expire
