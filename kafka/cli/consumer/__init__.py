import argparse
import logging

from kafka import KafkaConsumer
from kafka.errors import KafkaError
from ..common import add_common_cli_args, configure_logging, build_connect_kwargs


def main_parser():
    parser = argparse.ArgumentParser(
        prog='python -m kafka.consumer',
        description='Kafka console consumer',
    )
    add_common_cli_args(parser)
    parser.add_argument(
        '-t', '--topic', type=str, action='append', dest='topics', required=True,
        help='subscribe to topic')
    parser.add_argument(
        '-g', '--group', type=str, required=True,
        help='consumer group')
    parser.add_argument(
        '-i', '--group-instance-id', type=str,
        help='static group membership identifier')
    parser.add_argument(
        '-f', '--format', type=str, default='str',
        help='output format: str|raw|full')
    parser.add_argument(
        '--encoding', type=str, default='utf-8', help='encoding to use for str output decode()')
    return parser


def run_cli(args=None):
    parser = main_parser()
    config = parser.parse_args(args)

    if config.format not in ('str', 'raw', 'full'):
        raise ValueError('Unrecognized format: %s' % config.format)
    configure_logging(config)
    logger = logging.getLogger(__name__)

    kwargs = build_connect_kwargs(config)
    consumer = KafkaConsumer(
        group_id=config.group,
        group_instance_id=config.group_instance_id,
        **kwargs)
    consumer.subscribe(config.topics)
    try:
        for m in consumer:
            if config.format == 'str':
                print(m.value.decode(config.encoding))
            elif config.format == 'full':
                print(m)
            else:
                print(m.value)
    except KeyboardInterrupt:
        logger.info('Bye!')
        return 0
    except Exception:
        logger.critical('Error!', exc_info=True)
        return 1
    finally:
        consumer.close()
