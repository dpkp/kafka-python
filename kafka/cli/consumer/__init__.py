from __future__ import absolute_import, print_function

import argparse
import logging

from kafka import KafkaConsumer
from kafka.errors import KafkaError


def main_parser():
    parser = argparse.ArgumentParser(
        prog='python -m kafka.consumer',
        description='Kafka console consumer',
    )
    parser.add_argument(
        '-b', '--bootstrap-servers', type=str, action='append', required=True,
        help='host:port for cluster bootstrap servers')
    parser.add_argument(
        '-t', '--topic', type=str, action='append', dest='topics', required=True,
        help='subscribe to topic')
    parser.add_argument(
        '-g', '--group', type=str, required=True,
        help='consumer group')
    parser.add_argument(
        '-c', '--extra-config', type=str, action='append',
        help='additional configuration properties for kafka consumer')
    parser.add_argument(
        '-l', '--log-level', type=str,
        help='logging level, passed to logging.basicConfig')
    parser.add_argument(
        '-f', '--format', type=str, default='str',
        help='output format: str|raw|full')
    parser.add_argument(
        '--encoding', type=str, default='utf-8', help='encoding to use for str output decode()')
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
    config = parser.parse_args(args)
    if config.log_level:
        logging.basicConfig(level=_LOGGING_LEVELS[config.log_level.upper()])
    if config.format not in ('str', 'raw', 'full'):
        raise ValueError('Unrecognized format: %s' % config.format)
    logger = logging.getLogger(__name__)

    kwargs = build_kwargs(config.extra_config)
    consumer = KafkaConsumer(bootstrap_servers=config.bootstrap_servers, group_id=config.group, **kwargs)
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
    except KafkaError as e:
        logger.error(e)
        return 1
    except Exception:
        logger.exception('Error!')
        return 1
    finally:
        consumer.close()
