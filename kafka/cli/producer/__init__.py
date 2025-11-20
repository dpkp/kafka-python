from __future__ import absolute_import, print_function

import argparse
import logging
import sys

from kafka import KafkaProducer


def main_parser():
    parser = argparse.ArgumentParser(
        prog='python -m kafka.producer',
        description='Kafka console producer',
    )
    parser.add_argument(
        '-b', '--bootstrap-servers', type=str, action='append', required=True,
        help='host:port for cluster bootstrap servers')
    parser.add_argument(
        '-t', '--topic', type=str, required=True,
        help='publish to topic')
    parser.add_argument(
        '-c', '--extra-config', type=str, action='append',
        help='additional configuration properties for kafka producer')
    parser.add_argument(
        '-l', '--log-level', type=str,
        help='logging level, passed to logging.basicConfig')
    parser.add_argument(
        '--encoding', type=str, default='utf-8',
        help='byte encoding for produced messages')
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
    logger = logging.getLogger(__name__)

    kwargs = build_kwargs(config.extra_config)
    producer = KafkaProducer(bootstrap_servers=config.bootstrap_servers, **kwargs)

    def log_result(res_or_err):
        if isinstance(res_or_err, Exception):
            logger.error("Error producing message", exc_info=res_or_err)
        else:
            logger.info("Message produced: %s", res_or_err)

    try:
        input_py23 = raw_input
    except NameError:
        input_py23 = input

    try:
        while True:
            try:
                value = input_py23()
            except EOFError:
                value = sys.stdin.read().rstrip('\n')
                if not value:
                    return 0
            producer.send(config.topic, value=value.encode(config.encoding)).add_both(log_result)
    except KeyboardInterrupt:
        logger.info('Bye!')
        return 0
    except Exception:
        logger.exception('Error!')
        return 1
    finally:
        producer.close()
