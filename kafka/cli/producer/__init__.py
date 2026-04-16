import argparse
import logging
import sys

from kafka import KafkaProducer
from ..common import add_common_cli_args


def main_parser():
    parser = argparse.ArgumentParser(
        prog='python -m kafka.producer',
        description='Kafka console producer',
    )
    add_common_cli_args(parser)
    parser.add_argument(
        '-t', '--topic', type=str, required=True,
        help='publish to topic')
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

    logger = logging.getLogger(__name__)

    kwargs = build_kwargs(config.extra_config)
    producer = KafkaProducer(bootstrap_servers=config.bootstrap_servers, **kwargs)

    def log_result(res_or_err):
        if isinstance(res_or_err, Exception):
            logger.error("Error producing message", exc_info=res_or_err)
        else:
            logger.info("Message produced: %s", res_or_err)

    try:
        while True:
            try:
                value = input()
            except EOFError:
                value = sys.stdin.read().rstrip('\n')
                if not value:
                    return 0
            producer.send(config.topic, value=value.encode(config.encoding)).add_both(log_result)
    except KeyboardInterrupt:
        logger.info('Bye!')
        return 0
    except Exception:
        logger.critical('Error!', exc_info=True)
        return 1
    finally:
        producer.close()
