import argparse
import logging
import sys

from kafka import KafkaProducer
from ..common import add_common_cli_args, configure_logging, build_connect_kwargs


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


def run_cli(args=None):
    parser = main_parser()
    config = parser.parse_args(args)

    configure_logging(config)
    logger = logging.getLogger(__name__)

    kwargs = build_connect_kwargs(config)
    producer = KafkaProducer(**kwargs)

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
