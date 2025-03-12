#!/usr/bin/env python
# Adapted from https://github.com/mrafayaleem/kafka-jython

from __future__ import absolute_import, print_function

import argparse
import pprint
import sys
import threading
import traceback

from kafka.vendor.six.moves import range

from kafka import KafkaProducer
from test.fixtures import KafkaFixture, ZookeeperFixture


def start_brokers(n):
    print('Starting {0} {1}-node cluster...'.format(KafkaFixture.kafka_version, n))
    print('-> 1 Zookeeper')
    zk = ZookeeperFixture.instance()
    print('---> {0}:{1}'.format(zk.host, zk.port))
    print()

    partitions = min(n, 3)
    replicas = min(n, 3)
    print('-> {0} Brokers [{1} partitions / {2} replicas]'.format(n, partitions, replicas))
    brokers = [
        KafkaFixture.instance(i, zk, zk_chroot='',
                              partitions=partitions, replicas=replicas)
        for i in range(n)
    ]
    for broker in brokers:
        print('---> {0}:{1}'.format(broker.host, broker.port))
    print()
    return brokers


class ProducerPerformance(object):

    @staticmethod
    def run(args):
        try:
            props = {}
            for prop in args.producer_config:
                k, v = prop.split('=')
                try:
                    v = int(v)
                except ValueError:
                    pass
                if v == 'None':
                    v = None
                props[k] = v

            if args.brokers:
                brokers = start_brokers(args.brokers)
                props['bootstrap_servers'] = ['{0}:{1}'.format(broker.host, broker.port)
                                              for broker in brokers]
                print("---> bootstrap_servers={0}".format(props['bootstrap_servers']))
                print()
                print('-> OK!')
                print()

            print('Initializing producer...')
            record = bytes(bytearray(args.record_size))
            props['metrics_sample_window_ms'] = args.stats_interval * 1000

            producer = KafkaProducer(**props)
            for k, v in props.items():
                print('---> {0}={1}'.format(k, v))
            print('---> send {0} byte records'.format(args.record_size))
            print('---> report stats every {0} secs'.format(args.stats_interval))
            print('---> raw metrics? {0}'.format(args.raw_metrics))
            timer_stop = threading.Event()
            timer = StatsReporter(args.stats_interval, producer,
                                  event=timer_stop,
                                  raw_metrics=args.raw_metrics)
            timer.start()
            print('-> OK!')
            print()

            for i in range(args.num_records):
                producer.send(topic=args.topic, value=record)
            producer.flush()

            timer_stop.set()

        except Exception:
            exc_info = sys.exc_info()
            traceback.print_exception(*exc_info)
            sys.exit(1)


class StatsReporter(threading.Thread):
    def __init__(self, interval, producer, event=None, raw_metrics=False):
        super(StatsReporter, self).__init__()
        self.interval = interval
        self.producer = producer
        self.event = event
        self.raw_metrics = raw_metrics

    def print_stats(self):
        metrics = self.producer.metrics()
        if self.raw_metrics:
            pprint.pprint(metrics)
        else:
            print('{record-send-rate} records/sec ({byte-rate} B/sec),'
                  ' {request-latency-avg} latency,'
                  ' {record-size-avg} record size,'
                  ' {batch-size-avg} batch size,'
                  ' {records-per-request-avg} records/req'
                  .format(**metrics['producer-metrics']))

    def print_final(self):
        self.print_stats()

    def run(self):
        while self.event and not self.event.wait(self.interval):
            self.print_stats()
        else:
            self.print_final()


def get_args_parser():
    parser = argparse.ArgumentParser(
        description='This tool is used to verify the producer performance.')

    parser.add_argument(
        '--topic', type=str,
        help='Topic name for test',
        default='kafka-python-benchmark-test')
    parser.add_argument(
        '--num-records', type=int,
        help='number of messages to produce',
        default=1000000)
    parser.add_argument(
        '--record-size', type=int,
        help='message size in bytes',
        default=100)
    parser.add_argument(
        '--producer-config', type=str, nargs='+', default=(),
        help='kafka producer related configuaration properties like '
             'bootstrap_servers,client_id etc..')
    parser.add_argument(
        '--brokers', type=int,
        help='Number of kafka brokers to start',
        default=0)
    parser.add_argument(
        '--stats-interval', type=int,
        help='Interval in seconds for stats reporting to console',
        default=5)
    parser.add_argument(
        '--raw-metrics', action='store_true',
        help='Enable this flag to print full metrics dict on each interval')
    return parser


if __name__ == '__main__':
    args = get_args_parser().parse_args()
    ProducerPerformance.run(args)
