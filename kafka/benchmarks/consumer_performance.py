#!/usr/bin/env python
# Adapted from https://github.com/mrafayaleem/kafka-jython

from __future__ import absolute_import, print_function

import argparse
import pprint
import sys
import threading
import time
import traceback

from kafka import KafkaConsumer


class ConsumerPerformance(object):
    @staticmethod
    def run(args):
        try:
            props = {}
            for prop in args.consumer_config:
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
                props[k] = v

            print('Initializing Consumer...')
            props['bootstrap_servers'] = args.bootstrap_servers
            props['auto_offset_reset'] = 'earliest'
            if 'group_id' not in props:
                props['group_id'] = 'kafka-consumer-benchmark'
            if 'consumer_timeout_ms' not in props:
                props['consumer_timeout_ms'] = 10000
            props['metrics_sample_window_ms'] = args.stats_interval * 1000
            for k, v in props.items():
                print('---> {0}={1}'.format(k, v))
            consumer = KafkaConsumer(args.topic, **props)
            print('---> group_id={0}'.format(consumer.config['group_id']))
            print('---> report stats every {0} secs'.format(args.stats_interval))
            print('---> raw metrics? {0}'.format(args.raw_metrics))
            timer_stop = threading.Event()
            timer = StatsReporter(args.stats_interval, consumer,
                                  event=timer_stop,
                                  raw_metrics=args.raw_metrics)
            timer.start()
            print('-> OK!')
            print()

            start_time = time.time()
            records = 0
            for msg in consumer:
                records += 1
                if records >= args.num_records:
                    break

            end_time = time.time()
            timer_stop.set()
            timer.join()
            print('Consumed {0} records'.format(records))
            print('Execution time:', end_time - start_time, 'secs')

        except Exception:
            exc_info = sys.exc_info()
            traceback.print_exception(*exc_info)
            sys.exit(1)


class StatsReporter(threading.Thread):
    def __init__(self, interval, consumer, event=None, raw_metrics=False):
        super(StatsReporter, self).__init__()
        self.interval = interval
        self.consumer = consumer
        self.event = event
        self.raw_metrics = raw_metrics

    def print_stats(self):
        metrics = self.consumer.metrics()
        if self.raw_metrics:
            pprint.pprint(metrics)
        else:
            print('{records-consumed-rate} records/sec ({bytes-consumed-rate} B/sec),'
                  ' {fetch-latency-avg} latency,'
                  ' {fetch-rate} fetch/s,'
                  ' {fetch-size-avg} fetch size,'
                  ' {records-lag-max} max record lag,'
                  ' {records-per-request-avg} records/req'
                  .format(**metrics['consumer-fetch-manager-metrics']))


    def print_final(self):
        self.print_stats()

    def run(self):
        while self.event and not self.event.wait(self.interval):
            self.print_stats()
        else:
            self.print_final()


def get_args_parser():
    parser = argparse.ArgumentParser(
        description='This tool is used to verify the consumer performance.')

    parser.add_argument(
        '--bootstrap-servers', type=str, nargs='+', default=(),
        help='host:port for cluster bootstrap servers')
    parser.add_argument(
        '--topic', type=str,
        help='Topic for consumer test (default: kafka-python-benchmark-test)',
        default='kafka-python-benchmark-test')
    parser.add_argument(
        '--num-records', type=int,
        help='number of messages to consume (default: 1000000)',
        default=1000000)
    parser.add_argument(
        '--consumer-config', type=str, nargs='+', default=(),
        help='kafka consumer related configuration properties like '
             'bootstrap_servers,client_id etc..')
    parser.add_argument(
        '--fixture-compression', type=str,
        help='specify a compression type for use with broker fixtures / producer')
    parser.add_argument(
        '--stats-interval', type=int,
        help='Interval in seconds for stats reporting to console (default: 5)',
        default=5)
    parser.add_argument(
        '--raw-metrics', action='store_true',
        help='Enable this flag to print full metrics dict on each interval')
    return parser


if __name__ == '__main__':
    args = get_args_parser().parse_args()
    ConsumerPerformance.run(args)
