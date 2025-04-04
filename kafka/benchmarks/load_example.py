#!/usr/bin/env python
from __future__ import print_function

import argparse
import logging
import threading
import time

from kafka import KafkaConsumer, KafkaProducer


class Producer(threading.Thread):

    def __init__(self, bootstrap_servers, topic, stop_event, msg_size):
        super(Producer, self).__init__()
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.stop_event = stop_event
        self.big_msg = b'1' * msg_size

    def run(self):
        producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers)
        self.sent = 0

        while not self.stop_event.is_set():
            producer.send(self.topic, self.big_msg)
            self.sent += 1
        producer.flush()
        producer.close()


class Consumer(threading.Thread):
    def __init__(self, bootstrap_servers, topic, stop_event, msg_size):
        super(Consumer, self).__init__()
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.stop_event = stop_event
        self.msg_size = msg_size

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers=self.bootstrap_servers,
                                 auto_offset_reset='earliest')
        consumer.subscribe([self.topic])
        self.valid = 0
        self.invalid = 0

        for message in consumer:
            if len(message.value) == self.msg_size:
                self.valid += 1
            else:
                print('Invalid message:', len(message.value), self.msg_size)
                self.invalid += 1

            if self.stop_event.is_set():
                break
        consumer.close()


def get_args_parser():
    parser = argparse.ArgumentParser(
        description='This tool is used to demonstrate consumer and producer load.')

    parser.add_argument(
        '--bootstrap-servers', type=str, nargs='+', default=('localhost:9092'),
        help='host:port for cluster bootstrap servers (default: localhost:9092)')
    parser.add_argument(
        '--topic', type=str,
        help='Topic for load test (default: kafka-python-benchmark-load-example)',
        default='kafka-python-benchmark-load-example')
    parser.add_argument(
        '--msg-size', type=int,
        help='Message size, in bytes, for load test (default: 524288)',
        default=524288)
    parser.add_argument(
        '--load-time', type=int,
        help='number of seconds to run load test (default: 10)',
        default=10)
    parser.add_argument(
        '--log-level', type=str,
        help='Optional logging level for load test: ERROR|INFO|DEBUG etc',
        default=None)
    return parser


def main(args):
    if args.log_level:
        logging.basicConfig(
            format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
            level=getattr(logging, args.log_level))
    producer_stop = threading.Event()
    consumer_stop = threading.Event()
    threads = [
        Producer(args.bootstrap_servers, args.topic, producer_stop, args.msg_size),
        Consumer(args.bootstrap_servers, args.topic, consumer_stop, args.msg_size)
    ]

    for t in threads:
        t.start()

    time.sleep(args.load_time)
    producer_stop.set()
    consumer_stop.set()
    print('Messages sent: %d' % threads[0].sent)
    print('Messages recvd: %d' % threads[1].valid)
    print('Messages invalid: %d' % threads[1].invalid)


if __name__ == "__main__":
    args = get_args_parser().parse_args()
    main(args)
