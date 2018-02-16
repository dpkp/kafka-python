#!/usr/bin/env python

from kafka.streams.kafka import KafkaStreams
from kafka.streams.processor.topology_builder import TopologyBuilder
import logging
logging.basicConfig(level=logging.INFO)


def main():

    builder = TopologyBuilder()
    builder.add_source('foo', 'foo').add_sink('bar', 'bar', 'foo')

    hosts = ['[::1]:56686', '[::1]:56692', '[::1]:56702']

    streams = KafkaStreams(builder, application_id='dpkp-foobar', bootstrap_servers=hosts)
    streams.start()


if __name__ == '__main__':
    main()
