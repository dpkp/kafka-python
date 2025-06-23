from __future__ import absolute_import

from kafka.admin.config_resource import ConfigResource


class DescribeConfigs:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('describe', help='Describe Kafka Configs')
        parser.add_argument('-t', '--topic', type=str, action='append', dest='topics', default=[])
        parser.add_argument('-b', '--broker', type=str, action='append', dest='brokers', default=[])
        parser.set_defaults(command=cls.command)

    @classmethod
    def command(cls, client, args):
        resources = []
        for topic in args.topics:
            resources.append(ConfigResource('TOPIC', topic))
        for broker in args.brokers:
            resources.append(ConfigResource('BROKER', broker))

        response = client.describe_configs(resources)
        return list(zip([(r.resource_type.name, r.name) for r in resources], [{str(vals[0]): vals[1] for vals in r.resources[0][4]} for r in response]))
