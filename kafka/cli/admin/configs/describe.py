from kafka.admin import ConfigResource


class DescribeConfigs:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('describe', help='Describe Kafka Configs')
        parser.add_argument('-t', '--topic', type=str, action='append', dest='topics', default=[])
        parser.add_argument('-b', '--broker', type=str, action='append', dest='brokers', default=[])
        parser.add_argument('--broker-logger', type=str, action='append', dest='broker_loggers', default=[])
        parser.add_argument('-g', '--group', type=str, action='append', dest='groups', default=[])
        parser.add_argument('-k', '--key', type=str, action='append', dest='keys', default=None)
        parser.set_defaults(command=cls.command)

    @classmethod
    def command(cls, client, args):
        resources = []
        for topic in args.topics:
            resources.append(ConfigResource('TOPIC', topic, args.keys))
        for broker in args.brokers:
            resources.append(ConfigResource('BROKER', broker, args.keys))
        for broker in args.broker_loggers:
            resources.append(ConfigResource('BROKER_LOGGER', broker, args.keys))
        for group in args.groups:
            resources.append(ConfigResource('GROUP', group, args.keys))

        return client.describe_configs(resources)
