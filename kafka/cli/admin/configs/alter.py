from kafka.admin import ConfigResource


class AlterConfigs:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('alter', help='Alter Kafka Configs')
        parser.add_argument('-t', '--topic', type=str, action='append', dest='topics', default=[])
        parser.add_argument('-b', '--broker', type=str, action='append', dest='brokers', default=[])
        parser.add_argument('--broker-logger', type=str, action='append', dest='broker_loggers', default=[])
        parser.add_argument('-g', '--group', type=str, action='append', dest='groups', default=[])
        parser.add_argument('-c', '--config', type=str, action='append', dest='configs', default=None, help='key=value to alter')
        parser.add_argument('-v', '--validate-only', action='store_true', default=False)
        parser.add_argument('--allow-unknown', action='store_false', dest='raise_on_unknown', default=True)
        parser.set_defaults(command=cls.command)

    @classmethod
    def command(cls, client, args):
        configs = dict(config.split('=') for config in args.configs)
        resources = []
        for topic in args.topics:
            resources.append(ConfigResource('TOPIC', topic, configs))
        for broker in args.brokers:
            resources.append(ConfigResource('BROKER', broker, configs))
        for broker in args.broker_loggers:
            resources.append(ConfigResource('BROKER_LOGGER', broker, configs))
        for group in args.groups:
            resources.append(ConfigResource('GROUP', group, configs))
        return client.alter_configs(resources,
                                    validate_only=args.validate_only,
                                    raise_on_unknown=args.raise_on_unknown)
