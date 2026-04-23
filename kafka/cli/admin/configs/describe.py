from .common import add_resource_arguments, parse_resources


class DescribeConfigs:
    COMMAND = 'describe'
    HELP = 'Describe Kafka Configs'

    @classmethod
    def add_arguments(cls, parser):
        add_resource_arguments(parser)
        parser.add_argument('-c', '--config', type=str, action='append', dest='configs', default=None)
        parser.add_argument('--dynamic', action='store_true', default=False)
        parser.add_argument('--modified', action='store_true', default=False)
        parser.add_argument('--static', action='store_true', default=False)
        parser.add_argument('--default', action='store_true', default=False)

    @classmethod
    def command(cls, client, args):
        resources = parse_resources(args, configs=args.configs)
        if args.modified:
            config_filter = 'modified'
        elif args.dynamic:
            config_filter = 'dynamic'
        elif args.static:
            config_filter = 'static'
        elif args.default:
            config_filter = 'default'
        else:
            config_filter = 'all'
        return client.describe_configs(resources, config_filter=config_filter)
