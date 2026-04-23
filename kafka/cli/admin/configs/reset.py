from .common import add_resource_arguments, parse_resources


class ResetConfigs:
    COMMAND = 'reset'
    HELP = 'Reset Kafka Configs'

    @classmethod
    def add_arguments(cls, parser):
        add_resource_arguments(parser)
        parser.add_argument('-c', '--config', type=str, action='append', dest='configs', default=[], help='key to reset')
        parser.add_argument('-v', '--validate-only', action='store_true', default=False)
        parser.add_argument('--allow-unknown', action='store_false', dest='raise_on_unknown', default=True)

    @classmethod
    def command(cls, client, args):
        resources = parse_resources(args, configs=args.configs)
        return client.reset_configs(resources,
                                    validate_only=args.validate_only,
                                    raise_on_unknown=args.raise_on_unknown)
