from .common import add_resource_arguments, parse_resources


class AlterConfigs:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('alter', help='Alter Kafka Configs')
        add_resource_arguments(parser)
        parser.add_argument('-c', '--config', type=str, action='append', dest='configs', required=True, help='key=value to alter')
        parser.add_argument('-v', '--validate-only', action='store_true', default=False)
        parser.add_argument('--allow-unknown', action='store_false', dest='raise_on_unknown', default=True)
        parser.set_defaults(command=cls.command)

    @classmethod
    def command(cls, client, args):
        try:
            configs = dict(config.split('=') for config in args.configs)
        except ValueError:
            raise ValueError(f'Unable to parse configs! {args.configs}')
        resources = parse_resources(args, configs=configs)
        return client.alter_configs(resources,
                                    validate_only=args.validate_only,
                                    raise_on_unknown=args.raise_on_unknown)
