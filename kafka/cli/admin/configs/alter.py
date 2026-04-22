import re

from kafka.admin import AlterConfigOp
from .common import add_resource_arguments, parse_resources


class AlterConfigs:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('alter', help='Alter Kafka Configs')
        add_resource_arguments(parser)
        parser.add_argument('-c', '--config', type=str, action='append', dest='configs', required=True, help='key=value to alter')
        parser.add_argument('-v', '--validate-only', action='store_true', default=False)
        parser.add_argument('--allow-unknown', action='store_false', dest='raise_on_unknown', default=True)
        incremental = parser.add_mutually_exclusive_group()
        incremental.add_argument('--force-incremental', action='store_true', dest='incremental', default=None)
        incremental.add_argument('--force-alter', action='store_false', dest='incremental', default=None)
        parser.set_defaults(command=cls.command)

    @classmethod
    def command(cls, client, args):
        try:
            configs = dict(config.split('=') for config in args.configs)
            regex = r'^(set|del|add|sub)\((.*)\)$'
            ops = {
                'set': AlterConfigOp.SET,
                'del': AlterConfigOp.DELETE,
                'add': AlterConfigOp.APPEND,
                'sub': AlterConfigOp.SUBTRACT,
            }
            for key in configs:
                match = re.match(regex, configs[key])
                if match:
                    op_str, val = match.groups()
                    configs[key] = (ops[op_str], val)
        except ValueError:
            raise ValueError(f'Unable to parse configs! {args.configs}')
        resources = parse_resources(args, configs=configs)
        return client.alter_configs(resources,
                                    validate_only=args.validate_only,
                                    raise_on_unknown=args.raise_on_unknown,
                                    incremental=args.incremental)
