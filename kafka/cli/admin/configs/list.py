class ListConfigResources:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser(
            'list',
            help='List config resources known to the cluster (requires broker >= 4.1 '
                 'for non client_metrics types)')
        parser.add_argument(
            '-r', '--resource-type', type=str, action='append', dest='resource_types', default=[],
            help='Filter by resource type (repeatable): topic, broker, '
                  'broker_logger, client_metrics, group. Omit to list all '
                  'supported types.')
        parser.set_defaults(command=cls.command)

    @classmethod
    def command(cls, client, args):
        return client.list_config_resources(
            resource_types=args.resource_types or None)
