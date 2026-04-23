class ListConfigResources:
    COMMAND = 'list'
    HELP = 'List config resources known to the cluster (requires broker >= 4.1 for non client_metrics types)'

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument(
            '-r', '--resource-type', type=str, action='append', dest='resource_types', default=[],
            help='Filter by resource type (repeatable): topic, broker, '
                  'broker_logger, client_metrics, group. Omit to list all '
                  'supported types.')

    @classmethod
    def command(cls, client, args):
        return client.list_config_resources(
            resource_types=args.resource_types or None)
