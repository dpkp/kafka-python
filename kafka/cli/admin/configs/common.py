from kafka.admin import ConfigResource


def add_resource_arguments(parser):
    """Add arguments for specifying ConfigResource"""
    parser.add_argument(
        '-r', '--resource-type', type=str, required=True,
        help='Type of resource to describe: topic, broker, broker_logger, '
             'client_metrics, group.')
    parser.add_argument(
        '-n', '--resource-name', type=str, action='append', dest='resource_names', required=True,
        help='Name of resource(s) to describe. May be repeated.')


def parse_resources(args, configs=None):
    return [ConfigResource(args.resource_type.upper().replace('-', '_'), resource_name, configs)
            for resource_name in args.resource_names]
