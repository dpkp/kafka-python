class CreatePartitions:
    COMMAND = 'create'
    HELP = 'Create additional partitions for existing topics'

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument(
            '-p', '--topic-partitions', type=str, action='append',
            dest='topic_partitions', default=[], required=True,
            help='TOPIC:TOTAL_PARTITION_COUNT pair (repeatable)')
        parser.add_argument(
            '--timeout-ms', type=int, default=None,
            help='Request timeout in milliseconds')
        parser.add_argument(
            '--validate-only', action='store_true',
            help='Validate the request without actually creating partitions')

    @classmethod
    def command(cls, client, args):
        topic_partitions = {}
        for spec in args.topic_partitions:
            topic, count = spec.rsplit(':', 1)
            topic_partitions[topic] = int(count)
        return client.create_partitions(
            topic_partitions,
            timeout_ms=args.timeout_ms,
            validate_only=args.validate_only)
