class DescribeTopicPartitions:
    COMMAND = 'describe'
    HELP = 'Describe topic partitions with pagination (KIP-966, broker >=3.9)'

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument(
            '-t', '--topic', type=str, action='append',
            dest='topics', default=[], required=True,
            help='Topic to describe (repeatable)')
        parser.add_argument(
            '--response-partition-limit', type=int, default=2000,
            help='Maximum number of partitions to include in the response '
                 '(default: 2000)')
        parser.add_argument(
            '--cursor-topic', type=str, default=None,
            help='Topic name to start pagination from')
        parser.add_argument(
            '--cursor-partition', type=int, default=None,
            help='Partition index to start pagination from')

    @classmethod
    def command(cls, client, args):
        cursor = None
        if args.cursor_topic is not None or args.cursor_partition is not None:
            if args.cursor_topic is None or args.cursor_partition is None:
                raise ValueError(
                    '--cursor-topic and --cursor-partition must be used together')
            cursor = {
                'topic_name': args.cursor_topic,
                'partition_index': args.cursor_partition,
            }
        return client.describe_topic_partitions(
            args.topics,
            response_partition_limit=args.response_partition_limit,
            cursor=cursor)
