class DescribeLogDirs:
    COMMAND = 'log-dirs'
    HELP = 'Get topic log directories and stats'

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument('--broker', type=int, action='append', dest='brokers', help='Query specific broker(s)')
        parser.add_argument('--topic', type=str, action='append', dest='topics', help='Get data about specific topic(s)')

    @classmethod
    def command(cls, client, args):
        return client.describe_log_dirs(topic_partitions=args.topics, brokers=args.brokers)
