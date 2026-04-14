class DescribeLogDirs:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('log-dirs', help='Get topic log directories and stats')
        parser.add_argument('-b', '--broker', type=int, action='append', dest='brokers', help='Query specific broker(s)')
        parser.add_argument('-t', '--topic', type=str, action='append', dest='topics', help='Get data about specific topic(s)')
        parser.set_defaults(command=lambda cli, args: cli.describe_log_dirs(topic_partitions=args.topics, brokers=args.brokers))
