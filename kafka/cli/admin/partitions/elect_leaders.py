from collections import defaultdict


_ELECTION_TYPES = {'preferred': 0, 'unclean': 1}


class ElectLeaders:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser(
            'elect-leaders',
            help='Trigger leader election for partitions')
        parser.add_argument(
            '--election-type', type=str, default='preferred',
            choices=sorted(_ELECTION_TYPES),
            help='Election type (default: preferred)')
        parser.add_argument(
            '-p', '--topic-partition', type=str, action='append',
            dest='topic_partitions', default=[],
            help='TOPIC:PARTITION pair (repeatable). Omit to elect leaders for '
                 'all partitions of all topics.')
        parser.add_argument(
            '-t', '--topic', type=str, action='append',
            dest='topics', default=[],
            help='Elect leaders for all partitions of TOPIC (repeatable). '
                 'Mutually exclusive with --topic-partition.')
        parser.add_argument(
            '--timeout-ms', type=int, default=None,
            help='Request timeout in milliseconds')
        parser.add_argument(
            '--no-raise-errors', dest='raise_errors', action='store_false',
            help='Do not raise on partition-level errors; return the response instead')
        parser.set_defaults(command=cls.command, raise_errors=True)

    @classmethod
    def command(cls, client, args):
        if args.topic_partitions and args.topics:
            raise ValueError(
                '--topic-partition and --topic are mutually exclusive')
        if args.topic_partitions:
            topic2partitions = defaultdict(list)
            for spec in args.topic_partitions:
                topic, partition = spec.rsplit(':', 1)
                topic2partitions[topic].append(int(partition))
            topic_partitions = dict(topic2partitions)
        elif args.topics:
            topic_partitions = args.topics
        else:
            topic_partitions = None
        return client.elect_leaders(
            _ELECTION_TYPES[args.election_type],
            topic_partitions=topic_partitions,
            timeout_ms=args.timeout_ms,
            raise_errors=args.raise_errors)
