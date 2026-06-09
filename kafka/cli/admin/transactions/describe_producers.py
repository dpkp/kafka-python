from kafka.structs import TopicPartition


class DescribeProducers:
    COMMAND = 'describe-producers'
    HELP = 'Describe active producers on a partition (broker >= 2.8)'

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument(
            '-t', '--topic', type=str, required=True,
            help='Topic name.')
        parser.add_argument(
            '-p', '--partition', type=int, action='append',
            dest='partitions', required=True,
            help='Partition index (repeatable).')
        parser.add_argument(
            '--broker-id', type=int, default=None,
            help='Send to this replica instead of the partition leader.')

    @classmethod
    def command(cls, client, args):
        tps = [TopicPartition(args.topic, p) for p in args.partitions]
        results = client.describe_producers(tps, broker_id=args.broker_id)
        return {
            f'{tp.topic}:{tp.partition}': {
                'active_producers': [p._asdict() for p in state.active_producers],
            } for tp, state in results.items()
        }
