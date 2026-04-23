from collections import defaultdict

from kafka.structs import TopicPartition


class ListPartitionReassignments:
    COMMAND = 'list-reassignments'
    HELP = 'List the current ongoing partition reassignments'

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument(
            '-p', '--topic-partition', type=str, action='append',
            dest='topic_partitions', default=[],
            help='TOPIC:PARTITION pair (repeatable). Omit to list '
                 'reassignments for all partitions.')
        parser.add_argument(
            '--timeout-ms', type=int, default=None,
            help='Request timeout in milliseconds')

    @classmethod
    def command(cls, client, args):
        if args.topic_partitions:
            topic2partitions = defaultdict(list)
            for spec in args.topic_partitions:
                topic, partition = spec.rsplit(':', 1)
                topic2partitions[topic].append(int(partition))
            topic_partitions = dict(topic2partitions)
        else:
            topic_partitions = None
        result = client.list_partition_reassignments(
            topic_partitions, timeout_ms=args.timeout_ms)
        # TopicPartition keys don't render cleanly in --format json; normalize
        # to "topic:partition" strings for CLI output.
        return {'%s:%d' % (tp.topic, tp.partition): v for tp, v in result.items()}
