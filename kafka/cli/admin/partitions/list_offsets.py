from collections import defaultdict

from kafka.protocol.consumer import OffsetSpec
from kafka.structs import TopicPartition


class ListPartitionOffsets:
    COMMAND = 'list-offsets'
    HELP = 'List offsets for partitions by spec (earliest/latest/timestamp)'

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument(
            '-t', '--topic', type=str)
        parser.add_argument(
            '-s', '--spec', type=str,
            help='Spec may be one of earliest, latest, max-timestamp, earliest-local, '
            'latest-tiered, or a millisecond timestamp.')
        parser.add_argument(
            '-p', '--partition', type=str, action='append',
            dest='partitions', default=[],
            help='TOPIC:PARTITION:SPEC triple (repeatable). PARTITION may be a '
                 'single partition, a closed range (0-2), an open range (1-), or '
                 'a single wildcard "*" for all partitions. SPEC may be one of '
                 'earliest, latest, max-timestamp, earliest-local, latest-tiered, '
                 'or a millisecond timestamp.')

    @classmethod
    def command(cls, client, args):
        tp_offsets = cls._parse_partition_specs(client, args)
        output = defaultdict(dict)
        result = client.list_partition_offsets(tp_offsets)
        for tp, info in result.items():
            output[tp.topic][tp.partition] = {
                'offset': info.offset,
                'timestamp': info.timestamp,
                'leader_epoch': info.leader_epoch,
                'spec': tp_offsets[tp]
            }
        return dict(output)

    @classmethod
    def _parse_partition_specs(cls, client, args):
        if args.partitions:
            assert not args.topic and not args.spec, "Either --partition or (--topic and --spec) is supported, but not both."
            partitions = args.partitions
        else:
            assert args.topic and args.spec, "Both --topic and --spec must be provided."
            partitions = [f'{args.topic}:*:{args.spec}']
        tp_offsets = {}
        for entry in partitions:
            topic, partition, spec_str = entry.rsplit(':', 2)
            spec = OffsetSpec.build_from(spec_str)
            for tp in cls._parse_tp(client, topic, partition):
                if tp in tp_offsets:
                    # Passing multiple specs for a single partition results in an InvalidRequestError
                    raise ValueError('Only one spec allowed per partition')
                tp_offsets[tp] = spec
        return tp_offsets

    @classmethod
    def _parse_tp(cls, client, topic, partition, cache={}):
        try:
            return [TopicPartition(topic, int(partition))]
        except ValueError:
            pass
        if not partition == '*' and '-' not in partition:
            raise ValueError(f'Unrecognized partition: {partition}')

        if topic not in cache:
            cache[topic] = sorted([p['partition_index'] for p in client.describe_topics([topic])[0]['partitions']])

        if partition == '*':
            return [TopicPartition(topic, p)
                    for p in cache[topic]]

        elif '-' in partition:
            start, end = partition.split('-')
            if not start and not end:
                raise ValueError(f'Unrecognized partition: {partition}')
            if not start:
                start = cache[topic][0]
            if not end:
                end = cache[topic][-1]
            return [TopicPartition(topic, p)
                    for p in range(int(start), int(end) + 1)]
