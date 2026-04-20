from collections import defaultdict

from kafka.protocol.consumer import OffsetSpec
from kafka.structs import TopicPartition


class ListPartitionOffsets:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser(
            'list-offsets',
            help='List offsets for partitions by spec (earliest/latest/timestamp)')
        parser.add_argument(
            '-p', '--partition', type=str, action='append',
            dest='partitions', default=[], required=True,
            help='TOPIC:PARTITION:SPEC triple (repeatable). PARTITION may be a '
                 'single partition, a closed range (0-2), an open range (1-), or '
                 'a single wildcard "*" for all partitions. SPEC may be one of '
                 'earliest, latest, max-timestamp, earliest-local, latest-tiered, '
                 'or a millisecond timestamp.')
        parser.set_defaults(command=cls.command)

    @classmethod
    def command(cls, client, args):
        tp_offsets = cls._parse_partition_specs(client, args.partitions)
        output = {}
        result = client.list_partition_offsets(tp_offsets)
        for tp, info in result.items():
            output[f'{tp.topic}:{tp.partition}'] = {
                'offset': info.offset,
                'timestamp': info.timestamp,
                'leader_epoch': info.leader_epoch,
                'spec': tp_offsets[tp]
            }
        return output

    @staticmethod
    def _parse_spec(spec):
        try:
            return int(spec)
        except ValueError:
            pass
        try:
            spec_key = spec.upper().replace('-', '_')
            return OffsetSpec[spec_key]
        except KeyError:
            raise ValueError(f'{spec_key} is not a valid OffsetSpec')

    @classmethod
    def _parse_partition_specs(cls, client, partitions):
        tp_offsets = {}
        for entry in partitions:
            topic, partition, spec_str = entry.rsplit(':', 2)
            spec = cls._parse_spec(spec_str)
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
