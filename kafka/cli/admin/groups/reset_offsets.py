from collections import defaultdict

from kafka.admin import OffsetSpec
from kafka.structs import OffsetAndMetadata, TopicPartition


class ResetGroupOffsets:
    COMMAND = 'reset-offsets'
    HELP = 'Reset committed offsets for a consumer group'

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument('-g', '--group-id', type=str, required=True)
        parser.add_argument(
            '-s', '--spec', type=str,
            help='Spec may be one of earliest, latest, max-timestamp, earliest-local, '
            'latest-tiered, or a millisecond timestamp. '
            'Applies to all topic/partitions currently in group. Mutually exclusive '
            'with --partition')
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
        if not args.spec and not args.partitions:
            raise ValueError('One of --spec or --partition is required')
        elif args.spec and args.partitions:
            raise ValueError('Only one of --spec and --partition are allowed')
        group = client.describe_groups([args.group_id])
        state = group[args.group_id]['group_state']
        if state not in ('Empty', 'Dead'):
            raise RuntimeError(f'Group {args.group_id} is {state}, expecting Empty or Dead!')
        offset_specs = {}
        if args.spec:
            offsets = client.list_group_offsets(args.group_id)
            spec = OffsetSpec.build_from(args.spec)
            offset_specs = {tp: spec for tp in offsets}
        else:
            offset_specs = cls._parse_partition_specs(args.partitions)
        result = client.reset_group_offsets(args.group_id, offset_specs)
        output = defaultdict(dict)
        for tp, res in result.items():
            res['error'] = res['error'].__name__
            output[tp.topic][tp.partition] = res
        return dict(output)

    @classmethod
    def _parse_partition_specs(cls, partitions):
        tp_offsets = {}
        for entry in partitions:
            topic, partition, spec_str = entry.rsplit(':', 2)
            spec = OffsetSpec.build_from(spec_str)
            tp = TopicPartition(topic, int(partition))
            if tp in tp_offsets:
                # Passing multiple specs for a single partition results in an InvalidRequestError
                raise ValueError('Only one spec allowed per partition')
            tp_offsets[tp] = spec
        return tp_offsets
