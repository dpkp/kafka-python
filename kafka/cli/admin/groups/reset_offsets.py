from collections import defaultdict

from kafka.admin import OffsetSpec
from kafka.structs import OffsetAndMetadata, TopicPartition


class ResetGroupOffsets:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser(
            'reset-offsets',
            help='Reset committed offsets for a consumer group')
        parser.add_argument('-g', '--group-id', type=str, required=True)
        parser.add_argument(
            '-s', '--spec', type=str, required=True)
        parser.set_defaults(command=cls.command)

    @classmethod
    def command(cls, client, args):
        offsets = client.list_group_offsets(args.group_id)
        try:
            spec = OffsetSpec[args.spec.upper().replace('-', '_')]
        except KeyError:
            raise ValueError(f'Unrecognized spec: {args.spec}')
        offset_specs = {tp: spec for tp in offsets}
        result = client.reset_group_offsets(args.group_id, offset_specs)
        output = defaultdict(dict)
        for tp, res in result.items():
            res['error'] = res['error'].__name__
            output[tp.topic][tp.partition] = res
        return dict(output)
