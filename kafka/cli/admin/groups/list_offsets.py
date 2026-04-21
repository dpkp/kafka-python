from collections import defaultdict

from kafka.structs import TopicPartition
from kafka.admin import OffsetSpec


class ListGroupOffsets:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('list-offsets', help='List Offsets for Group')
        parser.add_argument('-g', '--group-id', type=str, required=True)
        parser.set_defaults(command=cls.command)

    @classmethod
    def command(cls, client, args):
        offsets = client.list_group_offsets(args.group_id)
        latest = client.list_partition_offsets({tp: OffsetSpec.LATEST for tp in offsets})
        results = defaultdict(dict)
        for tp in latest:
            committed = offsets[tp]
            results[tp.topic][tp.partition] = {
                'offset': committed.offset,
                'leader_epoch': committed.leader_epoch,
                'metadata': committed.metadata,
                'latest_offset': latest[tp].offset,
                'lag': latest[tp].offset - committed.offset,
            }
        return dict(results)
