from collections import defaultdict

from kafka.structs import TopicPartition
from kafka.admin import OffsetSpec


class ListGroupOffsets:
    COMMAND = 'list-offsets'
    HELP = 'List Offsets for Group'

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument('-g', '--group-id', type=str, required=True)

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
