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
        partitions = [TopicPartition(topic, partition) for topic in offsets for partition in offsets[topic]]
        latest = client.list_partition_offsets({tp: OffsetSpec.LATEST for tp in partitions})
        for tp in latest:
            part_res = offsets[tp.topic][tp.partition]
            part_res['latest_offset'] = latest[tp].offset
            part_res['lag'] = latest[tp].offset - part_res['committed_offset']
        return offsets
