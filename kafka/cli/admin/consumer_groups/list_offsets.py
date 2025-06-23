from __future__ import absolute_import


class ListConsumerGroupOffsets:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('list-offsets', help='List Offsets for Consumer Group')
        parser.add_argument('-g', '--group-id', type=str, required=True)
        parser.set_defaults(command=lambda cli, args: cli.list_consumer_group_offsets(args.group_id))
