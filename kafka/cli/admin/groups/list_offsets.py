class ListGroupOffsets:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('list-offsets', help='List Offsets for Group')
        parser.add_argument('-g', '--group-id', type=str, required=True)
        parser.set_defaults(command=lambda cli, args: cli.list_group_offsets(args.group_id))
