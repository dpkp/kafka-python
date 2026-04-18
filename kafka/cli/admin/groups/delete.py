class DeleteGroups:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('delete', help='Delete Groups')
        parser.add_argument('-g', '--group-id', type=str, action='append', dest='groups', required=True)
        parser.set_defaults(command=lambda cli, args: cli.delete_groups(args.groups))
