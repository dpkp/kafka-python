class ListGroups:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('list', help='List Groups')
        parser.set_defaults(command=lambda cli, _args: cli.list_consumer_groups())
