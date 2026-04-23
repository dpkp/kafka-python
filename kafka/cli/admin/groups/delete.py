class DeleteGroups:
    COMMAND = 'delete'
    HELP = 'Delete Groups'

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument('-g', '--group-id', type=str, action='append', dest='groups', required=True)

    @classmethod
    def command(cls, client, args):
        return client.delete_groups(args.groups)
