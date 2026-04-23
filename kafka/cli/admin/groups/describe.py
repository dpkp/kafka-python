class DescribeGroups:
    COMMAND = 'describe'
    HELP = 'Describe Groups'

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument('-g', '--group-id', type=str, action='append', dest='groups', required=True)

    @classmethod
    def command(cls, client, args):
        return client.describe_groups(args.groups)
