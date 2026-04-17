class DescribeUserScramCredentials:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser(
            'describe-scram-credentials',
            help='Describe SCRAM credentials for Kafka users')
        parser.add_argument(
            '--user', type=str, action='append', dest='users', default=[],
            help='User name to describe (repeatable). '
                 'If omitted, describes all users.')
        parser.set_defaults(command=cls.command)

    @classmethod
    def command(cls, client, args):
        users = args.users if args.users else None
        return client.describe_user_scram_credentials(users)
