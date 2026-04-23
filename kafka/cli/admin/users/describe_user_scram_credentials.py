class DescribeUserScramCredentials:
    COMMAND = 'describe-scram-credentials'
    HELP = 'Describe SCRAM credentials for Kafka users'

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument(
            '--user', type=str, action='append', dest='users', default=[],
            help='User name to describe (repeatable). '
                 'If omitted, describes all users.')

    @classmethod
    def command(cls, client, args):
        users = args.users if args.users else None
        return client.describe_user_scram_credentials(users)
