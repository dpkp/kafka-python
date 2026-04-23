from .alter_user_scram_credentials import AlterUserScramCredentials
from .describe_user_scram_credentials import DescribeUserScramCredentials


class UsersCommandGroup:
    GROUP = 'users'
    HELP = 'Manage Kafka Users'
    COMMANDS = [DescribeUserScramCredentials, AlterUserScramCredentials]
