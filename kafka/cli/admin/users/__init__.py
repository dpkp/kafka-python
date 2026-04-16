import sys

from .alter_user_scram_credentials import AlterUserScramCredentials


class UsersSubCommand:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('users', help='Manage Kafka Users')
        commands = parser.add_subparsers()
        for cmd in [AlterUserScramCredentials]:
            cmd.add_subparser(commands)
        parser.set_defaults(command=lambda *_args: parser.print_help() or sys.exit(2))
