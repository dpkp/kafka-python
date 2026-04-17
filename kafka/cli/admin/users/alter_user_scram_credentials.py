from kafka.admin import (
    ScramMechanism,
    UserScramCredentialDeletion,
    UserScramCredentialUpsertion,
)


class AlterUserScramCredentials:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser(
            'alter-scram-credentials',
            help='Alter SCRAM credentials for Kafka users')
        parser.add_argument(
            '--delete', type=str, action='append', dest='deletions', default=[],
            help='USER:MECHANISM pair to delete (e.g. alice:SCRAM-SHA-256)')
        parser.add_argument(
            '--upsert', type=str, action='append', dest='upsertions', default=[],
            help='USER:MECHANISM:PASSWORD triple to insert or update')
        parser.add_argument(
            '--iterations', type=int, default=None,
            help='PBKDF2 iteration count for upsertions (default: 4096)')
        parser.set_defaults(command=cls.command)

    @classmethod
    def command(cls, client, args):
        alterations = []
        for spec in args.deletions:
            user, mechanism = spec.split(':', 1)
            alterations.append(UserScramCredentialDeletion(user, mechanism))
        for spec in args.upsertions:
            user, mechanism, password = spec.split(':', 2)
            alterations.append(UserScramCredentialUpsertion(
                user, mechanism, password, iterations=args.iterations))
        return client.alter_user_scram_credentials(alterations)
