from .common import add_acl_args, acl_from_args


class CreateACLs:
    COMMAND = 'create'
    HELP = 'Create Kafka ACLs'

    @classmethod
    def add_arguments(cls, parser):
        add_acl_args(parser, required=True)

    @classmethod
    def command(cls, client, args):
        acl = acl_from_args(args)
        result = client.create_acls([acl])
        return {
            'succeeded': [repr(a) for a in result['succeeded']],
            'failed': [str(e) for e in result['failed']],
        }
