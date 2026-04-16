from .common import add_acl_args, acl_from_args


class CreateACLs:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('create', help='Create Kafka ACLs')
        add_acl_args(parser, required=True)
        parser.set_defaults(command=cls.command)

    @classmethod
    def command(cls, client, args):
        acls = acl_from_args(args)
        result = client.create_acls([acl])
        return {
            'succeeded': [repr(a) for a in result['succeeded']],
            'failed': [str(e) for e in result['failed']],
        }
