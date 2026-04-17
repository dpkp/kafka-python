from .common import add_acl_filter_args, acl_filter_from_args


class DeleteACLs:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('delete', help='Delete Kafka ACLs')
        add_acl_filter_args(parser)
        parser.set_defaults(command=cls.command)

    @classmethod
    def command(cls, client, args):
        acl_filter = acl_filter_from_args(args)
        results = client.delete_acls([acl_filter])
        output = []
        for acl_filter, matching_acls, error in results:
            output.append({
                'filter': repr(acl_filter),
                'deleted': [repr(acl) for acl in matching_acls],
                'error': str(error),
            })
        return output
