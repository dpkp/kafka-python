from .common import add_acl_filter_args, acl_filter_from_args


class DeleteACLs:
    COMMAND = 'delete'
    HELP = 'Delete Kafka ACLs'

    @classmethod
    def add_arguments(cls, parser):
        add_acl_filter_args(parser)

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
