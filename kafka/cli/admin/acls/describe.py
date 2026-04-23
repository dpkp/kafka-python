from .common import add_acl_filter_args, acl_filter_from_args


class DescribeACLs:
    COMMAND = 'describe'
    HELP = 'Describe Kafka ACLs'

    @classmethod
    def add_arguments(cls, parser):
        add_acl_filter_args(parser)

    @classmethod
    def command(cls, client, args):
        acl_filter = acl_filter_from_args(args)
        acls, error = client.describe_acls(acl_filter)
        return [repr(acl) for acl in acls]
