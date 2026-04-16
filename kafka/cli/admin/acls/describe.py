from .common import add_acl_filter_args, acl_filter_from_args


class DescribeACLs:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('describe', help='Describe Kafka ACLs')
        add_acl_filter_args(parser)
        parser.set_defaults(command=cls.command)

    @classmethod
    def command(cls, client, args):
        acl_filter = acl_filter_from_args(args)
        acls, error = client.describe_acls(acl_filter)
        return [repr(acl) for acl in acls]
