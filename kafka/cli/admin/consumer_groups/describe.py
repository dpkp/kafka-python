from __future__ import absolute_import


class DescribeConsumerGroups:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('describe', help='Describe Consumer Groups')
        parser.add_argument('-g', '--group-id', type=str, action='append', dest='groups', required=True)
        parser.set_defaults(command=lambda cli, args: cli.describe_consumer_groups(args.groups))
