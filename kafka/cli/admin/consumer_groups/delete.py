from __future__ import absolute_import


class DeleteConsumerGroups:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('delete', help='Delete Consumer Groups')
        parser.add_argument('-g', '--group-id', type=str, action='append', dest='groups', required=True)
        parser.set_defaults(command=lambda cli, args: cli.delete_consumer_groups(args.groups))
