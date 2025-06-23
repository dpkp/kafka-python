from __future__ import absolute_import


class DeleteTopic:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('delete', help='Delete Kafka Topic')
        parser.add_argument('-t', '--topic', type=str, required=True)
        parser.set_defaults(command=lambda cli, args: cli.delete_topics([args.topic]))
