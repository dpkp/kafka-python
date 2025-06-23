from __future__ import absolute_import


class ListTopics:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('list', help='List Kafka Topics')
        parser.set_defaults(command=lambda cli, _args: cli.list_topics())
