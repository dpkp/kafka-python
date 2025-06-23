from __future__ import absolute_import


class DescribeTopics:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('describe', help='Describe Kafka Topics')
        parser.add_argument('-t', '--topic', type=str, action='append', dest='topics')
        parser.set_defaults(command=lambda cli, args: cli.describe_topics(args.topics or None))
