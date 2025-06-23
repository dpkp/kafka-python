from __future__ import absolute_import

from kafka.admin.new_topic import NewTopic


class CreateTopic:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('create', help='Create a Kafka Topic')
        parser.add_argument('-t', '--topic', type=str, required=True)
        parser.add_argument('--num-partitions', type=int, default=-1)
        parser.add_argument('--replication-factor', type=int, default=-1)
        parser.set_defaults(command=cls.command)

    @classmethod
    def command(cls, client, args):
        return client.create_topics([NewTopic(args.topic, args.num_partitions, args.replication_factor)])
