import uuid


class DescribeTopics:
    COMMAND = 'describe'
    HELP = 'Describe Kafka Topics'

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument('-t', '--topic', type=str, action='append', dest='topics', default=[], help='topic name')
        parser.add_argument('--id', type=str, action='append', dest='topic_ids', default=[],
                            help='topic UUID (requires broker >= 2.8, KIP-516)')

    @classmethod
    def command(cls, client, args):
        topic_ids = [uuid.UUID(topic_id) for topic_id in args.topic_ids]
        topics = args.topics + topic_ids
        return client.describe_topics(topics or None)
