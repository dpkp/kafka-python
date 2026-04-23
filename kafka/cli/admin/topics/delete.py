import uuid


class DeleteTopic:
    COMMAND = 'delete'
    HELP = 'Delete Kafka Topic'

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument('-t', '--topic', type=str, action='append', dest='topics', default=[], help='topic name')
        parser.add_argument('--id', type=str, action='append', dest='topic_ids', default=[], help='topic UUID')

    @classmethod
    def command(cls, client, args):
        if not args.topics and not args.topic_ids:
            raise ValueError('At least one topic or topic_id is required!')
        topic_ids = [uuid.UUID(topic_id) for topic_id in args.topic_ids]
        topic_names = args.topics
        return client.delete_topics(topic_names + topic_ids)
