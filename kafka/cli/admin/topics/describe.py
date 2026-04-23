class DescribeTopics:
    COMMAND = 'describe'
    HELP = 'Describe Kafka Topics'

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument('-t', '--topic', type=str, action='append', dest='topics')

    @classmethod
    def command(cls, client, args):
        return client.describe_topics(args.topics or None)
