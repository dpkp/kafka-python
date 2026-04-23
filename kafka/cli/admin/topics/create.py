class CreateTopic:
    COMMAND = 'create'
    HELP = 'Create a Kafka Topic'

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument('-t', '--topic', type=str, required=True)
        parser.add_argument('--num-partitions', type=int, default=-1)
        parser.add_argument('--replication-factor', type=int, default=-1)

    @classmethod
    def command(cls, client, args):
        return client.create_topics({args.topic: {'num_partitions': args.num_partitions, 'replication_factor': args.replication_factor}})
