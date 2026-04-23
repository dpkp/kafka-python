from kafka.structs import TopicPartitionReplica


class DescribeLogDirs:
    COMMAND = 'describe-log-dirs'
    HELP = 'Get topic log directories and stats'

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument('--broker', type=int, action='append', dest='brokers', help='Query specific broker(s)')
        parser.add_argument('--topic', type=str, action='append', dest='topics', help='Get data about specific topic(s)')

    @classmethod
    def command(cls, client, args):
        return client.describe_log_dirs(topic_partitions=args.topics, brokers=args.brokers)


class AlterLogDirs:
    COMMAND = 'alter-log-dirs'
    HELP = 'Move replicas between log directories on their hosting brokers'

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument(
            '-a', '--assignment', type=str, action='append',
            dest='assignments', default=[], required=True,
            help='TOPIC:PARTITION:BROKER_ID=/absolute/log/dir/path (repeatable). '
                 'Instructs BROKER_ID to move its replica of TOPIC:PARTITION '
                 'into the given log directory.')

    @classmethod
    def command(cls, client, args):
        assignments = {}
        for spec in args.assignments:
            tpr_str, log_dir = spec.rsplit('=', 1)
            topic, partition, broker_id = tpr_str.rsplit(':', 2)
            tpr = TopicPartitionReplica(topic, int(partition), int(broker_id))
            assignments[tpr] = log_dir
        result = client.alter_replica_log_dirs(assignments)
        return {
            f'{tpr.topic}:{tpr.partition}:{tpr.broker_id}': err.__name__
            for tpr, err in result.items()
        }
