from kafka.structs import TopicPartition


class AlterPartitionReassignments:
    COMMAND = 'alter-reassignments'
    HELP = 'Alter replica assignments for partitions'

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument(
            '-r', '--reassign', type=str, action='append',
            dest='reassignments', default=[], required=True,
            help='TOPIC:PARTITION=BROKER_ID[,BROKER_ID...] to set a new '
                 'replica set, or TOPIC:PARTITION=cancel to cancel an '
                 'in-progress reassignment for that partition. Repeatable.')
        parser.add_argument(
            '--timeout-ms', type=int, default=None,
            help='Request timeout in milliseconds')

    @classmethod
    def command(cls, client, args):
        reassignments = {}
        for spec in args.reassignments:
            tp_str, replicas_str = spec.rsplit('=', 1)
            topic, partition = tp_str.rsplit(':', 1)
            tp = TopicPartition(topic, int(partition))
            if replicas_str.lower() == 'cancel':
                reassignments[tp] = None
            else:
                reassignments[tp] = [int(b) for b in replicas_str.split(',') if b]
        results = client.alter_partition_reassignments(
            reassignments,
            timeout_ms=args.timeout_ms)
        return {
            '%s:%d' % (tp.topic, tp.partition): (err.__name__ if err else None)
            for tp, err in results.items()
        }
