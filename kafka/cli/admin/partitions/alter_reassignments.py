from kafka.structs import TopicPartition


class AlterPartitionReassignments:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser(
            'alter-reassignments',
            help='Alter replica assignments for partitions')
        parser.add_argument(
            '-r', '--reassign', type=str, action='append',
            dest='reassignments', default=[], required=True,
            help='TOPIC:PARTITION=BROKER_ID[,BROKER_ID...] to set a new '
                 'replica set, or TOPIC:PARTITION=cancel to cancel an '
                 'in-progress reassignment for that partition. Repeatable.')
        parser.add_argument(
            '--timeout-ms', type=int, default=None,
            help='Request timeout in milliseconds')
        parser.add_argument(
            '--no-raise-errors', dest='raise_errors', action='store_false',
            help='Do not raise on partition-level errors; return the response instead')
        parser.set_defaults(command=cls.command, raise_errors=True)

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
        return client.alter_partition_reassignments(
            reassignments,
            timeout_ms=args.timeout_ms,
            raise_errors=args.raise_errors)
