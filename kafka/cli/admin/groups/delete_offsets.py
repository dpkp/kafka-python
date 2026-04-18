from kafka.structs import TopicPartition


class DeleteGroupOffsets:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser(
            'delete-offsets',
            help='Delete committed offsets for a consumer group')
        parser.add_argument('-g', '--group-id', type=str, required=True)
        parser.add_argument(
            '-p', '--partition', type=str, action='append',
            dest='partitions', default=[], required=True,
            help='TOPIC:PARTITION pair (repeatable).')
        parser.add_argument(
            '--group-coordinator-id', type=int, default=None,
            help='Send directly to this broker id, skipping coordinator lookup')
        parser.set_defaults(command=cls.command)

    @classmethod
    def command(cls, client, args):
        partitions = []
        for spec in args.partitions:
            topic, partition = spec.rsplit(':', 1)
            partitions.append(TopicPartition(topic, int(partition)))
        result = client.delete_group_offsets(
            args.group_id, partitions,
            group_coordinator_id=args.group_coordinator_id)
        return {'%s:%d' % (tp.topic, tp.partition): err.__name__
                for tp, err in result.items()}
