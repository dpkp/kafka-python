from kafka.structs import OffsetAndMetadata, TopicPartition


class AlterGroupOffsets:
    COMMAND = 'alter-offsets'
    HELP = 'Alter committed offsets for a consumer group'

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument('-g', '--group-id', type=str, required=True)
        parser.add_argument(
            '-o', '--offset', type=str, action='append',
            dest='offsets', default=[], required=True,
            help='TOPIC:PARTITION:OFFSET triple (repeatable).')
        parser.add_argument(
            '--group-coordinator-id', type=int, default=None,
            help='Send directly to this broker id, skipping coordinator lookup')

    @classmethod
    def command(cls, client, args):
        offsets = {}
        for spec in args.offsets:
            topic, partition, offset = spec.rsplit(':', 2)
            offsets[TopicPartition(topic, int(partition))] = \
                OffsetAndMetadata(int(offset), '', None)
        result = client.alter_group_offsets(
            args.group_id, offsets,
            group_coordinator_id=args.group_coordinator_id)
        return {'%s:%d' % (tp.topic, tp.partition): err.__name__
                for tp, err in result.items()}
