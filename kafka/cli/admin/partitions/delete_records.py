from kafka.structs import TopicPartition


class DeleteRecords:
    COMMAND = 'delete-records'
    HELP = 'Delete records from partitions up to a given offset'

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument(
            '-r', '--record', type=str, action='append',
            dest='records', default=[], required=True,
            help='TOPIC:PARTITION:OFFSET triple (repeatable). '
                 'Use -1 as OFFSET to delete up to the current high-water mark.')
        parser.add_argument(
            '--timeout-ms', type=int, default=None,
            help='Request timeout in milliseconds')
        parser.add_argument(
            '--partition-leader-id', type=int, default=None,
            help='Send all delete requests to this broker id, skipping metadata lookup')

    @classmethod
    def command(cls, client, args):
        records_to_delete = {}
        for spec in args.records:
            topic, partition, offset = spec.rsplit(':', 2)
            records_to_delete[TopicPartition(topic, int(partition))] = int(offset)
        return client.delete_records(
            records_to_delete,
            timeout_ms=args.timeout_ms,
            partition_leader_id=args.partition_leader_id)
