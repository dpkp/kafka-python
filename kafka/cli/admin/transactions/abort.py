from kafka.admin import AbortTransactionSpec
from kafka.structs import TopicPartition


class AbortTransaction:
    COMMAND = 'abort'
    HELP = 'Administratively abort an open transaction on a partition'

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument(
            '-t', '--topic', type=str, required=True,
            help='Topic name.')
        parser.add_argument(
            '-p', '--partition', type=int, required=True,
            help='Partition index.')
        parser.add_argument(
            '--producer-id', type=int, required=True,
            help='Producer id of the hanging transaction.')
        parser.add_argument(
            '--producer-epoch', type=int, required=True,
            help='Producer epoch of the hanging transaction.')
        parser.add_argument(
            '--coordinator-epoch', type=int, default=-1,
            help='Coordinator epoch (default: -1, the admin sentinel).')

    @classmethod
    def command(cls, client, args):
        spec = AbortTransactionSpec(
            topic_partition=TopicPartition(args.topic, args.partition),
            producer_id=args.producer_id,
            producer_epoch=args.producer_epoch,
            coordinator_epoch=args.coordinator_epoch,
        )
        client.abort_transaction(spec)
        return {'aborted': {'topic': args.topic, 'partition': args.partition,
                            'producer_id': args.producer_id,
                            'producer_epoch': args.producer_epoch}}
