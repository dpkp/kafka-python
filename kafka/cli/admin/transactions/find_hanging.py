class FindHangingTransactions:
    COMMAND = 'find-hanging'
    HELP = 'Find transactions older than the broker timeout + 5 min'

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument(
            '--broker-id', type=int, action='append', dest='broker_ids',
            help='Query only these brokers (repeatable). Default: all.')
        parser.add_argument(
            '--max-transaction-timeout-ms', type=int, default=900_000,
            help="Broker's max-transaction-timeout (ms). "
                 'Transactions older than this + 5 min are flagged as hanging. '
                 'Default: 900000 (Kafka default).')

    @classmethod
    def command(cls, client, args):
        results = client.find_hanging_transactions(
            broker_ids=args.broker_ids,
            max_transaction_timeout_ms=args.max_transaction_timeout_ms,
        )
        for row in results:
            row['topic_partitions'] = [
                {'topic': tp.topic, 'partition': tp.partition}
                for tp in row['topic_partitions']]
        return results
