class ListTransactions:
    COMMAND = 'list'
    HELP = 'List transactions across all brokers (broker >= 3.0)'

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument(
            '--broker-id', type=int, action='append', dest='broker_ids',
            help='Query only these brokers (repeatable). Default: all brokers.')
        parser.add_argument(
            '--producer-id', type=int, action='append', dest='producer_id_filters',
            help='Only list transactions for these producer ids (repeatable).')
        parser.add_argument(
            '--state', type=str, action='append', dest='state_filters',
            help='Only list transactions in these states (repeatable). '
                 'Valid values: Empty, Ongoing, PrepareCommit, PrepareAbort, '
                 'CompleteCommit, CompleteAbort, Dead, PrepareEpochFence.')
        parser.add_argument(
            '--duration-filter-ms', type=int, default=None,
            help='Only list transactions running longer than this. '
                 'Requires broker >= 3.8 (ListTransactions v1+).')
        parser.add_argument(
            '--id-pattern', type=str, default=None, dest='transactional_id_pattern',
            help='Only list transactions whose id matches this regex. '
                 'Requires broker >= 4.1 (KIP-1152).')

    @classmethod
    def command(cls, client, args):
        results = client.list_transactions(
            broker_ids=args.broker_ids,
            producer_id_filters=args.producer_id_filters,
            state_filters=args.state_filters,
            duration_filter_ms=args.duration_filter_ms,
            transactional_id_pattern=args.transactional_id_pattern,
        )
        return {broker_id: [t._asdict() for t in listings]
                for broker_id, listings in results.items()}
