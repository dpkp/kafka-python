class DescribeTransactions:
    COMMAND = 'describe'
    HELP = 'Describe one or more transactional ids (broker >= 3.0)'

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument(
            '--transactional-id', type=str, action='append',
            dest='transactional_ids', required=True,
            help='Transactional id to describe (repeatable).')

    @classmethod
    def command(cls, client, args):
        results = client.describe_transactions(args.transactional_ids)
        output = {}
        for txn_id, desc in results.items():
            out = desc._asdict()
            out['state'] = desc.state.value
            out['topic_partitions'] = sorted(
                [{'topic': tp.topic, 'partition': tp.partition}
                 for tp in desc.topic_partitions],
                key=lambda p: (p['topic'], p['partition']))
            output[txn_id] = out
        return output
