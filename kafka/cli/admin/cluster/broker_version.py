class GetBrokerVersion:
    COMMAND = 'broker-version'
    HELP = 'Get Version for Broker'

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument('--broker', required=True)

    @classmethod
    def command(cls, client, args):
        broker_id = int(args.broker)
        bvd = client.get_broker_version_data(broker_id)
        return {broker_id: '.'.join(map(str, bvd.broker_version))}
