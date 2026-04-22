class GetBrokerVersion:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('broker-version', help='Get Version for Broker')
        parser.add_argument('-b', '--broker', required=True)
        parser.set_defaults(command=cls.command)

    @classmethod
    def command(cls, client, args):
        broker_id = int(args.broker)
        bvd = client.get_broker_version_data(broker_id)
        return {broker_id: '.'.join(map(str, bvd.broker_version))}
