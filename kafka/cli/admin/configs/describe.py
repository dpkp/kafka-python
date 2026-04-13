from kafka.admin import ConfigResource


class DescribeConfigs:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('describe', help='Describe Kafka Configs')
        parser.add_argument('-t', '--topic', type=str, action='append', dest='topics', default=[])
        parser.add_argument('-b', '--broker', type=str, action='append', dest='brokers', default=[])
        parser.set_defaults(command=cls.command)

    @classmethod
    def command(cls, client, args):
        resources = []
        for topic in args.topics:
            resources.append(ConfigResource('TOPIC', topic))
        for broker in args.brokers:
            resources.append(ConfigResource('BROKER', broker))

        responses = client.describe_configs(resources)
        # Return shape is list of (resource, configs) tuples
        #  resource => (type, name)
        #  configs =>  {key: value}
        return [(
            (resources[i].resource_type.name, resources[i].name),
            {str(config.name): config.value for config in r.results[0].configs}
        ) for i, r in enumerate(responses)]
