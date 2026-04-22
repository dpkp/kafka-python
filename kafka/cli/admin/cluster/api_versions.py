from kafka.protocol.api_key import ApiKey


class GetApiVersions:

    @classmethod
    def add_subparser(cls, subparsers):
        parser = subparsers.add_parser('api-versions', help='Get Supported Api Versions')
        parser.add_argument('-k', '--api-key', type=str, action='append', dest='api_keys', default=None)
        parser.add_argument('--raw', action='store_true')
        parser.set_defaults(command=cls.command)

    @classmethod
    def command(cls, client, args):
        api_keys = set(ApiKey[k] for k in args.api_keys) if args.api_keys else set(ApiKey)
        api_versions = client.api_versions()
        return {(k.value if args.raw else k.name): v for k, v in api_versions.items()
                if k in api_keys}
