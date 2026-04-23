from kafka.protocol.api_key import ApiKey


class GetApiVersions:
    COMMAND = 'api-versions'
    HELP = 'Get Supported Api Versions'

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument('-k', '--api-key', type=str, action='append', dest='api_keys', default=None)
        parser.add_argument('--raw', action='store_true')

    @classmethod
    def command(cls, client, args):
        api_keys = set(ApiKey[k] for k in args.api_keys) if args.api_keys else set(ApiKey)
        api_versions = client.api_versions()
        return {(k.value if args.raw else k.name): v for k, v in api_versions.items()
                if k in api_keys}
