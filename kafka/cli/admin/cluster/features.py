from kafka.admin import UpdateFeatureType


class DescribeFeatures:
    COMMAND = 'describe-features'
    HELP = 'Describe Features of Kafka Cluster'

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument('-f', '--feature', type=str, action='append', dest='features', default=[],
                            help='Show one or more specific features. If not provided, returns all features.')

    @classmethod
    def command(cls, client, args):
        result = client.describe_features()
        if args.features:
            return {k: v for k, v in result.items() if k in args.features}
        else:
            return result


class UpdateFeatures:
    COMMAND = 'update-features'
    HELP = 'Update Features of Kafka Cluster'

    @classmethod
    def add_arguments(cls, parser):
        parser.add_argument('-f', '--feature', type=str, action='append', dest='features', default=[], help='set feature=value')
        parser.add_argument('--downgrade', action='store_true')
        parser.add_argument('--unsafe', action='store_true')
        parser.add_argument('--timeout', type=int, default=60)
        parser.add_argument('--validate-only', action='store_true')

    @staticmethod
    def _feature_type(args):
        if not args.downgrade:
            return UpdateFeatureType.UPGRADE
        elif args.unsafe:
            return UpdateFeatureType.UNSAFE_DOWNGRADE
        else:
            return UpdateFeatureType.SAFE_DOWNGRADE

    @classmethod
    def command(cls, client, args):
        feature_type = cls._feature_type(args)
        feature_updates = {
            feature_name: (feature_type, version)
            for feature_name, version in [feature.split('=') for feature in args.features]
        }
        return client.update_features(feature_updates,
                                      validate_only=args.validate_only,
                                      timeout_ms=1000*args.timeout)
