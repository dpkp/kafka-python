import pytest

from kafka.admin import KafkaAdminClient, UpdateFeatureType
from kafka.errors import (
    ClusterAuthorizationFailedError,
    FeatureUpdateFailedError,
    InvalidUpdateVersionError,
)  # noqa: F401
from kafka.protocol.admin import UpdateFeaturesRequest, UpdateFeaturesResponse
from kafka.protocol.metadata import ApiVersionsRequest, ApiVersionsResponse

from test.mock_broker import MockBroker


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def broker(request):
    broker_version = getattr(request, 'param', (4, 2))
    return MockBroker(broker_version=broker_version)


@pytest.fixture
def admin(broker):
    admin = KafkaAdminClient(
        kafka_client=broker.client_factory(),
        bootstrap_servers='%s:%d' % (broker.host, broker.port),
        request_timeout_ms=5000,
    )
    try:
        yield admin
    finally:
        admin.close()


# ---------------------------------------------------------------------------
# describe_features
# ---------------------------------------------------------------------------


def _api_versions_response(
    *,
    error_code=0,
    supported=(),
    finalized=(),
    finalized_epoch=-1,
):
    """supported: iterable of (name, min, max). finalized: iterable of (name, min_level, max_level)."""
    Api = ApiVersionsResponse.ApiVersion
    SF = ApiVersionsResponse.SupportedFeatureKey
    FF = ApiVersionsResponse.FinalizedFeatureKey
    return ApiVersionsResponse(
        error_code=error_code,
        api_keys=[Api(api_key=ApiVersionsRequest.API_KEY, min_version=0, max_version=4)],
        throttle_time_ms=0,
        supported_features=[
            SF(name=n, min_version=mn, max_version=mx) for n, mn, mx in supported
        ],
        finalized_features_epoch=finalized_epoch,
        finalized_features=[
            FF(name=n, min_version_level=mn, max_version_level=mx) for n, mn, mx in finalized
        ],
        zk_migration_ready=False,
    )


class TestDescribeFeaturesMockBroker:

    def test_happy_path(self, broker, admin):
        # Set the response directly to avoid bootstrap consuming our queued responses
        broker._api_versions_response = _api_versions_response(
            supported=[
                ('metadata.version', 1, 19),
                ('kraft.version', 0, 1),
            ],
            finalized=[('metadata.version', 8, 8)],
            finalized_epoch=42,
        )

        result = admin.describe_features()

        assert result['supported_features'] == {
            'metadata.version': (1, 19),
            'kraft.version': (0, 1),
        }
        assert result['finalized_features'] == {'metadata.version': (8, 8)}
        assert result['finalized_features_epoch'] == 42

    def test_empty_features(self, broker, admin):
        broker.respond(ApiVersionsRequest, _api_versions_response())
        result = admin.describe_features()
        assert result == {
            'supported_features': {},
            'finalized_features': {},
            'finalized_features_epoch': None,
        }

    def test_negative_epoch_normalized_to_none(self, broker, admin):
        broker.respond(ApiVersionsRequest, _api_versions_response(
            supported=[('metadata.version', 1, 19)],
            finalized_epoch=-1,
        ))
        assert admin.describe_features()['finalized_features_epoch'] is None

    def test_sends_request_version_3_plus(self, broker, admin):
        captured = {}

        def handler(api_key, api_version, correlation_id, request_bytes):
            captured['version'] = api_version
            return _api_versions_response()

        broker.respond_fn(ApiVersionsRequest, handler)
        admin.describe_features()
        # Admin explicitly requests min_version=3, broker supports up to 4
        assert captured['version'] >= 3


# ---------------------------------------------------------------------------
# update_features
# ---------------------------------------------------------------------------


def _update_features_response(results=(), error_code=0, error_message=None):
    """results: iterable of (feature, error_code, error_message)."""
    Result = UpdateFeaturesResponse.UpdatableFeatureResult
    return UpdateFeaturesResponse(
        throttle_time_ms=0,
        error_code=error_code,
        error_message=error_message,
        results=[Result(feature=f, error_code=ec, error_message=em)
                 for f, ec, em in results],
    )


def _capture_update(captured, response=None):
    def handler(api_key, api_version, correlation_id, request_bytes):
        captured['request'] = UpdateFeaturesRequest.decode(
            request_bytes, version=api_version, header=True)
        captured['version'] = api_version
        return response if response is not None else _update_features_response([
            ('metadata.version', 0, None),
        ])
    return handler


def _sent_updates(captured):
    """Return {feature: FeatureUpdateKey} from the decoded request. At v2 the
    AllowDowngrade field is absent; only feature/max_version_level/upgrade_type
    are present."""
    return {u.feature: u for u in captured['request'].feature_updates}


class TestUpdateFeaturesMockBroker:

    def test_upgrade_implicit(self, broker, admin):
        """Bare int level means UPGRADE (type=1)."""
        captured = {}
        broker.respond_fn(UpdateFeaturesRequest, _capture_update(captured))

        result = admin.update_features({'metadata.version': 19})

        sent = _sent_updates(captured)
        assert set(sent) == {'metadata.version'}
        assert sent['metadata.version'].max_version_level == 19
        assert sent['metadata.version'].upgrade_type == UpdateFeatureType.UPGRADE.value
        assert result == {'metadata.version': 'OK'}

    def test_safe_downgrade_tuple(self, broker, admin):
        captured = {}
        broker.respond_fn(UpdateFeaturesRequest, _capture_update(captured))

        admin.update_features({'metadata.version': (UpdateFeatureType.SAFE_DOWNGRADE, 8)})

        u = _sent_updates(captured)['metadata.version']
        assert u.max_version_level == 8
        assert u.upgrade_type == UpdateFeatureType.SAFE_DOWNGRADE.value

    def test_unsafe_downgrade_string(self, broker, admin):
        captured = {}
        broker.respond_fn(UpdateFeaturesRequest, _capture_update(captured))

        admin.update_features({'metadata.version': ('unsafe-downgrade', 8)})

        u = _sent_updates(captured)['metadata.version']
        assert u.upgrade_type == UpdateFeatureType.UNSAFE_DOWNGRADE.value

    def test_upgrade_type_by_int(self, broker, admin):
        captured = {}
        broker.respond_fn(UpdateFeaturesRequest, _capture_update(captured))

        admin.update_features({'metadata.version': (2, 8)})  # 2 = SAFE_DOWNGRADE

        assert _sent_updates(captured)['metadata.version'].upgrade_type == 2

    def test_deletion_request(self, broker, admin):
        """max_version_level < 1 requests deletion of the finalized feature."""
        captured = {}
        broker.respond_fn(UpdateFeaturesRequest, _capture_update(captured,
            _update_features_response([('kraft.version', 0, None)])))

        admin.update_features({'kraft.version': ('safe-downgrade', 0)})

        assert _sent_updates(captured)['kraft.version'].max_version_level == 0

    def test_validate_only_propagates(self, broker, admin):
        captured = {}
        broker.respond_fn(UpdateFeaturesRequest, _capture_update(captured))

        admin.update_features({'metadata.version': 19}, validate_only=True)

        assert captured['request'].validate_only is True

    def test_timeout_ms_propagates(self, broker, admin):
        captured = {}
        broker.respond_fn(UpdateFeaturesRequest, _capture_update(captured))

        admin.update_features({'metadata.version': 19}, timeout_ms=12345)

        assert captured['request'].timeout_ms == 12345

    def test_multiple_features(self, broker, admin):
        captured = {}
        broker.respond_fn(UpdateFeaturesRequest, _capture_update(captured,
            _update_features_response([
                ('metadata.version', 0, None),
                ('transaction.version', 0, None),
            ])))

        result = admin.update_features({
            'metadata.version': 19,
            'transaction.version': (UpdateFeatureType.UPGRADE, 2),
        })

        sent = _sent_updates(captured)
        assert sent['metadata.version'].max_version_level == 19
        assert sent['metadata.version'].upgrade_type == UpdateFeatureType.UPGRADE.value
        assert sent['transaction.version'].max_version_level == 2
        assert sent['transaction.version'].upgrade_type == UpdateFeatureType.UPGRADE.value
        assert result == {'metadata.version': 'OK', 'transaction.version': 'OK'}

    @pytest.mark.parametrize('broker', [(3, 9, 0)], indirect=True)
    def test_per_feature_error_surfaces_in_result(self, broker, admin):
        broker.respond(UpdateFeaturesRequest, _update_features_response([
            ('metadata.version', InvalidUpdateVersionError.errno,
             'Unsafe metadata downgrade is not supported in this version.'),
        ]))

        result = admin.update_features({'metadata.version': ('UNSAFE_DOWNGRADE', 8)})

        assert 'metadata.version' in result
        assert 'InvalidUpdateVersion' in result['metadata.version']
        assert 'Unsafe metadata downgrade' in result['metadata.version']

    def test_top_level_error_raises(self, broker, admin):
        broker.respond(UpdateFeaturesRequest, _update_features_response(
            error_code=ClusterAuthorizationFailedError.errno,
            error_message='not authorized',
        ))

        with pytest.raises(ClusterAuthorizationFailedError):
            admin.update_features({'metadata.version': 19})

    def test_v2_empty_results_defaults_to_ok(self, broker, admin):
        """v2 UpdateFeaturesResponse has no per-feature results; client sets 'OK'
        for each requested feature when the top-level error code is NoError."""
        broker.respond(UpdateFeaturesRequest, _update_features_response(results=[]))

        result = admin.update_features({
            'metadata.version': 19,
            'transaction.version': 2,
        })

        assert result == {'metadata.version': 'OK', 'transaction.version': 'OK'}

    def test_non_dict_raises(self, admin):
        with pytest.raises(TypeError, match='feature_updates must be a dict'):
            admin.update_features([('metadata.version', 19)])

    def test_unrecognized_upgrade_type_string(self, admin):
        with pytest.raises(ValueError, match='Unrecognized UpdateFeatureType'):
            admin.update_features({'metadata.version': ('bogus', 19)})

    @pytest.mark.parametrize('broker', [(3, 9, 0)], indirect=True)
    def test_partial_error_reports_both(self, broker, admin):
        """Mix of ok + failed features surfaces independently in the result dict."""
        broker.respond(UpdateFeaturesRequest, _update_features_response([
            ('metadata.version', 0, None),
            ('transaction.version', FeatureUpdateFailedError.errno, 'dependency not met'),
        ]))

        result = admin.update_features({
            'metadata.version': 19,
            'transaction.version': 2,
        })

        assert result['metadata.version'] == 'OK'
        assert 'FeatureUpdateFailed' in result['transaction.version']
        assert 'dependency not met' in result['transaction.version']
