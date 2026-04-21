import pytest

from kafka.admin import ConfigResource, ConfigResourceType, KafkaAdminClient
from kafka.protocol.admin import (
    AlterConfigsRequest, AlterConfigsResponse,
    DescribeConfigsRequest, DescribeConfigsResponse,
)

from test.mock_broker import MockBroker


# ConfigResourceType values (wire)
_TOPIC = ConfigResourceType.TOPIC.value

# ConfigSourceType values (wire)
_SRC_DYNAMIC_TOPIC = 1
_SRC_DEFAULT = 5


def test_config_resource():
    with pytest.raises(KeyError):
        _bad_resource = ConfigResource('something', 'foo')
    good_resource = ConfigResource('broker', 'bar')
    assert good_resource.resource_type == ConfigResourceType.BROKER
    assert good_resource.name == 'bar'
    assert good_resource.configs is None
    good_resource = ConfigResource(ConfigResourceType.TOPIC, 'baz', {'frob': 'nob'})
    assert good_resource.resource_type == ConfigResourceType.TOPIC
    assert good_resource.name == 'baz'
    assert good_resource.configs == {'frob': 'nob'}


# ---------------------------------------------------------------------------
# MockBroker helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_broker():
    return MockBroker()

@pytest.fixture
def admin(mock_broker):
    admin = KafkaAdminClient(
        kafka_client=mock_broker.client_factory(),
        bootstrap_servers='%s:%d' % (mock_broker.host, mock_broker.port),
        api_version=mock_broker.broker_version,
        request_timeout_ms=5000,
    )
    try:
        yield admin
    finally:
        admin.close()


def _describe_configs_response(resource_type, resource_name, configs):
    """configs: iterable of (name, value, source, read_only)."""
    Result = DescribeConfigsResponse.DescribeConfigsResult
    Config = Result.DescribeConfigsResourceResult
    return DescribeConfigsResponse(
        throttle_time_ms=0,
        results=[
            Result(
                error_code=0,
                error_message=None,
                resource_type=resource_type,
                resource_name=resource_name,
                configs=[
                    Config(
                        name=name,
                        value=value,
                        read_only=read_only,
                        config_source=source,
                        is_default=(source == _SRC_DEFAULT),
                        is_sensitive=False,
                        synonyms=[],
                        config_type=2,  # STRING
                        documentation='',
                    ) for name, value, source, read_only in configs
                ],
            ),
        ],
    )


def _alter_configs_response(resource_type, resource_name, error_code=0, error_message=None):
    Response = AlterConfigsResponse.AlterConfigsResourceResponse
    return AlterConfigsResponse(
        throttle_time_ms=0,
        responses=[
            Response(
                error_code=error_code,
                error_message=error_message,
                resource_type=resource_type,
                resource_name=resource_name,
            ),
        ],
    )


def _capture_alter(captured):
    """Return a respond_fn that records the decoded AlterConfigsRequest."""
    def handler(api_key, api_version, correlation_id, request_bytes):
        captured['request'] = AlterConfigsRequest.decode(
            request_bytes, version=api_version, header=True)
        captured['version'] = api_version
        return _alter_configs_response(_TOPIC, 'topic-a')
    return handler


def _sent_configs(captured):
    """Return the (name, value) pairs sent in the captured AlterConfigsRequest."""
    assert len(captured['request'].resources) == 1
    resource = captured['request'].resources[0]
    return {c.name: c.value for c in resource.configs}


# ---------------------------------------------------------------------------
# alter_configs
# ---------------------------------------------------------------------------


class TestAlterConfigsMockBroker:
    def test_fills_in_other_modified_keys(self, mock_broker, admin):
        """User asks to set foo; bar is already modified; both end up on the wire."""
        # validation describe (dynamic filter)
        mock_broker.respond(DescribeConfigsRequest, _describe_configs_response(
            _TOPIC, 'topic-a', [
                ('foo', 'old',    _SRC_DYNAMIC_TOPIC, False),
                ('bar', 'barval', _SRC_DYNAMIC_TOPIC, False),
                ('baz', None,     _SRC_DEFAULT,      False),
            ]))
        # add_missing describe (modified filter) — same wire response, Python filters
        mock_broker.respond(DescribeConfigsRequest, _describe_configs_response(
            _TOPIC, 'topic-a', [
                ('foo', 'old',    _SRC_DYNAMIC_TOPIC, False),
                ('bar', 'barval', _SRC_DYNAMIC_TOPIC, False),
                ('baz', None,     _SRC_DEFAULT,      False),
            ]))
        captured = {}
        mock_broker.respond_fn(AlterConfigsRequest, _capture_alter(captured))

        admin.alter_configs([ConfigResource('TOPIC', 'topic-a', {'foo': 'new'})])

        sent = _sent_configs(captured)
        assert sent == {'foo': 'new', 'bar': 'barval'}

    def test_user_value_wins_over_describe(self, mock_broker, admin):
        for _ in range(2):  # validation + add_missing
            mock_broker.respond(DescribeConfigsRequest, _describe_configs_response(
                _TOPIC, 'topic-a', [
                    ('foo', 'brokerval', _SRC_DYNAMIC_TOPIC, False),
                ]))
        captured = {}
        mock_broker.respond_fn(AlterConfigsRequest, _capture_alter(captured))

        admin.alter_configs([ConfigResource('TOPIC', 'topic-a', {'foo': 'userval'})])

        assert _sent_configs(captured) == {'foo': 'userval'}

    def test_none_value_from_describe_is_skipped(self, mock_broker, admin):
        for _ in range(2):
            mock_broker.respond(DescribeConfigsRequest, _describe_configs_response(
                _TOPIC, 'topic-a', [
                    ('foo', 'userval-placeholder', _SRC_DYNAMIC_TOPIC, False),
                    ('bar', None,                   _SRC_DYNAMIC_TOPIC, False),
                ]))
        captured = {}
        mock_broker.respond_fn(AlterConfigsRequest, _capture_alter(captured))

        admin.alter_configs([ConfigResource('TOPIC', 'topic-a', {'foo': 'userval'})])

        sent = _sent_configs(captured)
        assert sent == {'foo': 'userval'}
        assert 'bar' not in sent

    def test_raise_on_unknown_true_raises(self, mock_broker, admin):
        mock_broker.respond(DescribeConfigsRequest, _describe_configs_response(
            _TOPIC, 'topic-a', [
                ('foo', 'val', _SRC_DYNAMIC_TOPIC, False),
            ]))

        with pytest.raises(ValueError, match='Unrecognized configs'):
            admin.alter_configs(
                [ConfigResource('TOPIC', 'topic-a', {'mystery': 'x'})])

    def test_raise_on_unknown_false_submits_anyway(self, mock_broker, admin):
        # only add_missing describe (validation is skipped)
        mock_broker.respond(DescribeConfigsRequest, _describe_configs_response(
            _TOPIC, 'topic-a', [
                ('foo', 'val', _SRC_DYNAMIC_TOPIC, False),
            ]))
        captured = {}
        mock_broker.respond_fn(AlterConfigsRequest, _capture_alter(captured))

        admin.alter_configs(
            [ConfigResource('TOPIC', 'topic-a', {'mystery': 'x'})],
            raise_on_unknown=False)

        sent = _sent_configs(captured)
        assert sent['mystery'] == 'x'
        assert sent['foo'] == 'val'

    def test_validate_only_propagates(self, mock_broker, admin):
        for _ in range(2):
            mock_broker.respond(DescribeConfigsRequest, _describe_configs_response(
                _TOPIC, 'topic-a', [
                    ('foo', 'val', _SRC_DYNAMIC_TOPIC, False),
                ]))
        captured = {}
        mock_broker.respond_fn(AlterConfigsRequest, _capture_alter(captured))

        admin.alter_configs(
            [ConfigResource('TOPIC', 'topic-a', {'foo': 'new'})],
            validate_only=True)

        assert captured['request'].validate_only is True


# ---------------------------------------------------------------------------
# reset_configs
# ---------------------------------------------------------------------------


class TestResetConfigsMockBroker:
    def test_full_reset_sends_empty_configs(self, mock_broker, admin):
        """Resource with no configs => submit empty configs list (full reset)."""
        captured = {}
        mock_broker.respond_fn(AlterConfigsRequest, _capture_alter(captured))

        # configs=[] (empty iterable) opts out of validation + get_missing describe
        admin.reset_configs(
            [ConfigResource('TOPIC', 'topic-a', [])],
            raise_on_unknown=False)

        assert captured['request'].resources[0].configs == []

    def test_partial_reset_excludes_user_keys_keeps_others(self, mock_broker, admin):
        """reset_configs({foo}) => submit all OTHER modified keys so only foo resets."""
        # validation describe (dynamic filter)
        mock_broker.respond(DescribeConfigsRequest, _describe_configs_response(
            _TOPIC, 'topic-a', [
                ('foo', 'fooval', _SRC_DYNAMIC_TOPIC, False),
                ('bar', 'barval', _SRC_DYNAMIC_TOPIC, False),
                ('baz', 'bazval', _SRC_DYNAMIC_TOPIC, False),
            ]))
        # get_missing describe (modified filter)
        mock_broker.respond(DescribeConfigsRequest, _describe_configs_response(
            _TOPIC, 'topic-a', [
                ('foo', 'fooval', _SRC_DYNAMIC_TOPIC, False),
                ('bar', 'barval', _SRC_DYNAMIC_TOPIC, False),
                ('baz', 'bazval', _SRC_DYNAMIC_TOPIC, False),
            ]))
        captured = {}
        mock_broker.respond_fn(AlterConfigsRequest, _capture_alter(captured))

        admin.reset_configs([ConfigResource('TOPIC', 'topic-a', ['foo'])])

        sent = _sent_configs(captured)
        assert 'foo' not in sent
        assert sent == {'bar': 'barval', 'baz': 'bazval'}

    def test_reset_all_modified_keys_sends_empty(self, mock_broker, admin):
        """If user resets every currently-modified key, the wire body is empty."""
        for _ in range(2):
            mock_broker.respond(DescribeConfigsRequest, _describe_configs_response(
                _TOPIC, 'topic-a', [
                    ('foo', 'fooval', _SRC_DYNAMIC_TOPIC, False),
                    ('bar', 'barval', _SRC_DYNAMIC_TOPIC, False),
                ]))
        captured = {}
        mock_broker.respond_fn(AlterConfigsRequest, _capture_alter(captured))

        admin.reset_configs([ConfigResource('TOPIC', 'topic-a', ['foo', 'bar'])])

        assert _sent_configs(captured) == {}

    def test_validate_only_propagates(self, mock_broker, admin):
        for _ in range(2):
            mock_broker.respond(DescribeConfigsRequest, _describe_configs_response(
                _TOPIC, 'topic-a', [
                    ('foo', 'fooval', _SRC_DYNAMIC_TOPIC, False),
                    ('bar', 'barval', _SRC_DYNAMIC_TOPIC, False),
                ]))
        captured = {}
        mock_broker.respond_fn(AlterConfigsRequest, _capture_alter(captured))

        admin.reset_configs(
            [ConfigResource('TOPIC', 'topic-a', ['foo'])],
            validate_only=True)

        assert captured['request'].validate_only is True
