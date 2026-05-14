import pytest

from kafka.admin import (
    AlterConfigOp, ConfigResource, ConfigResourceType)
from kafka.errors import ClusterAuthorizationFailedError, InvalidConfigurationError
from kafka.protocol.admin import (
    AlterConfigsRequest, AlterConfigsResponse,
    DescribeConfigsRequest, DescribeConfigsResponse,
    IncrementalAlterConfigsRequest, IncrementalAlterConfigsResponse,
    ListConfigResourcesRequest, ListConfigResourcesResponse,
)

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


def test_admin_client_context_manager_closes_on_exit(broker):
    from kafka.admin import KafkaAdminClient
    with KafkaAdminClient(
        kafka_client=broker.client_factory(),
        bootstrap_servers='%s:%d' % (broker.host, broker.port),
        request_timeout_ms=5000,
    ) as admin:
        assert admin._closed is False
    assert admin._closed is True


# ---------------------------------------------------------------------------
# MockBroker helpers
# ---------------------------------------------------------------------------


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
    def test_fills_in_other_modified_keys(self, broker, admin):
        """User asks to set foo; bar is already modified; both end up on the wire."""
        # validation describe (dynamic filter)
        broker.respond(DescribeConfigsRequest, _describe_configs_response(
            _TOPIC, 'topic-a', [
                ('foo', 'old',    _SRC_DYNAMIC_TOPIC, False),
                ('bar', 'barval', _SRC_DYNAMIC_TOPIC, False),
                ('baz', None,     _SRC_DEFAULT,      False),
            ]))
        # add_missing describe (modified filter) — same wire response, Python filters
        broker.respond(DescribeConfigsRequest, _describe_configs_response(
            _TOPIC, 'topic-a', [
                ('foo', 'old',    _SRC_DYNAMIC_TOPIC, False),
                ('bar', 'barval', _SRC_DYNAMIC_TOPIC, False),
                ('baz', None,     _SRC_DEFAULT,      False),
            ]))
        captured = {}
        broker.respond_fn(AlterConfigsRequest, _capture_alter(captured))

        admin.alter_configs([ConfigResource('TOPIC', 'topic-a', {'foo': 'new'})], incremental=False)

        sent = _sent_configs(captured)
        assert sent == {'foo': 'new', 'bar': 'barval'}

    def test_user_value_wins_over_describe(self, broker, admin):
        for _ in range(2):  # validation + add_missing
            broker.respond(DescribeConfigsRequest, _describe_configs_response(
                _TOPIC, 'topic-a', [
                    ('foo', 'brokerval', _SRC_DYNAMIC_TOPIC, False),
                ]))
        captured = {}
        broker.respond_fn(AlterConfigsRequest, _capture_alter(captured))

        admin.alter_configs([ConfigResource('TOPIC', 'topic-a', {'foo': 'userval'})], incremental=False)

        assert _sent_configs(captured) == {'foo': 'userval'}

    def test_none_value_from_describe_is_skipped(self, broker, admin):
        for _ in range(2):
            broker.respond(DescribeConfigsRequest, _describe_configs_response(
                _TOPIC, 'topic-a', [
                    ('foo', 'userval-placeholder', _SRC_DYNAMIC_TOPIC, False),
                    ('bar', None,                   _SRC_DYNAMIC_TOPIC, False),
                ]))
        captured = {}
        broker.respond_fn(AlterConfigsRequest, _capture_alter(captured))

        admin.alter_configs([ConfigResource('TOPIC', 'topic-a', {'foo': 'userval'})], incremental=False)

        sent = _sent_configs(captured)
        assert sent == {'foo': 'userval'}
        assert 'bar' not in sent

    def test_raise_on_unknown_true_raises(self, broker, admin):
        broker.respond(DescribeConfigsRequest, _describe_configs_response(
            _TOPIC, 'topic-a', [
                ('foo', 'val', _SRC_DYNAMIC_TOPIC, False),
            ]))

        with pytest.raises(ValueError, match='Unrecognized configs'):
            admin.alter_configs(
                [ConfigResource('TOPIC', 'topic-a', {'mystery': 'x'})],
                incremental=False)

    def test_raise_on_unknown_false_submits_anyway(self, broker, admin):
        # only add_missing describe (validation is skipped)
        broker.respond(DescribeConfigsRequest, _describe_configs_response(
            _TOPIC, 'topic-a', [
                ('foo', 'val', _SRC_DYNAMIC_TOPIC, False),
            ]))
        captured = {}
        broker.respond_fn(AlterConfigsRequest, _capture_alter(captured))

        admin.alter_configs(
            [ConfigResource('TOPIC', 'topic-a', {'mystery': 'x'})],
            raise_on_unknown=False,
            incremental=False)

        sent = _sent_configs(captured)
        assert sent['mystery'] == 'x'
        assert sent['foo'] == 'val'

    def test_validate_only_propagates(self, broker, admin):
        for _ in range(2):
            broker.respond(DescribeConfigsRequest, _describe_configs_response(
                _TOPIC, 'topic-a', [
                    ('foo', 'val', _SRC_DYNAMIC_TOPIC, False),
                ]))
        captured = {}
        broker.respond_fn(AlterConfigsRequest, _capture_alter(captured))

        admin.alter_configs(
            [ConfigResource('TOPIC', 'topic-a', {'foo': 'new'})],
            validate_only=True,
            incremental=False)

        assert captured['request'].validate_only is True


# ---------------------------------------------------------------------------
# reset_configs
# ---------------------------------------------------------------------------


class TestResetConfigsMockBroker:
    def test_full_reset_sends_empty_configs(self, broker, admin):
        """Resource with no configs => submit empty configs list (full reset)."""
        captured = {}
        broker.respond_fn(AlterConfigsRequest, _capture_alter(captured))

        # configs=[] (empty iterable) opts out of validation + get_missing describe
        admin.reset_configs(
            [ConfigResource('TOPIC', 'topic-a', [])],
            raise_on_unknown=False,
            incremental=False)

        assert captured['request'].resources[0].configs == []

    def test_partial_reset_excludes_user_keys_keeps_others(self, broker, admin):
        """reset_configs({foo}) => submit all OTHER modified keys so only foo resets."""
        # validation describe (dynamic filter)
        broker.respond(DescribeConfigsRequest, _describe_configs_response(
            _TOPIC, 'topic-a', [
                ('foo', 'fooval', _SRC_DYNAMIC_TOPIC, False),
                ('bar', 'barval', _SRC_DYNAMIC_TOPIC, False),
                ('baz', 'bazval', _SRC_DYNAMIC_TOPIC, False),
            ]))
        # get_missing describe (modified filter)
        broker.respond(DescribeConfigsRequest, _describe_configs_response(
            _TOPIC, 'topic-a', [
                ('foo', 'fooval', _SRC_DYNAMIC_TOPIC, False),
                ('bar', 'barval', _SRC_DYNAMIC_TOPIC, False),
                ('baz', 'bazval', _SRC_DYNAMIC_TOPIC, False),
            ]))
        captured = {}
        broker.respond_fn(AlterConfigsRequest, _capture_alter(captured))

        admin.reset_configs([ConfigResource('TOPIC', 'topic-a', ['foo'])], incremental=False)

        sent = _sent_configs(captured)
        assert 'foo' not in sent
        assert sent == {'bar': 'barval', 'baz': 'bazval'}

    def test_reset_all_modified_keys_sends_empty(self, broker, admin):
        """If user resets every currently-modified key, the wire body is empty."""
        for _ in range(2):
            broker.respond(DescribeConfigsRequest, _describe_configs_response(
                _TOPIC, 'topic-a', [
                    ('foo', 'fooval', _SRC_DYNAMIC_TOPIC, False),
                    ('bar', 'barval', _SRC_DYNAMIC_TOPIC, False),
                ]))
        captured = {}
        broker.respond_fn(AlterConfigsRequest, _capture_alter(captured))

        admin.reset_configs([ConfigResource('TOPIC', 'topic-a', ['foo', 'bar'])], incremental=False)

        assert _sent_configs(captured) == {}

    def test_validate_only_propagates(self, broker, admin):
        for _ in range(2):
            broker.respond(DescribeConfigsRequest, _describe_configs_response(
                _TOPIC, 'topic-a', [
                    ('foo', 'fooval', _SRC_DYNAMIC_TOPIC, False),
                    ('bar', 'barval', _SRC_DYNAMIC_TOPIC, False),
                ]))
        captured = {}
        broker.respond_fn(AlterConfigsRequest, _capture_alter(captured))

        admin.reset_configs(
            [ConfigResource('TOPIC', 'topic-a', ['foo'])],
            validate_only=True,
            incremental=False)

        assert captured['request'].validate_only is True


# ---------------------------------------------------------------------------
# list_config_resources
# ---------------------------------------------------------------------------


def _list_config_resources_response(resources, error_code=0):
    """resources: iterable of (resource_name, resource_type)."""
    Resource = ListConfigResourcesResponse.ConfigResource
    return ListConfigResourcesResponse(
        throttle_time_ms=0,
        error_code=error_code,
        config_resources=[
            Resource(resource_name=name, resource_type=rtype)
            for name, rtype in resources
        ],
    )


def _capture_list_config_resources(captured, response):
    def handler(api_key, api_version, correlation_id, request_bytes):
        captured['request'] = ListConfigResourcesRequest.decode(
            request_bytes, version=api_version, header=True)
        captured['version'] = api_version
        return response
    return handler


class TestListConfigResourcesMockBroker:

    def test_groups_results_by_resource_type(self, broker, admin):
        broker.respond(
            ListConfigResourcesRequest,
            _list_config_resources_response([
                ('topic-a', ConfigResourceType.TOPIC.value),
                ('topic-b', ConfigResourceType.TOPIC.value),
                ('mygroup', ConfigResourceType.GROUP.value),
                ('metrics-1', ConfigResourceType.CLIENT_METRICS.value),
            ]),
        )
        result = admin.list_config_resources()
        assert result == {
            'topic': ['topic-a', 'topic-b'],
            'group': ['mygroup'],
            'client_metrics': ['metrics-1'],
        }

    def test_no_resource_types_sends_empty_filter(self, broker, admin):
        captured = {}
        broker.respond_fn(
            ListConfigResourcesRequest,
            _capture_list_config_resources(
                captured, _list_config_resources_response([])))

        admin.list_config_resources()
        assert captured['request'].resource_types == []

    def test_string_filter_is_normalized_and_sent_as_int8(self, broker, admin):
        captured = {}
        broker.respond_fn(
            ListConfigResourcesRequest,
            _capture_list_config_resources(
                captured, _list_config_resources_response([])))

        admin.list_config_resources(
            resource_types=['topic', 'Client-Metrics'])
        assert set(captured['request'].resource_types) == {
            ConfigResourceType.TOPIC.value,
            ConfigResourceType.CLIENT_METRICS.value,
        }

    def test_enum_filter_accepted(self, broker, admin):
        captured = {}
        broker.respond_fn(
            ListConfigResourcesRequest,
            _capture_list_config_resources(
                captured, _list_config_resources_response([])))

        admin.list_config_resources(
            resource_types=[ConfigResourceType.GROUP])
        assert captured['request'].resource_types == [
            ConfigResourceType.GROUP.value]

    def test_unrecognized_type_raises(self, admin):
        with pytest.raises(ValueError, match='Unrecognized ConfigResourceType'):
            admin.list_config_resources(resource_types=['bogus'])

    def test_error_code_raises(self, broker, admin):
        broker.respond(
            ListConfigResourcesRequest,
            _list_config_resources_response(
                [], error_code=ClusterAuthorizationFailedError.errno))
        with pytest.raises(ClusterAuthorizationFailedError):
            admin.list_config_resources()

    def test_empty_response_returns_empty_dict(self, broker, admin):
        broker.respond(
            ListConfigResourcesRequest,
            _list_config_resources_response([]))
        assert admin.list_config_resources() == {}


# ---------------------------------------------------------------------------
# incremental_alter_configs
# ---------------------------------------------------------------------------


def _incremental_alter_configs_response(responses):
    """responses: iterable of (resource_type, resource_name, error_code, error_message)."""
    Response = IncrementalAlterConfigsResponse.AlterConfigsResourceResponse
    return IncrementalAlterConfigsResponse(
        throttle_time_ms=0,
        responses=[
            Response(
                error_code=error_code,
                error_message=error_message,
                resource_type=rtype,
                resource_name=rname,
            ) for rtype, rname, error_code, error_message in responses
        ],
    )


def _capture_incremental_alter(captured, response=None):
    def handler(api_key, api_version, correlation_id, request_bytes):
        captured['request'] = IncrementalAlterConfigsRequest.decode(
            request_bytes, version=api_version, header=True)
        captured['version'] = api_version
        return response if response is not None else _incremental_alter_configs_response([
            (_TOPIC, 'topic-a', 0, None),
        ])
    return handler


def _sent_incremental(captured):
    """Return a dict {name: (op, value)} from the captured IncrementalAlterConfigsRequest."""
    assert len(captured['request'].resources) == 1
    resource = captured['request'].resources[0]
    return {c.name: (c.config_operation, c.value) for c in resource.configs}


class TestIncrementalAlterConfigsMockBroker:

    def test_set_op_sends_triples(self, broker, admin):
        # validation describe (dynamic filter)
        broker.respond(DescribeConfigsRequest, _describe_configs_response(
            _TOPIC, 'topic-a', [
                ('foo', 'old', _SRC_DYNAMIC_TOPIC, False),
            ]))
        captured = {}
        broker.respond_fn(IncrementalAlterConfigsRequest, _capture_incremental_alter(captured))

        result = admin.alter_configs(
            [ConfigResource('TOPIC', 'topic-a',
                            {'foo': (AlterConfigOp.SET, 'new')})],
            incremental=True)

        sent = _sent_incremental(captured)
        assert sent == {'foo': (AlterConfigOp.SET.value, 'new')}
        assert result == {'topic': {'topic-a': 'OK'}}

    def test_bare_value_is_treated_as_set(self, broker, admin):
        broker.respond(DescribeConfigsRequest, _describe_configs_response(
            _TOPIC, 'topic-a', [
                ('foo', 'old', _SRC_DYNAMIC_TOPIC, False),
            ]))
        captured = {}
        broker.respond_fn(IncrementalAlterConfigsRequest, _capture_incremental_alter(captured))

        admin.alter_configs(
            [ConfigResource('TOPIC', 'topic-a', {'foo': 'new'})],
            incremental=True)

        assert _sent_incremental(captured) == {'foo': (AlterConfigOp.SET.value, 'new')}

    def test_delete_op_forces_null_value(self, broker, admin):
        broker.respond(DescribeConfigsRequest, _describe_configs_response(
            _TOPIC, 'topic-a', [
                ('foo', 'old', _SRC_DYNAMIC_TOPIC, False),
            ]))
        captured = {}
        broker.respond_fn(IncrementalAlterConfigsRequest, _capture_incremental_alter(captured))

        admin.alter_configs(
            [ConfigResource('TOPIC', 'topic-a',
                            {'foo': (AlterConfigOp.DELETE, 'ignored')})],
            incremental=True)

        assert _sent_incremental(captured) == {'foo': (AlterConfigOp.DELETE.value, None)}

    def test_append_and_subtract_ops(self, broker, admin):
        broker.respond(DescribeConfigsRequest, _describe_configs_response(
            _TOPIC, 'topic-a', [
                ('follower.replication.throttled.replicas', '*', _SRC_DYNAMIC_TOPIC, False),
            ]))
        captured = {}
        broker.respond_fn(IncrementalAlterConfigsRequest, _capture_incremental_alter(captured))

        admin.alter_configs(
            [ConfigResource('TOPIC', 'topic-a', {
                'follower.replication.throttled.replicas':
                    (AlterConfigOp.APPEND, '1:0'),
            })],
            incremental=True)

        assert _sent_incremental(captured) == {
            'follower.replication.throttled.replicas':
                (AlterConfigOp.APPEND.value, '1:0'),
        }

        captured = {}
        broker.respond(DescribeConfigsRequest, _describe_configs_response(
            _TOPIC, 'topic-a', [
                ('follower.replication.throttled.replicas', '*', _SRC_DYNAMIC_TOPIC, False),
            ]))
        broker.respond_fn(IncrementalAlterConfigsRequest, _capture_incremental_alter(captured))

        admin.alter_configs(
            [ConfigResource('TOPIC', 'topic-a', {
                'follower.replication.throttled.replicas':
                    ('subtract', '1:0'),
            })],
            incremental=True)

        assert _sent_incremental(captured) == {
            'follower.replication.throttled.replicas':
                (AlterConfigOp.SUBTRACT.value, '1:0'),
        }

    def test_string_op_is_normalized(self, broker, admin):
        broker.respond(DescribeConfigsRequest, _describe_configs_response(
            _TOPIC, 'topic-a', [
                ('foo', 'val', _SRC_DYNAMIC_TOPIC, False),
            ]))
        captured = {}
        broker.respond_fn(IncrementalAlterConfigsRequest, _capture_incremental_alter(captured))

        admin.alter_configs(
            [ConfigResource('TOPIC', 'topic-a', {'foo': ('set', 'v')})],
            incremental=True)

        assert _sent_incremental(captured) == {'foo': (AlterConfigOp.SET.value, 'v')}

    def test_unrecognized_op_raises(self, admin):
        with pytest.raises(ValueError, match='Unrecognized AlterConfigOp'):
            admin.alter_configs(
                [ConfigResource('TOPIC', 'topic-a', {'foo': ('bogus', 'v')})],
                raise_on_unknown=False,
                incremental=True)

    def test_raise_on_unknown_true_raises(self, broker, admin):
        broker.respond(DescribeConfigsRequest, _describe_configs_response(
            _TOPIC, 'topic-a', [
                ('foo', 'val', _SRC_DYNAMIC_TOPIC, False),
            ]))

        with pytest.raises(ValueError, match='Unrecognized configs'):
            admin.alter_configs(
                [ConfigResource('TOPIC', 'topic-a',
                                {'mystery': (AlterConfigOp.SET, 'x')})],
                incremental=True)

    def test_raise_on_unknown_false_skips_describe(self, broker, admin):
        captured = {}
        broker.respond_fn(IncrementalAlterConfigsRequest, _capture_incremental_alter(captured))

        admin.alter_configs(
            [ConfigResource('TOPIC', 'topic-a',
                            {'mystery': (AlterConfigOp.SET, 'x')})],
            raise_on_unknown=False,
            incremental=True)

        assert _sent_incremental(captured) == {
            'mystery': (AlterConfigOp.SET.value, 'x'),
        }

    def test_validate_only_propagates(self, broker, admin):
        captured = {}
        broker.respond_fn(IncrementalAlterConfigsRequest, _capture_incremental_alter(captured))

        admin.alter_configs(
            [ConfigResource('TOPIC', 'topic-a',
                            {'foo': (AlterConfigOp.SET, 'v')})],
            validate_only=True,
            raise_on_unknown=False,
            incremental=True)

        assert captured['request'].validate_only is True

    def test_does_not_fill_in_other_keys(self, broker, admin):
        """Unlike alter_configs, incremental should NOT send untouched keys."""
        broker.respond(DescribeConfigsRequest, _describe_configs_response(
            _TOPIC, 'topic-a', [
                ('foo', 'val', _SRC_DYNAMIC_TOPIC, False),
                ('bar', 'barval', _SRC_DYNAMIC_TOPIC, False),
            ]))
        captured = {}
        broker.respond_fn(IncrementalAlterConfigsRequest, _capture_incremental_alter(captured))

        admin.alter_configs(
            [ConfigResource('TOPIC', 'topic-a',
                            {'foo': (AlterConfigOp.SET, 'new')})],
            incremental=True)

        assert _sent_incremental(captured) == {
            'foo': (AlterConfigOp.SET.value, 'new'),
        }

    def test_broker_resource_routed_by_broker_id(self, broker, admin):
        captured = {}
        broker.respond_fn(IncrementalAlterConfigsRequest, _capture_incremental_alter(
            captured,
            _incremental_alter_configs_response([
                (ConfigResourceType.BROKER.value, '0', 0, None),
            ]),
        ))

        result = admin.alter_configs(
            [ConfigResource('BROKER', '0',
                            {'max.connections': (AlterConfigOp.SET, '100')})],
            raise_on_unknown=False,
            incremental=True)

        assert len(captured['request'].resources) == 1
        assert captured['request'].resources[0].resource_type == ConfigResourceType.BROKER.value
        assert captured['request'].resources[0].resource_name == '0'
        assert result == {'broker': {'0': 'OK'}}

    def test_non_integer_broker_name_raises(self, admin):
        with pytest.raises(ValueError, match='Broker resource names must be an integer'):
            admin.alter_configs(
                [ConfigResource('BROKER', 'not-an-int',
                                {'foo': (AlterConfigOp.SET, 'v')})],
                raise_on_unknown=False,
                incremental=True)

    def test_error_response_surfaces_in_result(self, broker, admin):
        broker.respond(
            IncrementalAlterConfigsRequest,
            _incremental_alter_configs_response([
                (_TOPIC, 'topic-a', InvalidConfigurationError.errno, 'bad value'),
            ]))

        result = admin.alter_configs(
            [ConfigResource('TOPIC', 'topic-a',
                            {'foo': (AlterConfigOp.SET, 'v')})],
            raise_on_unknown=False,
            incremental=True)

        assert 'topic' in result
        assert 'bad value' in result['topic']['topic-a']
