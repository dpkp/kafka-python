import pytest

from kafka.admin import ConfigResource, ConfigResourceType


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
