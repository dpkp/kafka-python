import pytest

from kafka.admin import (
    ACL, ACLOperation, ACLPermissionType, ResourcePattern,
    ResourceType, ACLResourcePatternType,
)
from kafka.errors import IllegalArgumentError


def test_different_acls_are_different():
    one = ACL(
        principal='User:A',
        host='*',
        operation=ACLOperation.ALL,
        permission_type=ACLPermissionType.ALLOW,
        resource_pattern=ResourcePattern(
            resource_type=ResourceType.TOPIC,
            resource_name='some-topic',
            pattern_type=ACLResourcePatternType.LITERAL
        )
    )

    two = ACL(
        principal='User:B',  # Different principal
        host='*',
        operation=ACLOperation.ALL,
        permission_type=ACLPermissionType.ALLOW,
        resource_pattern=ResourcePattern(
            resource_type=ResourceType.TOPIC,
            resource_name='some-topic',
            pattern_type=ACLResourcePatternType.LITERAL
        )
    )

    assert one != two
    assert hash(one) != hash(two)

def test_different_acls_are_different_with_glob_topics():
    one = ACL(
        principal='User:A',
        host='*',
        operation=ACLOperation.ALL,
        permission_type=ACLPermissionType.ALLOW,
        resource_pattern=ResourcePattern(
            resource_type=ResourceType.TOPIC,
            resource_name='*',
            pattern_type=ACLResourcePatternType.LITERAL
        )
    )

    two = ACL(
        principal='User:B',  # Different principal
        host='*',
        operation=ACLOperation.ALL,
        permission_type=ACLPermissionType.ALLOW,
        resource_pattern=ResourcePattern(
            resource_type=ResourceType.TOPIC,
            resource_name='*',
            pattern_type=ACLResourcePatternType.LITERAL
        )
    )

    assert one != two
    assert hash(one) != hash(two)

def test_same_acls_are_same():
    one = ACL(
        principal='User:A',
        host='*',
        operation=ACLOperation.ALL,
        permission_type=ACLPermissionType.ALLOW,
        resource_pattern=ResourcePattern(
            resource_type=ResourceType.TOPIC,
            resource_name='some-topic',
            pattern_type=ACLResourcePatternType.LITERAL
        )
    )

    two = ACL(
        principal='User:A',
        host='*',
        operation=ACLOperation.ALL,
        permission_type=ACLPermissionType.ALLOW,
        resource_pattern=ResourcePattern(
            resource_type=ResourceType.TOPIC,
            resource_name='some-topic',
            pattern_type=ACLResourcePatternType.LITERAL
        )
    )

    assert one == two
    assert hash(one) == hash(two)
    assert len(set((one, two))) == 1


def test_acl_resource():
    good_acl = ACL(
        "User:bar",
        "*",
        ACLOperation.ALL,
        ACLPermissionType.ALLOW,
        ResourcePattern(
            ResourceType.TOPIC,
            "foo",
            ACLResourcePatternType.LITERAL
        )
    )

    assert(good_acl.resource_pattern.resource_type == ResourceType.TOPIC)
    assert(good_acl.operation == ACLOperation.ALL)
    assert(good_acl.permission_type == ACLPermissionType.ALLOW)
    assert(good_acl.resource_pattern.pattern_type == ACLResourcePatternType.LITERAL)

    with pytest.raises(IllegalArgumentError):
        ACL(
            "User:bar",
            "*",
            ACLOperation.ANY,
            ACLPermissionType.ANY,
            ResourcePattern(
                ResourceType.TOPIC,
                "foo",
                ACLResourcePatternType.LITERAL
            )
        )
