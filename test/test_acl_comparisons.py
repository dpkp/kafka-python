from kafka.admin.acl_resource import ACL
from kafka.admin.acl_resource import ACLOperation
from kafka.admin.acl_resource import ACLPermissionType
from kafka.admin.acl_resource import ResourcePattern
from kafka.admin.acl_resource import ResourceType
from kafka.admin.acl_resource import ACLResourcePatternType


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
