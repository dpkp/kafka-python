import pytest

from test.testutil import env_kafka_version

from kafka.errors import NoError
from kafka.admin import ACLFilter, ACLOperation, ACLPermissionType, ResourcePattern, ResourceType, ACL


@pytest.mark.skipif(env_kafka_version() < (0, 11), reason="ACL features require broker >=0.11")
def test_create_describe_delete_acls(kafka_admin_client):
    """Tests that we can add, list and remove ACLs
    """

    # Check that we don't have any ACLs in the cluster
    acls, error = kafka_admin_client.describe_acls(
        ACLFilter(
            principal=None,
            host="*",
            operation=ACLOperation.ANY,
            permission_type=ACLPermissionType.ANY,
            resource_pattern=ResourcePattern(ResourceType.TOPIC, "topic")
        )
    )

    assert error is NoError
    assert len(acls) == 0

    # Try to add an ACL
    acl = ACL(
        principal="User:test",
        host="*",
        operation=ACLOperation.READ,
        permission_type=ACLPermissionType.ALLOW,
        resource_pattern=ResourcePattern(ResourceType.TOPIC, "topic")
    )
    result = kafka_admin_client.create_acls([acl])

    assert len(result["failed"]) == 0
    assert len(result["succeeded"]) == 1

    # Check that we can list the ACL we created
    acl_filter = ACLFilter(
        principal=None,
        host="*",
        operation=ACLOperation.ANY,
        permission_type=ACLPermissionType.ANY,
        resource_pattern=ResourcePattern(ResourceType.TOPIC, "topic")
    )
    acls, error = kafka_admin_client.describe_acls(acl_filter)

    assert error is NoError
    assert len(acls) == 1

    # Remove the ACL
    delete_results = kafka_admin_client.delete_acls(
        [
            ACLFilter(
                principal="User:test",
                host="*",
                operation=ACLOperation.READ,
                permission_type=ACLPermissionType.ALLOW,
                resource_pattern=ResourcePattern(ResourceType.TOPIC, "topic")
            )
        ]
    )

    assert len(delete_results) == 1
    assert len(delete_results[0][1]) == 1  # Check number of affected ACLs

    # Make sure the ACL does not exist in the cluster anymore
    acls, error = kafka_admin_client.describe_acls(
        ACLFilter(
            principal="*",
            host="*",
            operation=ACLOperation.ANY,
            permission_type=ACLPermissionType.ANY,
            resource_pattern=ResourcePattern(ResourceType.TOPIC, "topic")
        )
    )

    assert error is NoError
    assert len(acls) == 0
