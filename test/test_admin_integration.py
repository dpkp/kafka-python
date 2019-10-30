import pytest
import os

from test.fixtures import ZookeeperFixture, KafkaFixture
from test.testutil import KafkaIntegrationTestCase, env_kafka_version, current_offset

from kafka.errors import NoError
from kafka.admin import KafkaAdminClient, ACLFilter, ACLOperation, ACLPermissionType, ResourcePattern, ResourceType, ACL

# This test suite passes for me locally, but fails on travis
# Needs investigation
DISABLED = True

# TODO: Convert to pytest / fixtures
# Note that ACL features require broker 0.11, but other admin apis may work on
# earlier broker versions
class TestAdminClientIntegration(KafkaIntegrationTestCase):
    @classmethod
    def setUpClass(cls):  # noqa
        if env_kafka_version() < (0, 11) or DISABLED:
            return

        cls.zk = ZookeeperFixture.instance()
        cls.server = KafkaFixture.instance(0, cls.zk)

    @classmethod
    def tearDownClass(cls):  # noqa
        if env_kafka_version() < (0, 11) or DISABLED:
            return

        cls.server.close()
        cls.zk.close()

    def setUp(self):
        if env_kafka_version() < (0, 11) or DISABLED:
            self.skipTest('Admin ACL Integration test requires KAFKA_VERSION >= 0.11')
        super(TestAdminClientIntegration, self).setUp()

    def tearDown(self):
        if env_kafka_version() < (0, 11) or DISABLED:
            return
        super(TestAdminClientIntegration, self).tearDown()

    def test_create_describe_delete_acls(self):
        """Tests that we can add, list and remove ACLs
        """

        # Setup
        brokers = '%s:%d' % (self.server.host, self.server.port)
        admin_client = KafkaAdminClient(
            bootstrap_servers=brokers
        )

        # Check that we don't have any ACLs in the cluster
        acls, error = admin_client.describe_acls(
            ACLFilter(
                principal=None,
                host="*",
                operation=ACLOperation.ANY,
                permission_type=ACLPermissionType.ANY,
                resource_pattern=ResourcePattern(ResourceType.TOPIC, "topic")
            )
        )

        self.assertIs(error, NoError)
        self.assertEqual(0, len(acls))

        # Try to add an ACL
        acl = ACL(
            principal="User:test",
            host="*",
            operation=ACLOperation.READ,
            permission_type=ACLPermissionType.ALLOW,
            resource_pattern=ResourcePattern(ResourceType.TOPIC, "topic")
        )
        result = admin_client.create_acls([acl])

        self.assertFalse(len(result["failed"]))
        self.assertEqual(len(result["succeeded"]), 1)

        # Check that we can list the ACL we created
        acl_filter = ACLFilter(
            principal=None,
            host="*",
            operation=ACLOperation.ANY,
            permission_type=ACLPermissionType.ANY,
            resource_pattern=ResourcePattern(ResourceType.TOPIC, "topic")
        )
        acls, error = admin_client.describe_acls(acl_filter)

        self.assertIs(error, NoError)
        self.assertEqual(1, len(acls))

        # Remove the ACL
        delete_results = admin_client.delete_acls(
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

        self.assertEqual(1, len(delete_results))
        self.assertEqual(1, len(delete_results[0][1]))  # Check number of affected ACLs


        # Make sure the ACL does not exist in the cluster anymore
        acls, error = admin_client.describe_acls(
            ACLFilter(
                principal="*",
                host="*",
                operation=ACLOperation.ANY,
                permission_type=ACLPermissionType.ANY,
                resource_pattern=ResourcePattern(ResourceType.TOPIC, "topic")
            )
        )
        self.assertIs(error, NoError)
        self.assertEqual(0, len(acls))
