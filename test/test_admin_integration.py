import os

from kafka import KafkaAdminClient
from kafka.admin import ConfigResource, ConfigResourceType
from test.fixtures import ZookeeperFixture, KafkaFixture
from test.testutil import KafkaIntegrationTestCase, kafka_versions


class TestAdminClientIntegration(KafkaIntegrationTestCase):
    @classmethod
    def setUpClass(cls):  # noqa
        if not os.environ.get('KAFKA_VERSION'):
            return

        cls.zk = ZookeeperFixture.instance()
        cls.server = KafkaFixture.instance(0, cls.zk)

    @classmethod
    def tearDownClass(cls):  # noqa
        if not os.environ.get('KAFKA_VERSION'):
            return

        cls.server.close()
        cls.zk.close()

    def admin_client(self, **configs):
        brokers = '%s:%d' % (self.server.host, self.server.port)
        admin_client = KafkaAdminClient(
            bootstrap_servers=brokers, **configs)
        return admin_client

    @kafka_versions('>=0.10.0')
    def test_describe_configs_broker_resource_returns_configs(self):
        broker_id = self.client.brokers[0].nodeId
        configs = self.admin_client().describe_configs([ConfigResource(ConfigResourceType.BROKER, broker_id)])

        self.assertEqual(len(configs.resources), 1)
        self.assertEqual(configs.resources[0][2], ConfigResourceType.BROKER)
        self.assertEqual(configs.resources[0][3], str(broker_id))
        self.assertGreater(len(configs.resources[0][4]), 1)

    @kafka_versions('>=0.10.0')
    def test_describe_configs_topic_resource_returns_configs(self):
        topic = self.client.topics[0]
        configs = self.admin_client().describe_configs([ConfigResource(ConfigResourceType.TOPIC, topic)])

        self.assertEqual(len(configs.resources), 1)
        self.assertEqual(configs.resources[0][2], ConfigResourceType.TOPIC)
        self.assertEqual(configs.resources[0][3], topic)
        self.assertGreater(len(configs.resources[0][4]), 1)

    @kafka_versions('>=0.10.0')
    def test_describe_configs_mixed_resources_returns_configs(self):
        topic = self.client.topics[0]
        broker_id = self.client.brokers[0].nodeId
        configs = self.admin_client().describe_configs([
            ConfigResource(ConfigResourceType.TOPIC, topic),
            ConfigResource(ConfigResourceType.BROKER, broker_id)])

        self.assertEqual(len(configs.resources), 2)

        for resource in configs.resources:
            self.assertTrue(
                (configs.resources[0][2] == ConfigResourceType.TOPIC and configs.resources[0][3] == topic) or
                (configs.resources[0][2] == ConfigResourceType.BROKER and configs.resources[0][3] == str(broker_id)))
            self.assertGreater(len(resource[4]), 1)
