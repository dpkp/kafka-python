import pytest
from . import unittest
from .testutil import random_string

from test.fixtures import ZookeeperFixture, KafkaFixture
from test.testutil import env_kafka_version
# from kafka.client import SimpleClient, KafkaClient
# from kafka.producer import KafkaProducer, SimpleProducer
# from kafka.consumer import SimpleConsumer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic


class SASLIntegrationTestCase(unittest.TestCase):
    sasl_mechanism = None
    sasl_transport = "SASL_PLAINTEXT"
    server = None
    zk = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.zk = ZookeeperFixture.instance()
        cls.server = KafkaFixture.instance(
            0, cls.zk, zk_chroot=random_string(10), transport=cls.sasl_transport, sasl_mechanism=cls.sasl_mechanism
        )

    @classmethod
    def tearDownClass(cls) -> None:
        pass

    @classmethod
    def bootstrap_servers(cls) -> str:
        return "{}:{}".format(cls.server.host, cls.server.port)

    def test_admin(self):
        admin = self.create_admin()
        admin.create_topics([NewTopic('mytopic', 1, 1)])

    def create_admin(self) -> KafkaAdminClient:
        raise NotImplementedError()


@pytest.mark.skipif(
    not env_kafka_version() or env_kafka_version() < (0, 10), reason="No KAFKA_VERSION or version too low"
)
class TestSaslPlain(SASLIntegrationTestCase):
    sasl_mechanism = "PLAIN"

    def create_admin(self) -> KafkaAdminClient:
        return KafkaAdminClient(
            bootstrap_servers=self.bootstrap_servers(),
            security_protocol=self.sasl_transport,
            sasl_mechanism=self.sasl_mechanism,
            sasl_plain_username=self.server.broker_user,
            sasl_plain_password=self.server.broker_password
        )

#
# @pytest.mark.skipif(
#     not env_kafka_version() or env_kafka_version() < (0, 10, 2), reason="No KAFKA_VERSION or version too low"
# )
# class TestSaslScram256(SASLIntegrationTestCase):
#     sasl_mechanism = "SCRAM-SHA-256"
#
#
#
# @pytest.mark.skipif(
#     not env_kafka_version() or env_kafka_version() < (0, 10, 2), reason="No KAFKA_VERSION or version too low"
# )
# class TestSaslScram512(SASLIntegrationTestCase):
#     sasl_mechanism = "SCRAM-SHA-512"
