import pytest
from . import unittest
from .testutil import random_string

from test.fixtures import ZookeeperFixture, KafkaFixture
from test.testutil import KafkaIntegrationTestCase, env_kafka_version


class SASLIntegrationTestCase(unittest.TestCase):
    sasl_mechanism = "PLAIN"
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


@pytest.mark.skipif(
    not env_kafka_version() or env_kafka_version() < (0, 10), reason="No KAFKA_VERSION or version too low"
)
class TestSaslPlain(SASLIntegrationTestCase):
    def test_sasl_plain(self):
        pass

    def test_sasl_scram(self, strength):
        pass


@pytest.mark.skipif(
    not env_kafka_version() or env_kafka_version() < (0, 10, 2), reason="No KAFKA_VERSION or version too low"
)
class TestSaslScram256(SASLIntegrationTestCase):
    sasl_mechanism = "SCRAM-SHA-256"

    def test_sasl_plain(self):
        pass

    @pytest.mark.parametrize("strength", [256, 512])
    def test_sasl_scram(self, strength):
        pass


@pytest.mark.skipif(
    not env_kafka_version() or env_kafka_version() < (0, 10, 2), reason="No KAFKA_VERSION or version too low"
)
class TestSaslScram512(SASLIntegrationTestCase):
    sasl_mechanism = "SCRAM-SHA-512"
