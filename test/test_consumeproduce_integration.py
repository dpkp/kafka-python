# -*- coding: utf-8 -*-
import os
from test.fixtures import ZookeeperFixture, KafkaFixture
from test.testutil import (
    KafkaIntegrationTestCase, kafka_versions, random_string
)
from kafka import (
    SimpleConsumer, SimpleProducer
)
from kafka.protocol import CODEC_SNAPPY


class TestConsumerIntegration(KafkaIntegrationTestCase):
    @classmethod
    def setUpClass(cls):
        if not os.environ.get('KAFKA_VERSION'):
            return

        cls.zk = ZookeeperFixture.instance()
        chroot = random_string(10)
        cls.server = KafkaFixture.instance(0, cls.zk.host, cls.zk.port,
                                            zk_chroot=chroot)

    @classmethod
    def tearDownClass(cls):
        if not os.environ.get('KAFKA_VERSION'):
            return

        cls.server.close()
        cls.zk.close()

    def test_snappy(self):
        cons = SimpleConsumer(self.client, "group", self.topic)
        prod = SimpleProducer(self.client, codec=CODEC_SNAPPY)
        content = "hey-hey are you going to transfer me?"
        prod.send_messages(self.topic, content)
        msg = cons.get_message()
        assert msg.message.value == content
