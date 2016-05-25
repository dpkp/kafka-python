# -*- coding: utf-8 -*-
import os
from test.fixtures import ZookeeperFixture, KafkaFixture
from test.testutil import (
    KafkaIntegrationTestCase, kafka_versions, random_string
)
from kafka import (
    SimpleConsumer, SimpleProducer, SimpleClient
)
from kafka.protocol import CODEC_SNAPPY
import six


class TestConsumerIntegration(KafkaIntegrationTestCase):
    @classmethod
    def setUpClass(cls):
        if not os.environ.get('KAFKA_VERSION'):
            return

        cls.zk = ZookeeperFixture.instance()
        chroot = random_string(10)
        cls.server = KafkaFixture.instance(0, cls.zk.host, cls.zk.port,
                                            zk_chroot=chroot)

    def setUp(self):
        super(TestConsumerIntegration, self).setUp()
        self.client_producer = SimpleClient('%s:%d' % (self.server.host, self.server.port))

    @classmethod
    def tearDownClass(cls):
        if not os.environ.get('KAFKA_VERSION'):
            return

        cls.server.close()
        cls.zk.close()

    @kafka_versions('>=0.8.1')
    def test_snappy(self):
        cons = SimpleConsumer(self.client, "group", self.topic)
        prod = SimpleProducer(self.client_producer, codec=CODEC_SNAPPY)
        content = six.b("hey-hey are you going to transfer me?")
        prod.send_messages(self.topic, content)
        msg = cons.get_message(timeout=1.0)
        assert msg.message.value == content

