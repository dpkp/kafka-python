import unittest
import time

from kafka import *  # noqa
from kafka.common import *  # noqa
from kafka.codec import has_gzip, has_snappy
from .fixtures import ZookeeperFixture, KafkaFixture
from .testutil import *

@unittest.skipIf(skip_integration(), 'Skipping Integration')
class TestKafkaClientIntegration(KafkaIntegrationTestCase):
    @classmethod
    def setUpClass(cls):  # noqa
        cls.zk = ZookeeperFixture.instance()
        cls.server = KafkaFixture.instance(0, cls.zk.host, cls.zk.port)

    @classmethod
    def tearDownClass(cls):  # noqa
        cls.server.close()
        cls.zk.close()

    def test_consume_none(self):
        fetch = FetchRequest(self.topic, 0, 0, 1024)

        fetch_resp, = self.client.send_fetch_request([fetch])
        self.assertEquals(fetch_resp.error, 0)
        self.assertEquals(fetch_resp.topic, self.topic)
        self.assertEquals(fetch_resp.partition, 0)

        messages = list(fetch_resp.messages)
        self.assertEquals(len(messages), 0)

    ####################
    #   Offset Tests   #
    ####################

    @unittest.skip('commit offset not supported in this version')
    def test_commit_fetch_offsets(self):
        req = OffsetCommitRequest(self.topic, 0, 42, "metadata")
        (resp,) = self.client.send_offset_commit_request("group", [req])
        self.assertEquals(resp.error, 0)

        req = OffsetFetchRequest(self.topic, 0)
        (resp,) = self.client.send_offset_fetch_request("group", [req])
        self.assertEquals(resp.error, 0)
        self.assertEquals(resp.offset, 42)
        self.assertEquals(resp.metadata, "")  # Metadata isn't stored for now
