import os

from kafka.common import (
    FetchRequest, OffsetCommitRequest, OffsetFetchRequest,
    KafkaTimeoutError
)

from test.fixtures import ZookeeperFixture, KafkaFixture
from test.testutil import (
    KafkaIntegrationTestCase, kafka_versions
)

class TestKafkaClientIntegration(KafkaIntegrationTestCase):
    @classmethod
    def setUpClass(cls):  # noqa
        if not os.environ.get('KAFKA_VERSION'):
            return

        cls.zk = ZookeeperFixture.instance()
        cls.server = KafkaFixture.instance(0, cls.zk.host, cls.zk.port)

    @classmethod
    def tearDownClass(cls):  # noqa
        if not os.environ.get('KAFKA_VERSION'):
            return

        cls.server.close()
        cls.zk.close()

    @kafka_versions("all")
    def test_consume_none(self):
        fetch = FetchRequest(self.topic, 0, 0, 1024)

        fetch_resp, = self.client.send_fetch_request([fetch])
        self.assertEqual(fetch_resp.error, 0)
        self.assertEqual(fetch_resp.topic, self.topic)
        self.assertEqual(fetch_resp.partition, 0)

        messages = list(fetch_resp.messages)
        self.assertEqual(len(messages), 0)

    @kafka_versions("all")
    def test_ensure_topic_exists(self):

        # assume that self.topic was created by setUp
        # if so, this should succeed
        self.client.ensure_topic_exists(self.topic, timeout=1)

        # ensure_topic_exists should fail with KafkaTimeoutError
        with self.assertRaises(KafkaTimeoutError):
            self.client.ensure_topic_exists(b"this_topic_doesnt_exist", timeout=0)

    ####################
    #   Offset Tests   #
    ####################

    @kafka_versions("0.8.1", "0.8.1.1")
    def test_commit_fetch_offsets(self):
        req = OffsetCommitRequest(self.topic, 0, 42, b"metadata")
        (resp,) = self.client.send_offset_commit_request(b"group", [req])
        self.assertEqual(resp.error, 0)

        req = OffsetFetchRequest(self.topic, 0)
        (resp,) = self.client.send_offset_fetch_request(b"group", [req])
        self.assertEqual(resp.error, 0)
        self.assertEqual(resp.offset, 42)
        self.assertEqual(resp.metadata, b"")  # Metadata isn't stored for now
