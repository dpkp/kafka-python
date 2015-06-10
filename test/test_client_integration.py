import os

from kafka.common import (
    FetchRequest, OffsetCommitRequest, OffsetFetchRequest,
    KafkaTimeoutError, ProduceRequest
)
from kafka.protocol import create_message

from test.fixtures import ZookeeperFixture, KafkaFixture
from test.testutil import KafkaIntegrationTestCase, kafka_versions


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
        fetch = FetchRequest(self.bytes_topic, 0, 0, 1024)

        fetch_resp, = self.client.send_fetch_request([fetch])
        self.assertEqual(fetch_resp.error, 0)
        self.assertEqual(fetch_resp.topic, self.bytes_topic)
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

    @kafka_versions('all')
    def test_send_produce_request_maintains_request_response_order(self):

        self.client.ensure_topic_exists(b'foo')
        self.client.ensure_topic_exists(b'bar')

        requests = [
            ProduceRequest(
                b'foo', 0,
                [create_message(b'a'), create_message(b'b')]),
            ProduceRequest(
                b'bar', 1,
                [create_message(b'a'), create_message(b'b')]),
            ProduceRequest(
                b'foo', 1,
                [create_message(b'a'), create_message(b'b')]),
            ProduceRequest(
                b'bar', 0,
                [create_message(b'a'), create_message(b'b')]),
        ]

        responses = self.client.send_produce_request(requests)
        while len(responses):
            request = requests.pop()
            response = responses.pop()
            self.assertEqual(request.topic, response.topic)
            self.assertEqual(request.partition, response.partition)


    ####################
    #   Offset Tests   #
    ####################

    @kafka_versions("0.8.1", "0.8.1.1", "0.8.2.1")
    def test_commit_fetch_offsets(self):
        req = OffsetCommitRequest(self.bytes_topic, 0, 42, b"metadata")
        (resp,) = self.client.send_offset_commit_request(b"group", [req])
        self.assertEqual(resp.error, 0)

        req = OffsetFetchRequest(self.bytes_topic, 0)
        (resp,) = self.client.send_offset_fetch_request(b"group", [req])
        self.assertEqual(resp.error, 0)
        self.assertEqual(resp.offset, 42)
        self.assertEqual(resp.metadata, b"")  # Metadata isn't stored for now
