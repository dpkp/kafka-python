import os

from kafka.errors import KafkaTimeoutError
from kafka.protocol import create_message
from kafka.structs import (
    FetchRequestPayload, OffsetCommitRequestPayload, OffsetFetchRequestPayload,
    ProduceRequestPayload)

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

    def test_consume_none(self):
        fetch = FetchRequestPayload(self.topic, 0, 0, 1024)

        fetch_resp, = self.client.send_fetch_request([fetch])
        self.assertEqual(fetch_resp.error, 0)
        self.assertEqual(fetch_resp.topic, self.topic)
        self.assertEqual(fetch_resp.partition, 0)

        messages = list(fetch_resp.messages)
        self.assertEqual(len(messages), 0)

    def test_ensure_topic_exists(self):

        # assume that self.topic was created by setUp
        # if so, this should succeed
        self.client.ensure_topic_exists(self.topic, timeout=1)

        # ensure_topic_exists should fail with KafkaTimeoutError
        with self.assertRaises(KafkaTimeoutError):
            self.client.ensure_topic_exists('this_topic_doesnt_exist', timeout=0)

    def test_send_produce_request_maintains_request_response_order(self):

        self.client.ensure_topic_exists('foo')
        self.client.ensure_topic_exists('bar')

        requests = [
            ProduceRequestPayload(
                'foo', 0,
                [create_message(b'a'), create_message(b'b')]),
            ProduceRequestPayload(
                'bar', 1,
                [create_message(b'a'), create_message(b'b')]),
            ProduceRequestPayload(
                'foo', 1,
                [create_message(b'a'), create_message(b'b')]),
            ProduceRequestPayload(
                'bar', 0,
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

    @kafka_versions('>=0.8.1')
    def test_commit_fetch_offsets(self):
        req = OffsetCommitRequestPayload(self.topic, 0, 42, 'metadata')
        (resp,) = self.client.send_offset_commit_request('group', [req])
        self.assertEqual(resp.error, 0)

        req = OffsetFetchRequestPayload(self.topic, 0)
        (resp,) = self.client.send_offset_fetch_request('group', [req])
        self.assertEqual(resp.error, 0)
        self.assertEqual(resp.offset, 42)
        self.assertEqual(resp.metadata, '')  # Metadata isn't stored for now
