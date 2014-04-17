import unittest
import time

from kafka import *  # noqa
from kafka.common import *  # noqa
from kafka.codec import has_gzip, has_snappy
from .fixtures import ZookeeperFixture, KafkaFixture
from .testutil import *

class TestKafkaProducerIntegration(KafkaIntegrationTestCase):
    topic = 'produce_topic'

    @classmethod
    def setUpClass(cls):  # noqa
        cls.zk = ZookeeperFixture.instance()
        cls.server = KafkaFixture.instance(0, cls.zk.host, cls.zk.port)
        cls.client = KafkaClient('%s:%d' % (cls.server.host, cls.server.port))

    @classmethod
    def tearDownClass(cls):  # noqa
        cls.client.close()
        cls.server.close()
        cls.zk.close()

    def test_produce_many_simple(self):
        start_offset = self.current_offset(self.topic, 0)

        produce = ProduceRequest(self.topic, 0, messages=[
            create_message("Test message %d" % i) for i in range(100)
        ])

        resp = self.client.send_produce_request([produce])
        self.assertEqual(len(resp), 1)                 # Only one response
        self.assertEqual(resp[0].error, 0)             # No error
        self.assertEqual(resp[0].offset, start_offset) # Initial offset of first message

        self.assertEqual(self.current_offset(self.topic, 0), start_offset+100)

        resp = self.client.send_produce_request([produce])
        self.assertEqual(len(resp), 1)                     # Only one response
        self.assertEqual(resp[0].error, 0)                 # No error
        self.assertEqual(resp[0].offset, start_offset+100) # Initial offset of first message

        self.assertEqual(self.current_offset(self.topic, 0), start_offset+200)

    def test_produce_10k_simple(self):
        start_offset = self.current_offset(self.topic, 0)

        produce = ProduceRequest(self.topic, 0, messages=[
            create_message("Test message %d" % i) for i in range(10000)
        ])

        resp = self.client.send_produce_request([produce])
        self.assertEqual(len(resp), 1)                 # Only one response
        self.assertEqual(resp[0].error, 0)             # No error
        self.assertEqual(resp[0].offset, start_offset) # Initial offset of first message

        self.assertEqual(self.current_offset(self.topic, 0), start_offset+10000)

    def test_produce_many_gzip(self):
        start_offset = self.current_offset(self.topic, 0)

        message1 = create_gzip_message(["Gzipped 1 %d" % i for i in range(100)])
        message2 = create_gzip_message(["Gzipped 2 %d" % i for i in range(100)])

        produce = ProduceRequest(self.topic, 0, messages=[message1, message2])

        resp = self.client.send_produce_request([produce])
        self.assertEqual(len(resp), 1)                 # Only one response
        self.assertEqual(resp[0].error, 0)             # No error
        self.assertEqual(resp[0].offset, start_offset) # Initial offset of first message

        self.assertEqual(self.current_offset(self.topic, 0), start_offset+200)

    @unittest.skip("All snappy integration tests fail with nosnappyjava")
    def test_produce_many_snappy(self):
        start_offset = self.current_offset(self.topic, 0)

        produce = ProduceRequest(self.topic, 0, messages=[
            create_snappy_message(["Snappy 1 %d" % i for i in range(100)]),
            create_snappy_message(["Snappy 2 %d" % i for i in range(100)]),
        ])

        resp = self.client.send_produce_request([produce])

        self.assertEqual(len(resp), 1)                 # Only one response
        self.assertEqual(resp[0].error, 0)             # No error
        self.assertEqual(resp[0].offset, start_offset) # Initial offset of first message

        self.assertEqual(self.current_offset(self.topic, 0), start_offset+200)

    def test_produce_mixed(self):
        start_offset = self.current_offset(self.topic, 0)

        msg_count = 1+100
        messages = [
            create_message("Just a plain message"),
            create_gzip_message(["Gzipped %d" % i for i in range(100)]),
        ]

        # All snappy integration tests fail with nosnappyjava
        if False and has_snappy():
            msg_count += 100
            messages.append(create_snappy_message(["Snappy %d" % i for i in range(100)]))

        produce = ProduceRequest(self.topic, 0, messages=messages)
        resp = self.client.send_produce_request([produce])

        self.assertEqual(len(resp), 1)                 # Only one response
        self.assertEqual(resp[0].error, 0)             # No error
        self.assertEqual(resp[0].offset, start_offset) # Initial offset of first message

        self.assertEqual(self.current_offset(self.topic, 0), start_offset+msg_count)

    def test_produce_100k_gzipped(self):
        start_offset = self.current_offset(self.topic, 0)

        req1 = ProduceRequest(self.topic, 0, messages=[
            create_gzip_message(["Gzipped batch 1, message %d" % i for i in range(50000)])
        ])
        resp1 = self.client.send_produce_request([req1])

        self.assertEqual(len(resp1), 1)                 # Only one response
        self.assertEqual(resp1[0].error, 0)             # No error
        self.assertEqual(resp1[0].offset, start_offset) # Initial offset of first message

        self.assertEqual(self.current_offset(self.topic, 0), start_offset+50000)

        req2 = ProduceRequest(self.topic, 0, messages=[
            create_gzip_message(["Gzipped batch 2, message %d" % i for i in range(50000)])
        ])

        resp2 = self.client.send_produce_request([req2])

        self.assertEqual(len(resp2), 1)                       # Only one response
        self.assertEqual(resp2[0].error, 0)                   # No error
        self.assertEqual(resp2[0].offset, start_offset+50000) # Initial offset of first message

        self.assertEqual(self.current_offset(self.topic, 0), start_offset+100000)
