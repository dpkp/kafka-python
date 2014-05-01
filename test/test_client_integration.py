import os
import random
import socket
import time
import unittest2

import kafka
from kafka.common import *
from fixtures import ZookeeperFixture, KafkaFixture
from testutil import *

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

    @unittest2.skip("This doesn't appear to work on Linux?")
    def test_timeout(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_port = get_open_port()
        server_socket.bind(('localhost', server_port))

        with Timer() as t:
            with self.assertRaises((socket.timeout, socket.error)):
                conn = kafka.conn.KafkaConnection("localhost", server_port, 1.0)
        self.assertGreaterEqual(t.interval, 1.0)

    @kafka_versions("all")
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

    @kafka_versions("0.8.1")
    def test_commit_fetch_offsets(self):
        req = OffsetCommitRequest(self.topic, 0, 42, "metadata")
        (resp,) = self.client.send_offset_commit_request("group", [req])
        self.assertEquals(resp.error, 0)

        req = OffsetFetchRequest(self.topic, 0)
        (resp,) = self.client.send_offset_fetch_request("group", [req])
        self.assertEquals(resp.error, 0)
        self.assertEquals(resp.offset, 42)
        self.assertEquals(resp.metadata, "")  # Metadata isn't stored for now
