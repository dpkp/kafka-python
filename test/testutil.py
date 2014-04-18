import uuid
import time
import unittest
import os
import random
import string
import logging
from kafka.common import OffsetRequest
from kafka import KafkaClient

def random_string(l):
    s = "".join(random.choice(string.letters) for i in xrange(l))
    return s

def skip_integration():
    return os.environ.get('SKIP_INTEGRATION')

def ensure_topic_creation(client, topic_name, timeout = 30):
    start_time = time.time()

    client.load_metadata_for_topics(topic_name)
    while not client.has_metadata_for_topic(topic_name):
        if time.time() > start_time + timeout:
            raise Exception("Unable to create topic %s" % topic_name)
        client.load_metadata_for_topics(topic_name)
        time.sleep(1)

class KafkaIntegrationTestCase(unittest.TestCase):
    create_client = True
    topic = None

    def setUp(self):
        super(KafkaIntegrationTestCase, self).setUp()
        if not self.topic:
            self.topic = "%s-%s" % (self.id()[self.id().rindex(".") + 1:], random_string(10))

        if self.create_client:
            self.client = KafkaClient('%s:%d' % (self.server.host, self.server.port))

        ensure_topic_creation(self.client, self.topic)

        self._messages = {}

    def tearDown(self):
        super(KafkaIntegrationTestCase, self).tearDown()
        if self.create_client:
            self.client.close()

    def current_offset(self, topic, partition):
        offsets, = self.client.send_offset_request([ OffsetRequest(topic, partition, -1, 1) ])
        return offsets.offsets[0]

    def msgs(self, iterable):
        return [ self.msg(x) for x in iterable ]

    def msg(self, s):
        if s not in self._messages:
            self._messages[s] = '%s-%s-%s' % (s, self.id(), str(uuid.uuid4()))

        return self._messages[s]

class Timer(object):
    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.interval = self.end - self.start

logging.basicConfig(level=logging.DEBUG)
