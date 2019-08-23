from __future__ import absolute_import

import os
import random
import string
import time
import uuid

import pytest
from . import unittest

from kafka import SimpleClient
from kafka.errors import (
    LeaderNotAvailableError, KafkaTimeoutError, InvalidTopicError,
    NotLeaderForPartitionError, UnknownTopicOrPartitionError,
    FailedPayloadsError
)
from kafka.structs import OffsetRequestPayload


def random_string(length):
    return "".join(random.choice(string.ascii_letters) for i in range(length))


def env_kafka_version():
    """Return the Kafka version set in the OS environment as a tuple.

     Example: '0.8.1.1' --> (0, 8, 1, 1)
    """
    if 'KAFKA_VERSION' not in os.environ:
        return ()
    return tuple(map(int, os.environ['KAFKA_VERSION'].split('.')))


def current_offset(client, topic, partition, kafka_broker=None):
    """Get the current offset of a topic's partition
    """
    try:
        offsets, = client.send_offset_request([OffsetRequestPayload(topic,
                                                                    partition, -1, 1)])
    except Exception:
        # XXX: We've seen some UnknownErrors here and can't debug w/o server logs
        if kafka_broker:
            kafka_broker.dump_logs()
        raise
    else:
        return offsets.offsets[0]


def assert_message_count(messages, num_messages):
    """Check that we received the expected number of messages with no duplicates."""
    # Make sure we got them all
    assert len(messages) == num_messages
    # Make sure there are no duplicates
    # Note: Currently duplicates are identified only using key/value. Other attributes like topic, partition, headers,
    # timestamp, etc are ignored... this could be changed if necessary, but will be more tolerant of dupes.
    unique_messages = {(m.key, m.value) for m in messages}
    assert len(unique_messages) == num_messages


class KafkaIntegrationTestCase(unittest.TestCase):
    create_client = True
    topic = None
    zk = None
    server = None

    def setUp(self):
        super(KafkaIntegrationTestCase, self).setUp()
        if not os.environ.get('KAFKA_VERSION'):
            self.skipTest('Integration test requires KAFKA_VERSION')

        if not self.topic:
            topic = "%s-%s" % (self.id()[self.id().rindex(".") + 1:], random_string(10))
            self.topic = topic

        if self.create_client:
            self.client = SimpleClient('%s:%d' % (self.server.host, self.server.port))

        timeout = time.time() + 30
        while time.time() < timeout:
            try:
                self.client.load_metadata_for_topics(self.topic, ignore_leadernotavailable=False)
                if self.client.has_metadata_for_topic(topic):
                    break
            except (LeaderNotAvailableError, InvalidTopicError):
                time.sleep(1)
        else:
            raise KafkaTimeoutError('Timeout loading topic metadata!')


        # Ensure topic partitions have been created on all brokers to avoid UnknownPartitionErrors
        # TODO: It might be a good idea to move this to self.client.ensure_topic_exists
        for partition in self.client.get_partition_ids_for_topic(self.topic):
            while True:
                try:
                    req = OffsetRequestPayload(self.topic, partition, -1, 100)
                    self.client.send_offset_request([req])
                    break
                except (NotLeaderForPartitionError, UnknownTopicOrPartitionError, FailedPayloadsError) as e:
                    if time.time() > timeout:
                        raise KafkaTimeoutError('Timeout loading topic metadata!')
                    time.sleep(.1)

        self._messages = {}

    def tearDown(self):
        super(KafkaIntegrationTestCase, self).tearDown()
        if not os.environ.get('KAFKA_VERSION'):
            return

        if self.create_client:
            self.client.close()

    def current_offset(self, topic, partition):
        try:
            offsets, = self.client.send_offset_request([OffsetRequestPayload(topic,
                                                                             partition, -1, 1)])
        except Exception:
            # XXX: We've seen some UnknownErrors here and can't debug w/o server logs
            self.zk.child.dump_logs()
            self.server.child.dump_logs()
            raise
        else:
            return offsets.offsets[0]

    def msgs(self, iterable):
        return [self.msg(x) for x in iterable]

    def msg(self, s):
        if s not in self._messages:
            self._messages[s] = '%s-%s-%s' % (s, self.id(), str(uuid.uuid4()))

        return self._messages[s].encode('utf-8')

    def key(self, k):
        return k.encode('utf-8')


class Timer(object):
    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.interval = self.end - self.start
