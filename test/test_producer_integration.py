import os
import time
import uuid

from six.moves import range

from kafka import (
    SimpleProducer, KeyedProducer,
    create_message, create_gzip_message, create_snappy_message,
    RoundRobinPartitioner, HashedPartitioner
)
from kafka.codec import has_snappy
from kafka.common import (
    FetchRequest, ProduceRequest,
    UnknownTopicOrPartitionError, LeaderNotAvailableError
)

from test.fixtures import ZookeeperFixture, KafkaFixture
from test.testutil import KafkaIntegrationTestCase, kafka_versions

class TestKafkaProducerIntegration(KafkaIntegrationTestCase):
    topic = b'produce_topic'

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
    def test_produce_many_simple(self):
        start_offset = self.current_offset(self.topic, 0)

        self.assert_produce_request(
            [create_message(("Test message %d" % i).encode('utf-8'))
             for i in range(100)],
            start_offset,
            100,
        )

        self.assert_produce_request(
            [create_message(("Test message %d" % i).encode('utf-8'))
             for i in range(100)],
            start_offset+100,
            100,
        )

    @kafka_versions("all")
    def test_produce_10k_simple(self):
        start_offset = self.current_offset(self.topic, 0)

        self.assert_produce_request(
            [create_message(("Test message %d" % i).encode('utf-8'))
             for i in range(10000)],
            start_offset,
            10000,
        )

    @kafka_versions("all")
    def test_produce_many_gzip(self):
        start_offset = self.current_offset(self.topic, 0)

        message1 = create_gzip_message([
            ("Gzipped 1 %d" % i).encode('utf-8') for i in range(100)])
        message2 = create_gzip_message([
            ("Gzipped 2 %d" % i).encode('utf-8') for i in range(100)])

        self.assert_produce_request(
            [ message1, message2 ],
            start_offset,
            200,
        )

    @kafka_versions("all")
    def test_produce_many_snappy(self):
        self.skipTest("All snappy integration tests fail with nosnappyjava")
        start_offset = self.current_offset(self.topic, 0)

        self.assert_produce_request([
                create_snappy_message(["Snappy 1 %d" % i for i in range(100)]),
                create_snappy_message(["Snappy 2 %d" % i for i in range(100)]),
            ],
            start_offset,
            200,
        )

    @kafka_versions("all")
    def test_produce_mixed(self):
        start_offset = self.current_offset(self.topic, 0)

        msg_count = 1+100
        messages = [
            create_message(b"Just a plain message"),
            create_gzip_message([
                ("Gzipped %d" % i).encode('utf-8') for i in range(100)]),
        ]

        # All snappy integration tests fail with nosnappyjava
        if False and has_snappy():
            msg_count += 100
            messages.append(create_snappy_message(["Snappy %d" % i for i in range(100)]))

        self.assert_produce_request(messages, start_offset, msg_count)

    @kafka_versions("all")
    def test_produce_100k_gzipped(self):
        start_offset = self.current_offset(self.topic, 0)

        self.assert_produce_request([
            create_gzip_message([
                ("Gzipped batch 1, message %d" % i).encode('utf-8')
                for i in range(50000)])
            ],
            start_offset,
            50000,
        )

        self.assert_produce_request([
            create_gzip_message([
                ("Gzipped batch 1, message %d" % i).encode('utf-8')
                for i in range(50000)])
            ],
            start_offset+50000,
            50000,
        )

    ############################
    #   SimpleProducer Tests   #
    ############################

    @kafka_versions("all")
    def test_simple_producer(self):
        start_offset0 = self.current_offset(self.topic, 0)
        start_offset1 = self.current_offset(self.topic, 1)
        producer = SimpleProducer(self.client)

        # Goes to first partition, randomly.
        resp = producer.send_messages(self.topic, self.msg("one"), self.msg("two"))
        self.assert_produce_response(resp, start_offset0)

        # Goes to the next partition, randomly.
        resp = producer.send_messages(self.topic, self.msg("three"))
        self.assert_produce_response(resp, start_offset1)

        self.assert_fetch_offset(0, start_offset0, [ self.msg("one"), self.msg("two") ])
        self.assert_fetch_offset(1, start_offset1, [ self.msg("three") ])

        # Goes back to the first partition because there's only two partitions
        resp = producer.send_messages(self.topic, self.msg("four"), self.msg("five"))
        self.assert_produce_response(resp, start_offset0+2)
        self.assert_fetch_offset(0, start_offset0, [ self.msg("one"), self.msg("two"), self.msg("four"), self.msg("five") ])

        producer.stop()

    @kafka_versions("all")
    def test_produce__new_topic_fails_with_reasonable_error(self):
        new_topic = 'new_topic_{guid}'.format(guid = str(uuid.uuid4())).encode('utf-8')
        producer = SimpleProducer(self.client)

        # At first it doesn't exist
        with self.assertRaises((UnknownTopicOrPartitionError,
                                LeaderNotAvailableError)):
            producer.send_messages(new_topic, self.msg("one"))

    @kafka_versions("all")
    def test_producer_random_order(self):
        producer = SimpleProducer(self.client, random_start = True)
        resp1 = producer.send_messages(self.topic, self.msg("one"), self.msg("two"))
        resp2 = producer.send_messages(self.topic, self.msg("three"))
        resp3 = producer.send_messages(self.topic, self.msg("four"), self.msg("five"))

        self.assertEqual(resp1[0].partition, resp3[0].partition)
        self.assertNotEqual(resp1[0].partition, resp2[0].partition)

    @kafka_versions("all")
    def test_producer_ordered_start(self):
        producer = SimpleProducer(self.client, random_start = False)
        resp1 = producer.send_messages(self.topic, self.msg("one"), self.msg("two"))
        resp2 = producer.send_messages(self.topic, self.msg("three"))
        resp3 = producer.send_messages(self.topic, self.msg("four"), self.msg("five"))

        self.assertEqual(resp1[0].partition, 0)
        self.assertEqual(resp2[0].partition, 1)
        self.assertEqual(resp3[0].partition, 0)

    @kafka_versions("all")
    def test_round_robin_partitioner(self):
        start_offset0 = self.current_offset(self.topic, 0)
        start_offset1 = self.current_offset(self.topic, 1)

        producer = KeyedProducer(self.client, partitioner=RoundRobinPartitioner)
        resp1 = producer.send(self.topic, self.key("key1"), self.msg("one"))
        resp2 = producer.send(self.topic, self.key("key2"), self.msg("two"))
        resp3 = producer.send(self.topic, self.key("key3"), self.msg("three"))
        resp4 = producer.send(self.topic, self.key("key4"), self.msg("four"))

        self.assert_produce_response(resp1, start_offset0+0)
        self.assert_produce_response(resp2, start_offset1+0)
        self.assert_produce_response(resp3, start_offset0+1)
        self.assert_produce_response(resp4, start_offset1+1)

        self.assert_fetch_offset(0, start_offset0, [ self.msg("one"), self.msg("three") ])
        self.assert_fetch_offset(1, start_offset1, [ self.msg("two"), self.msg("four")  ])

        producer.stop()

    @kafka_versions("all")
    def test_hashed_partitioner(self):
        start_offset0 = self.current_offset(self.topic, 0)
        start_offset1 = self.current_offset(self.topic, 1)

        producer = KeyedProducer(self.client, partitioner=HashedPartitioner)
        resp1 = producer.send(self.topic, self.key("1"), self.msg("one"))
        resp2 = producer.send(self.topic, self.key("2"), self.msg("two"))
        resp3 = producer.send(self.topic, self.key("3"), self.msg("three"))
        resp4 = producer.send(self.topic, self.key("3"), self.msg("four"))
        resp5 = producer.send(self.topic, self.key("4"), self.msg("five"))

        offsets = {0: start_offset0, 1: start_offset1}
        messages = {0: [], 1: []}

        keys = [self.key(k) for k in ["1", "2", "3", "3", "4"]]
        resps = [resp1, resp2, resp3, resp4, resp5]
        msgs = [self.msg(m) for m in ["one", "two", "three", "four", "five"]]

        for key, resp, msg in zip(keys, resps, msgs):
            k = hash(key) % 2
            offset = offsets[k]
            self.assert_produce_response(resp, offset)
            offsets[k] += 1
            messages[k].append(msg)

        self.assert_fetch_offset(0, start_offset0, messages[0])
        self.assert_fetch_offset(1, start_offset1, messages[1])

        producer.stop()

    @kafka_versions("all")
    def test_acks_none(self):
        start_offset0 = self.current_offset(self.topic, 0)

        producer = SimpleProducer(self.client, req_acks=SimpleProducer.ACK_NOT_REQUIRED)
        resp = producer.send_messages(self.topic, self.msg("one"))
        self.assertEqual(len(resp), 0)

        self.assert_fetch_offset(0, start_offset0, [ self.msg("one") ])
        producer.stop()

    @kafka_versions("all")
    def test_acks_local_write(self):
        start_offset0 = self.current_offset(self.topic, 0)

        producer = SimpleProducer(self.client, req_acks=SimpleProducer.ACK_AFTER_LOCAL_WRITE)
        resp = producer.send_messages(self.topic, self.msg("one"))

        self.assert_produce_response(resp, start_offset0)
        self.assert_fetch_offset(0, start_offset0, [ self.msg("one") ])

        producer.stop()

    @kafka_versions("all")
    def test_acks_cluster_commit(self):
        start_offset0 = self.current_offset(self.topic, 0)

        producer = SimpleProducer(
            self.client,
            req_acks=SimpleProducer.ACK_AFTER_CLUSTER_COMMIT)

        resp = producer.send_messages(self.topic, self.msg("one"))
        self.assert_produce_response(resp, start_offset0)
        self.assert_fetch_offset(0, start_offset0, [ self.msg("one") ])

        producer.stop()

    @kafka_versions("all")
    def test_batched_simple_producer__triggers_by_message(self):
        start_offset0 = self.current_offset(self.topic, 0)
        start_offset1 = self.current_offset(self.topic, 1)

        producer = SimpleProducer(self.client,
                                  batch_send=True,
                                  batch_send_every_n=5,
                                  batch_send_every_t=20)

        # Send 5 messages and do a fetch
        resp = producer.send_messages(self.topic,
            self.msg("one"),
            self.msg("two"),
            self.msg("three"),
            self.msg("four"),
        )

        # Batch mode is async. No ack
        self.assertEqual(len(resp), 0)

        # It hasn't sent yet
        self.assert_fetch_offset(0, start_offset0, [])
        self.assert_fetch_offset(1, start_offset1, [])

        resp = producer.send_messages(self.topic,
            self.msg("five"),
            self.msg("six"),
            self.msg("seven"),
        )

        # Batch mode is async. No ack
        self.assertEqual(len(resp), 0)

        self.assert_fetch_offset(0, start_offset0, [
            self.msg("one"),
            self.msg("two"),
            self.msg("three"),
            self.msg("four"),
        ])

        self.assert_fetch_offset(1, start_offset1, [
            self.msg("five"),
        #    self.msg("six"),
        #    self.msg("seven"),
        ])

        producer.stop()

    @kafka_versions("all")
    def test_batched_simple_producer__triggers_by_time(self):
        start_offset0 = self.current_offset(self.topic, 0)
        start_offset1 = self.current_offset(self.topic, 1)

        producer = SimpleProducer(self.client,
                                  batch_send=True,
                                  batch_send_every_n=100,
                                  batch_send_every_t=5)

        # Send 5 messages and do a fetch
        resp = producer.send_messages(self.topic,
            self.msg("one"),
            self.msg("two"),
            self.msg("three"),
            self.msg("four"),
        )

        # Batch mode is async. No ack
        self.assertEqual(len(resp), 0)

        # It hasn't sent yet
        self.assert_fetch_offset(0, start_offset0, [])
        self.assert_fetch_offset(1, start_offset1, [])

        resp = producer.send_messages(self.topic,
            self.msg("five"),
            self.msg("six"),
            self.msg("seven"),
        )

        # Batch mode is async. No ack
        self.assertEqual(len(resp), 0)

        # Wait the timeout out
        time.sleep(5)

        self.assert_fetch_offset(0, start_offset0, [
            self.msg("one"),
            self.msg("two"),
            self.msg("three"),
            self.msg("four"),
        ])

        self.assert_fetch_offset(1, start_offset1, [
            self.msg("five"),
            self.msg("six"),
            self.msg("seven"),
        ])

        producer.stop()

    @kafka_versions("all")
    def test_async_simple_producer(self):
        start_offset0 = self.current_offset(self.topic, 0)

        producer = SimpleProducer(self.client, async=True)
        resp = producer.send_messages(self.topic, self.msg("one"))
        self.assertEqual(len(resp), 0)

        self.assert_fetch_offset(0, start_offset0, [ self.msg("one") ])

        producer.stop()

    @kafka_versions("all")
    def test_async_keyed_producer(self):
        start_offset0 = self.current_offset(self.topic, 0)

        producer = KeyedProducer(self.client, partitioner = RoundRobinPartitioner, async=True)

        resp = producer.send(self.topic, self.key("key1"), self.msg("one"))
        self.assertEqual(len(resp), 0)

        self.assert_fetch_offset(0, start_offset0, [ self.msg("one") ])

        producer.stop()

    def assert_produce_request(self, messages, initial_offset, message_ct):
        produce = ProduceRequest(self.topic, 0, messages=messages)

        # There should only be one response message from the server.
        # This will throw an exception if there's more than one.
        resp = self.client.send_produce_request([ produce ])
        self.assert_produce_response(resp, initial_offset)

        self.assertEqual(self.current_offset(self.topic, 0), initial_offset + message_ct)

    def assert_produce_response(self, resp, initial_offset):
        self.assertEqual(len(resp), 1)
        self.assertEqual(resp[0].error, 0)
        self.assertEqual(resp[0].offset, initial_offset)

    def assert_fetch_offset(self, partition, start_offset, expected_messages):
        # There should only be one response message from the server.
        # This will throw an exception if there's more than one.

        resp, = self.client.send_fetch_request([ FetchRequest(self.topic, partition, start_offset, 1024) ])

        self.assertEqual(resp.error, 0)
        self.assertEqual(resp.partition, partition)
        messages = [ x.message.value for x in resp.messages ]

        self.assertEqual(messages, expected_messages)
        self.assertEqual(resp.highwaterMark, start_offset+len(expected_messages))
