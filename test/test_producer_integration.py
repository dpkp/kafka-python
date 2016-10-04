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
from kafka.errors import UnknownTopicOrPartitionError, LeaderNotAvailableError
from kafka.producer.base import Producer
from kafka.structs import FetchRequestPayload, ProduceRequestPayload

from test.fixtures import ZookeeperFixture, KafkaFixture
from test.testutil import KafkaIntegrationTestCase, kafka_versions


class TestKafkaProducerIntegration(KafkaIntegrationTestCase):

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

    def test_produce_10k_simple(self):
        start_offset = self.current_offset(self.topic, 0)

        self.assert_produce_request(
            [create_message(("Test message %d" % i).encode('utf-8'))
             for i in range(10000)],
            start_offset,
            10000,
        )

    def test_produce_many_gzip(self):
        start_offset = self.current_offset(self.topic, 0)

        message1 = create_gzip_message([
            (("Gzipped 1 %d" % i).encode('utf-8'), None) for i in range(100)])
        message2 = create_gzip_message([
            (("Gzipped 2 %d" % i).encode('utf-8'), None) for i in range(100)])

        self.assert_produce_request(
            [ message1, message2 ],
            start_offset,
            200,
        )

    def test_produce_many_snappy(self):
        self.skipTest("All snappy integration tests fail with nosnappyjava")
        start_offset = self.current_offset(self.topic, 0)

        self.assert_produce_request([
                create_snappy_message([("Snappy 1 %d" % i, None) for i in range(100)]),
                create_snappy_message([("Snappy 2 %d" % i, None) for i in range(100)]),
            ],
            start_offset,
            200,
        )

    def test_produce_mixed(self):
        start_offset = self.current_offset(self.topic, 0)

        msg_count = 1+100
        messages = [
            create_message(b"Just a plain message"),
            create_gzip_message([
                (("Gzipped %d" % i).encode('utf-8'), None) for i in range(100)]),
        ]

        # All snappy integration tests fail with nosnappyjava
        if False and has_snappy():
            msg_count += 100
            messages.append(create_snappy_message([("Snappy %d" % i, None) for i in range(100)]))

        self.assert_produce_request(messages, start_offset, msg_count)

    def test_produce_100k_gzipped(self):
        start_offset = self.current_offset(self.topic, 0)

        self.assert_produce_request([
            create_gzip_message([
                (("Gzipped batch 1, message %d" % i).encode('utf-8'), None)
                for i in range(50000)])
            ],
            start_offset,
            50000,
        )

        self.assert_produce_request([
            create_gzip_message([
                (("Gzipped batch 1, message %d" % i).encode('utf-8'), None)
                for i in range(50000)])
            ],
            start_offset+50000,
            50000,
        )

    ############################
    #   SimpleProducer Tests   #
    ############################

    def test_simple_producer_new_topic(self):
        producer = SimpleProducer(self.client)
        resp = producer.send_messages('new_topic', self.msg('foobar'))
        self.assert_produce_response(resp, 0)
        producer.stop()

    def test_simple_producer(self):
        partitions = self.client.get_partition_ids_for_topic(self.topic)
        start_offsets = [self.current_offset(self.topic, p) for p in partitions]

        producer = SimpleProducer(self.client, random_start=False)

        # Goes to first partition, randomly.
        resp = producer.send_messages(self.topic, self.msg("one"), self.msg("two"))
        self.assert_produce_response(resp, start_offsets[0])

        # Goes to the next partition, randomly.
        resp = producer.send_messages(self.topic, self.msg("three"))
        self.assert_produce_response(resp, start_offsets[1])

        self.assert_fetch_offset(partitions[0], start_offsets[0], [ self.msg("one"), self.msg("two") ])
        self.assert_fetch_offset(partitions[1], start_offsets[1], [ self.msg("three") ])

        # Goes back to the first partition because there's only two partitions
        resp = producer.send_messages(self.topic, self.msg("four"), self.msg("five"))
        self.assert_produce_response(resp, start_offsets[0]+2)
        self.assert_fetch_offset(partitions[0], start_offsets[0], [ self.msg("one"), self.msg("two"), self.msg("four"), self.msg("five") ])

        producer.stop()

    def test_producer_random_order(self):
        producer = SimpleProducer(self.client, random_start=True)
        resp1 = producer.send_messages(self.topic, self.msg("one"), self.msg("two"))
        resp2 = producer.send_messages(self.topic, self.msg("three"))
        resp3 = producer.send_messages(self.topic, self.msg("four"), self.msg("five"))

        self.assertEqual(resp1[0].partition, resp3[0].partition)
        self.assertNotEqual(resp1[0].partition, resp2[0].partition)

    def test_producer_ordered_start(self):
        producer = SimpleProducer(self.client, random_start=False)
        resp1 = producer.send_messages(self.topic, self.msg("one"), self.msg("two"))
        resp2 = producer.send_messages(self.topic, self.msg("three"))
        resp3 = producer.send_messages(self.topic, self.msg("four"), self.msg("five"))

        self.assertEqual(resp1[0].partition, 0)
        self.assertEqual(resp2[0].partition, 1)
        self.assertEqual(resp3[0].partition, 0)

    def test_async_simple_producer(self):
        partition = self.client.get_partition_ids_for_topic(self.topic)[0]
        start_offset = self.current_offset(self.topic, partition)

        producer = SimpleProducer(self.client, async=True, random_start=False)
        resp = producer.send_messages(self.topic, self.msg("one"))
        self.assertEqual(len(resp), 0)

        # flush messages
        producer.stop()

        self.assert_fetch_offset(partition, start_offset, [ self.msg("one") ])


    def test_batched_simple_producer__triggers_by_message(self):
        partitions = self.client.get_partition_ids_for_topic(self.topic)
        start_offsets = [self.current_offset(self.topic, p) for p in partitions]

        # Configure batch producer
        batch_messages = 5
        batch_interval = 5
        producer = SimpleProducer(
            self.client,
            async=True,
            batch_send_every_n=batch_messages,
            batch_send_every_t=batch_interval,
            random_start=False)

        # Send 4 messages -- should not trigger a batch
        resp = producer.send_messages(
            self.topic,
            self.msg("one"),
            self.msg("two"),
            self.msg("three"),
            self.msg("four"),
        )

        # Batch mode is async. No ack
        self.assertEqual(len(resp), 0)

        # It hasn't sent yet
        self.assert_fetch_offset(partitions[0], start_offsets[0], [])
        self.assert_fetch_offset(partitions[1], start_offsets[1], [])

        # send 3 more messages -- should trigger batch on first 5
        resp = producer.send_messages(
            self.topic,
            self.msg("five"),
            self.msg("six"),
            self.msg("seven"),
        )

        # Batch mode is async. No ack
        self.assertEqual(len(resp), 0)

        # Wait until producer has pulled all messages from internal queue
        # this should signal that the first batch was sent, and the producer
        # is now waiting for enough messages to batch again (or a timeout)
        timeout = 5
        start = time.time()
        while not producer.queue.empty():
            if time.time() - start > timeout:
                self.fail('timeout waiting for producer queue to empty')
            time.sleep(0.1)

        # send messages groups all *msgs in a single call to the same partition
        # so we should see all messages from the first call in one partition
        self.assert_fetch_offset(partitions[0], start_offsets[0], [
            self.msg("one"),
            self.msg("two"),
            self.msg("three"),
            self.msg("four"),
        ])

        # Because we are batching every 5 messages, we should only see one
        self.assert_fetch_offset(partitions[1], start_offsets[1], [
            self.msg("five"),
        ])

        producer.stop()

    def test_batched_simple_producer__triggers_by_time(self):
        partitions = self.client.get_partition_ids_for_topic(self.topic)
        start_offsets = [self.current_offset(self.topic, p) for p in partitions]

        batch_interval = 5
        producer = SimpleProducer(
            self.client,
            async=True,
            batch_send_every_n=100,
            batch_send_every_t=batch_interval,
            random_start=False)

        # Send 5 messages and do a fetch
        resp = producer.send_messages(
            self.topic,
            self.msg("one"),
            self.msg("two"),
            self.msg("three"),
            self.msg("four"),
        )

        # Batch mode is async. No ack
        self.assertEqual(len(resp), 0)

        # It hasn't sent yet
        self.assert_fetch_offset(partitions[0], start_offsets[0], [])
        self.assert_fetch_offset(partitions[1], start_offsets[1], [])

        resp = producer.send_messages(self.topic,
            self.msg("five"),
            self.msg("six"),
            self.msg("seven"),
        )

        # Batch mode is async. No ack
        self.assertEqual(len(resp), 0)

        # Wait the timeout out
        time.sleep(batch_interval)

        self.assert_fetch_offset(partitions[0], start_offsets[0], [
            self.msg("one"),
            self.msg("two"),
            self.msg("three"),
            self.msg("four"),
        ])

        self.assert_fetch_offset(partitions[1], start_offsets[1], [
            self.msg("five"),
            self.msg("six"),
            self.msg("seven"),
        ])

        producer.stop()


    ############################
    #   KeyedProducer Tests    #
    ############################

    @kafka_versions('>=0.8.1')
    def test_keyedproducer_null_payload(self):
        partitions = self.client.get_partition_ids_for_topic(self.topic)
        start_offsets = [self.current_offset(self.topic, p) for p in partitions]

        producer = KeyedProducer(self.client, partitioner=RoundRobinPartitioner)
        key = "test"

        resp = producer.send_messages(self.topic, self.key("key1"), self.msg("one"))
        self.assert_produce_response(resp, start_offsets[0])
        resp = producer.send_messages(self.topic, self.key("key2"), None)
        self.assert_produce_response(resp, start_offsets[1])
        resp = producer.send_messages(self.topic, self.key("key3"), None)
        self.assert_produce_response(resp, start_offsets[0]+1)
        resp = producer.send_messages(self.topic, self.key("key4"), self.msg("four"))
        self.assert_produce_response(resp, start_offsets[1]+1)

        self.assert_fetch_offset(partitions[0], start_offsets[0], [ self.msg("one"), None ])
        self.assert_fetch_offset(partitions[1], start_offsets[1], [ None, self.msg("four") ])

        producer.stop()

    def test_round_robin_partitioner(self):
        partitions = self.client.get_partition_ids_for_topic(self.topic)
        start_offsets = [self.current_offset(self.topic, p) for p in partitions]

        producer = KeyedProducer(self.client, partitioner=RoundRobinPartitioner)
        resp1 = producer.send_messages(self.topic, self.key("key1"), self.msg("one"))
        resp2 = producer.send_messages(self.topic, self.key("key2"), self.msg("two"))
        resp3 = producer.send_messages(self.topic, self.key("key3"), self.msg("three"))
        resp4 = producer.send_messages(self.topic, self.key("key4"), self.msg("four"))

        self.assert_produce_response(resp1, start_offsets[0]+0)
        self.assert_produce_response(resp2, start_offsets[1]+0)
        self.assert_produce_response(resp3, start_offsets[0]+1)
        self.assert_produce_response(resp4, start_offsets[1]+1)

        self.assert_fetch_offset(partitions[0], start_offsets[0], [ self.msg("one"), self.msg("three") ])
        self.assert_fetch_offset(partitions[1], start_offsets[1], [ self.msg("two"), self.msg("four")  ])

        producer.stop()

    def test_hashed_partitioner(self):
        partitions = self.client.get_partition_ids_for_topic(self.topic)
        start_offsets = [self.current_offset(self.topic, p) for p in partitions]

        producer = KeyedProducer(self.client, partitioner=HashedPartitioner)
        resp1 = producer.send_messages(self.topic, self.key("1"), self.msg("one"))
        resp2 = producer.send_messages(self.topic, self.key("2"), self.msg("two"))
        resp3 = producer.send_messages(self.topic, self.key("3"), self.msg("three"))
        resp4 = producer.send_messages(self.topic, self.key("3"), self.msg("four"))
        resp5 = producer.send_messages(self.topic, self.key("4"), self.msg("five"))

        offsets = {partitions[0]: start_offsets[0], partitions[1]: start_offsets[1]}
        messages = {partitions[0]: [], partitions[1]: []}

        keys = [self.key(k) for k in ["1", "2", "3", "3", "4"]]
        resps = [resp1, resp2, resp3, resp4, resp5]
        msgs = [self.msg(m) for m in ["one", "two", "three", "four", "five"]]

        for key, resp, msg in zip(keys, resps, msgs):
            k = hash(key) % 2
            partition = partitions[k]
            offset = offsets[partition]
            self.assert_produce_response(resp, offset)
            offsets[partition] += 1
            messages[partition].append(msg)

        self.assert_fetch_offset(partitions[0], start_offsets[0], messages[partitions[0]])
        self.assert_fetch_offset(partitions[1], start_offsets[1], messages[partitions[1]])

        producer.stop()

    def test_async_keyed_producer(self):
        partition = self.client.get_partition_ids_for_topic(self.topic)[0]
        start_offset = self.current_offset(self.topic, partition)

        producer = KeyedProducer(self.client,
                                 partitioner=RoundRobinPartitioner,
                                 async=True,
                                 batch_send_every_t=1)

        resp = producer.send_messages(self.topic, self.key("key1"), self.msg("one"))
        self.assertEqual(len(resp), 0)

        # wait for the server to report a new highwatermark
        while self.current_offset(self.topic, partition) == start_offset:
            time.sleep(0.1)

        self.assert_fetch_offset(partition, start_offset, [ self.msg("one") ])

        producer.stop()

    ############################
    #   Producer ACK Tests     #
    ############################

    def test_acks_none(self):
        partition = self.client.get_partition_ids_for_topic(self.topic)[0]
        start_offset = self.current_offset(self.topic, partition)

        producer = Producer(
            self.client,
            req_acks=Producer.ACK_NOT_REQUIRED,
        )
        resp = producer.send_messages(self.topic, partition, self.msg("one"))

        # No response from produce request with no acks required
        self.assertEqual(len(resp), 0)

        # But the message should still have been delivered
        self.assert_fetch_offset(partition, start_offset, [ self.msg("one") ])
        producer.stop()

    def test_acks_local_write(self):
        partition = self.client.get_partition_ids_for_topic(self.topic)[0]
        start_offset = self.current_offset(self.topic, partition)

        producer = Producer(
            self.client,
            req_acks=Producer.ACK_AFTER_LOCAL_WRITE,
        )
        resp = producer.send_messages(self.topic, partition, self.msg("one"))

        self.assert_produce_response(resp, start_offset)
        self.assert_fetch_offset(partition, start_offset, [ self.msg("one") ])

        producer.stop()

    def test_acks_cluster_commit(self):
        partition = self.client.get_partition_ids_for_topic(self.topic)[0]
        start_offset = self.current_offset(self.topic, partition)

        producer = Producer(
            self.client,
            req_acks=Producer.ACK_AFTER_CLUSTER_COMMIT,
        )

        resp = producer.send_messages(self.topic, partition, self.msg("one"))
        self.assert_produce_response(resp, start_offset)
        self.assert_fetch_offset(partition, start_offset, [ self.msg("one") ])

        producer.stop()

    def assert_produce_request(self, messages, initial_offset, message_ct,
                               partition=0):
        produce = ProduceRequestPayload(self.topic, partition, messages=messages)

        # There should only be one response message from the server.
        # This will throw an exception if there's more than one.
        resp = self.client.send_produce_request([ produce ])
        self.assert_produce_response(resp, initial_offset)

        self.assertEqual(self.current_offset(self.topic, partition), initial_offset + message_ct)

    def assert_produce_response(self, resp, initial_offset):
        self.assertEqual(len(resp), 1)
        self.assertEqual(resp[0].error, 0)
        self.assertEqual(resp[0].offset, initial_offset)

    def assert_fetch_offset(self, partition, start_offset, expected_messages):
        # There should only be one response message from the server.
        # This will throw an exception if there's more than one.

        resp, = self.client.send_fetch_request([FetchRequestPayload(self.topic, partition, start_offset, 1024)])

        self.assertEqual(resp.error, 0)
        self.assertEqual(resp.partition, partition)
        messages = [ x.message.value for x in resp.messages ]

        self.assertEqual(messages, expected_messages)
        self.assertEqual(resp.highwaterMark, start_offset+len(expected_messages))
