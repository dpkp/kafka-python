from __future__ import absolute_import

import logging
import time
import random

try:
    from queue import Empty
except ImportError:
    from Queue import Empty
from collections import defaultdict
from itertools import cycle
from multiprocessing import Queue, Process

import six
from six.moves import xrange

from kafka.common import (
    ProduceRequest, TopicAndPartition, UnsupportedCodecError, UnknownTopicOrPartitionError
)
from kafka.partitioner import HashedPartitioner
from kafka.protocol import CODEC_NONE, ALL_CODECS, create_message_set

log = logging.getLogger("kafka")

BATCH_SEND_DEFAULT_INTERVAL = 20
BATCH_SEND_MSG_COUNT = 20

STOP_ASYNC_PRODUCER = -1


def _send_upstream(queue, client, codec, batch_time, batch_size,
                   req_acks, ack_timeout):
    """
    Listen on the queue for a specified number of messages or till
    a specified timeout and send them upstream to the brokers in one
    request

    NOTE: Ideally, this should have been a method inside the Producer
    class. However, multiprocessing module has issues in windows. The
    functionality breaks unless this function is kept outside of a class
    """
    stop = False
    client.reinit()

    while not stop:
        timeout = batch_time
        count = batch_size
        send_at = time.time() + timeout
        msgset = defaultdict(list)

        # Keep fetching till we gather enough messages or a
        # timeout is reached
        while count > 0 and timeout >= 0:
            try:
                topic_partition, msg = queue.get(timeout=timeout)

            except Empty:
                break

            # Check if the controller has requested us to stop
            if topic_partition == STOP_ASYNC_PRODUCER:
                stop = True
                break

            # Adjust the timeout to match the remaining period
            count -= 1
            timeout = send_at - time.time()
            msgset[topic_partition].append(msg)

        # Send collected requests upstream
        reqs = []
        for topic_partition, msg in msgset.items():
            messages = create_message_set(msg, codec)
            req = ProduceRequest(topic_partition.topic,
                                 topic_partition.partition,
                                 messages)
            reqs.append(req)

        try:
            client.send_produce_request(reqs,
                                        acks=req_acks,
                                        timeout=ack_timeout)
        except Exception:
            log.exception("Unable to send message")


class Producer(object):
    """
    Base class to be used by producers

    Params:
    client - The Kafka client instance to use
    async - If set to true, the messages are sent asynchronously via another
            thread (process). We will not wait for a response to these
            WARNING!!! current implementation of async producer does not
            guarantee message delivery.  Use at your own risk! Or help us
            improve with a PR!
    req_acks - A value indicating the acknowledgements that the server must
               receive before responding to the request
    ack_timeout - Value (in milliseconds) indicating a timeout for waiting
                  for an acknowledgement
    batch_send - If True, messages are send in batches
    batch_send_every_n - If set, messages are send in batches of this size
    batch_send_every_t - If set, messages are send after this timeout
    """

    ACK_NOT_REQUIRED = 0            # No ack is required
    ACK_AFTER_LOCAL_WRITE = 1       # Send response after it is written to log
    ACK_AFTER_CLUSTER_COMMIT = -1   # Send response after data is committed

    DEFAULT_ACK_TIMEOUT = 1000

    def __init__(self, client, async=False,
                 req_acks=ACK_AFTER_LOCAL_WRITE,
                 ack_timeout=DEFAULT_ACK_TIMEOUT,
                 codec=None,
                 batch_send=False,
                 batch_send_every_n=BATCH_SEND_MSG_COUNT,
                 batch_send_every_t=BATCH_SEND_DEFAULT_INTERVAL):

        if batch_send:
            async = True
            assert batch_send_every_n > 0
            assert batch_send_every_t > 0
        else:
            batch_send_every_n = 1
            batch_send_every_t = 3600

        self.client = client
        self.async = async
        self.req_acks = req_acks
        self.ack_timeout = ack_timeout

        if codec is None:
            codec = CODEC_NONE
        elif codec not in ALL_CODECS:
            raise UnsupportedCodecError("Codec 0x%02x unsupported" % codec)

        self.codec = codec

        if self.async:
            log.warning("async producer does not guarantee message delivery!")
            log.warning("Current implementation does not retry Failed messages")
            log.warning("Use at your own risk! (or help improve with a PR!)")
            self.queue = Queue()  # Messages are sent through this queue
            self.proc = Process(target=_send_upstream,
                                args=(self.queue,
                                      self.client.copy(),
                                      self.codec,
                                      batch_send_every_t,
                                      batch_send_every_n,
                                      self.req_acks,
                                      self.ack_timeout))

            # Process will die if main thread exits
            self.proc.daemon = True
            self.proc.start()

    def send_messages(self, topic, partition, *msg):
        """
        Helper method to send produce requests
        @param: topic, name of topic for produce request -- type str
        @param: partition, partition number for produce request -- type int
        @param: *msg, one or more message payloads -- type str
        @returns: ResponseRequest returned by server
        raises on error

        Note that msg type *must* be encoded to str by user.
        Passing unicode message will not work, for example
        you should encode before calling send_messages via
        something like `unicode_message.encode('utf-8')`

        All messages produced via this method will set the message 'key' to Null
        """

        # Guarantee that msg is actually a list or tuple (should always be true)
        if not isinstance(msg, (list, tuple)):
            raise TypeError("msg is not a list or tuple!")

        # Raise TypeError if any message is not encoded as bytes
        if any(not isinstance(m, six.binary_type) for m in msg):
            raise TypeError("all produce message payloads must be type bytes")

        if self.async:
            for m in msg:
                self.queue.put((TopicAndPartition(topic, partition), m))
            resp = []
        else:
            messages = create_message_set(msg, self.codec)
            req = ProduceRequest(topic, partition, messages)
            try:
                resp = self.client.send_produce_request([req], acks=self.req_acks,
                                                        timeout=self.ack_timeout)
            except Exception:
                log.exception("Unable to send messages")
                raise
        return resp

    def stop(self, timeout=1):
        """
        Stop the producer. Optionally wait for the specified timeout before
        forcefully cleaning up.
        """
        if self.async:
            self.queue.put((STOP_ASYNC_PRODUCER, None))
            self.proc.join(timeout)

            if self.proc.is_alive():
                self.proc.terminate()


class SimpleProducer(Producer):
    """
    A simple, round-robin producer. Each message goes to exactly one partition

    Params:
    client - The Kafka client instance to use
    async - If True, the messages are sent asynchronously via another
            thread (process). We will not wait for a response to these
    req_acks - A value indicating the acknowledgements that the server must
               receive before responding to the request
    ack_timeout - Value (in milliseconds) indicating a timeout for waiting
                  for an acknowledgement
    batch_send - If True, messages are send in batches
    batch_send_every_n - If set, messages are send in batches of this size
    batch_send_every_t - If set, messages are send after this timeout
    random_start - If true, randomize the initial partition which the
                   the first message block will be published to, otherwise
                   if false, the first message block will always publish
                   to partition 0 before cycling through each partition
    """
    def __init__(self, client, async=False,
                 req_acks=Producer.ACK_AFTER_LOCAL_WRITE,
                 ack_timeout=Producer.DEFAULT_ACK_TIMEOUT,
                 codec=None,
                 batch_send=False,
                 batch_send_every_n=BATCH_SEND_MSG_COUNT,
                 batch_send_every_t=BATCH_SEND_DEFAULT_INTERVAL,
                 random_start=False):
        self.partition_cycles = {}
        self.random_start = random_start
        super(SimpleProducer, self).__init__(client, async, req_acks,
                                             ack_timeout, codec, batch_send,
                                             batch_send_every_n,
                                             batch_send_every_t)

    def _next_partition(self, topic):
        if topic not in self.partition_cycles:
            if topic not in self.client.topic_partitions:
                self.client.load_metadata_for_topics(topic)
            try:
                self.partition_cycles[topic] = cycle(self.client.topic_partitions[topic])
            except KeyError:
                raise UnknownTopicOrPartitionError(topic)

            # Randomize the initial partition that is returned
            if self.random_start:
                num_partitions = len(self.client.topic_partitions[topic])
                for _ in xrange(random.randint(0, num_partitions-1)):
                    next(self.partition_cycles[topic])

        return next(self.partition_cycles[topic])

    def send_messages(self, topic, *msg):
        partition = self._next_partition(topic)
        return super(SimpleProducer, self).send_messages(topic, partition, *msg)

    def __repr__(self):
        return '<SimpleProducer batch=%s>' % self.async


class KeyedProducer(Producer):
    """
    A producer which distributes messages to partitions based on the key

    Args:
    client - The kafka client instance
    partitioner - A partitioner class that will be used to get the partition
        to send the message to. Must be derived from Partitioner
    async - If True, the messages are sent asynchronously via another
            thread (process). We will not wait for a response to these
    ack_timeout - Value (in milliseconds) indicating a timeout for waiting
                  for an acknowledgement
    batch_send - If True, messages are send in batches
    batch_send_every_n - If set, messages are send in batches of this size
    batch_send_every_t - If set, messages are send after this timeout
    """
    def __init__(self, client, partitioner=None, async=False,
                 req_acks=Producer.ACK_AFTER_LOCAL_WRITE,
                 ack_timeout=Producer.DEFAULT_ACK_TIMEOUT,
                 codec=None,
                 batch_send=False,
                 batch_send_every_n=BATCH_SEND_MSG_COUNT,
                 batch_send_every_t=BATCH_SEND_DEFAULT_INTERVAL):
        if not partitioner:
            partitioner = HashedPartitioner
        self.partitioner_class = partitioner
        self.partitioners = {}

        super(KeyedProducer, self).__init__(client, async, req_acks,
                                            ack_timeout, codec, batch_send,
                                            batch_send_every_n,
                                            batch_send_every_t)

    def _next_partition(self, topic, key):
        if topic not in self.partitioners:
            if topic not in self.client.topic_partitions:
                self.client.load_metadata_for_topics(topic)
            self.partitioners[topic] = \
                self.partitioner_class(self.client.topic_partitions[topic])
        partitioner = self.partitioners[topic]
        return partitioner.partition(key, self.client.topic_partitions[topic])

    def send(self, topic, key, msg):
        partition = self._next_partition(topic, key)
        return self.send_messages(topic, partition, msg)

    def __repr__(self):
        return '<KeyedProducer batch=%s>' % self.async
