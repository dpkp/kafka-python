from collections import defaultdict
from datetime import datetime, timedelta
from itertools import cycle
from multiprocessing import Queue, Process
from Queue import Empty
import logging
import sys

from kafka.common import ProduceRequest
from kafka.common import FailedPayloadsException
from kafka.protocol import create_message
from kafka.partitioner import HashedPartitioner

log = logging.getLogger("kafka")

BATCH_SEND_DEFAULT_INTERVAL = 20
BATCH_SEND_MSG_COUNT = 20

STOP_ASYNC_PRODUCER = -1


def _send_upstream(topic, queue, client, batch_time, batch_size,
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
        send_at = datetime.now() + timedelta(seconds=timeout)
        msgset = defaultdict(list)

        # Keep fetching till we gather enough messages or a
        # timeout is reached
        while count > 0 and timeout >= 0:
            try:
                partition, msg = queue.get(timeout=timeout)
            except Empty:
                break

            # Check if the controller has requested us to stop
            if partition == STOP_ASYNC_PRODUCER:
                stop = True
                break

            # Adjust the timeout to match the remaining period
            count -= 1
            timeout = (send_at - datetime.now()).total_seconds()
            msgset[partition].append(msg)

        # Send collected requests upstream
        reqs = []
        for partition, messages in msgset.items():
            req = ProduceRequest(topic, partition, messages)
            reqs.append(req)

        try:
            client.send_produce_request(reqs,
                                        acks=req_acks,
                                        timeout=ack_timeout)
        except Exception as exp:
            log.exception("Unable to send message")


class Producer(object):
    """
    Base class to be used by producers

    Params:
    client - The Kafka client instance to use
    topic - The topic for sending messages to
    async - If set to true, the messages are sent asynchronously via another
            thread (process). We will not wait for a response to these
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

        if self.async:
            self.queue = Queue()  # Messages are sent through this queue
            self.proc = Process(target=_send_upstream,
                                args=(self.topic,
                                      self.queue,
                                      self.client.copy(),
                                      batch_send_every_t,
                                      batch_send_every_n,
                                      self.req_acks,
                                      self.ack_timeout))

            # Process will die if main thread exits
            self.proc.daemon = True
            self.proc.start()

    def send_messages(self, partition, *msg):
        """
        Helper method to send produce requests
        """
        if self.async:
            for m in msg:
                self.queue.put((partition, create_message(m)))
            resp = []
        else:
            messages = [create_message(m) for m in msg]
            req = ProduceRequest(self.topic, partition, messages)
            try:
                resp = self.client.send_produce_request([req], acks=self.req_acks,
                                                        timeout=self.ack_timeout)
            except Exception as e:
                log.exception("Unable to send messages")
                raise e
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
    A simple, round-robbin producer. Each message goes to exactly one partition

    Params:
    client - The Kafka client instance to use
    topic - The topic for sending messages to
    async - If True, the messages are sent asynchronously via another
            thread (process). We will not wait for a response to these
    req_acks - A value indicating the acknowledgements that the server must
               receive before responding to the request
    ack_timeout - Value (in milliseconds) indicating a timeout for waiting
                  for an acknowledgement
    batch_send - If True, messages are send in batches
    batch_send_every_n - If set, messages are send in batches of this size
    batch_send_every_t - If set, messages are send after this timeout
    """
    def __init__(self, client, topic, async=False,
                 req_acks=Producer.ACK_AFTER_LOCAL_WRITE,
                 ack_timeout=Producer.DEFAULT_ACK_TIMEOUT,
                 batch_send=False,
                 batch_send_every_n=BATCH_SEND_MSG_COUNT,
                 batch_send_every_t=BATCH_SEND_DEFAULT_INTERVAL):
        self.topic = topic
        client._load_metadata_for_topics(topic)
        self.next_partition = cycle(client.topic_partitions[topic])

        super(SimpleProducer, self).__init__(client, async, req_acks,
                                             ack_timeout, batch_send,
                                             batch_send_every_n,
                                             batch_send_every_t)

    def send_messages(self, *msg):
        partition = self.next_partition.next()
        return super(SimpleProducer, self).send_messages(partition, *msg)


class KeyedProducer(Producer):
    """
    A producer which distributes messages to partitions based on the key

    Args:
    client - The kafka client instance
    topic - The kafka topic to send messages to
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
    def __init__(self, client, topic, partitioner=None, async=False,
                 req_acks=Producer.ACK_AFTER_LOCAL_WRITE,
                 ack_timeout=Producer.DEFAULT_ACK_TIMEOUT,
                 batch_send=False,
                 batch_send_every_n=BATCH_SEND_MSG_COUNT,
                 batch_send_every_t=BATCH_SEND_DEFAULT_INTERVAL):
        self.topic = topic
        client._load_metadata_for_topics(topic)

        if not partitioner:
            partitioner = HashedPartitioner

        self.partitioner = partitioner(client.topic_partitions[topic])

        super(KeyedProducer, self).__init__(client, async, req_acks,
                                            ack_timeout, batch_send,
                                            batch_send_every_n,
                                            batch_send_every_t)

    def send(self, key, msg):
        partitions = self.client.topic_partitions[self.topic]
        partition = self.partitioner.partition(key, partitions)
        return self.send_messages(partition, msg)
