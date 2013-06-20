from itertools import cycle
from multiprocessing import Queue, Process
import logging

from kafka.common import ProduceRequest
from kafka.protocol import create_message
from kafka.partitioner import HashedPartitioner

log = logging.getLogger("kafka")


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
    """

    ACK_NOT_REQUIRED = 0            # No ack is required
    ACK_AFTER_LOCAL_WRITE = 1       # Send response after it is written to log
    ACK_AFTER_CLUSTER_COMMIT = -1   # Send response after data is committed

    DEFAULT_ACK_TIMEOUT = 1000

    def __init__(self, client, async=False, req_acks=ACK_NOT_REQUIRED,
                 ack_timeout=DEFAULT_ACK_TIMEOUT):
        self.client = client
        self.async = async
        self.req_acks = req_acks
        self.ack_timeout = ack_timeout

        if self.async:
            self.queue = Queue()  # Messages are sent through this queue
            self.proc = Process(target=self._send_upstream, args=(self.queue,))
            self.proc.daemon = True   # Process will die if main thread exits
            self.proc.start()

    def _send_upstream(self, queue):
        """
        Listen on the queue for messages and send them upstream to the brokers
        """
        while True:
            req = queue.get()
            # Ignore any acks in the async mode
            self.client.send_produce_request([req], acks=self.req_acks,
                                             timeout=self.ack_timeout)

    def send_request(self, req):
        """
        Helper method to send produce requests
        """
        resp = []
        if self.async:
            self.queue.put(req)
        else:
            resp = self.client.send_produce_request([req], acks=self.req_acks,
                                                    timeout=self.ack_timeout)
        return resp

    def stop(self):
        if self.async:
            self.proc.terminate()
            self.proc.join()


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
    """
    def __init__(self, client, topic, async=False,
                 req_acks=Producer.ACK_NOT_REQUIRED,
                 ack_timeout=Producer.DEFAULT_ACK_TIMEOUT):
        self.topic = topic
        client._load_metadata_for_topics(topic)
        self.next_partition = cycle(client.topic_partitions[topic])

        super(SimpleProducer, self).__init__(client, async,
                                             req_acks, ack_timeout)

    def send_messages(self, *msg):
        req = ProduceRequest(self.topic, self.next_partition.next(),
                             messages=[create_message(m) for m in msg])
        return self.send_request(req)


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
    """
    def __init__(self, client, topic, partitioner=None, async=False,
                 req_acks=Producer.ACK_NOT_REQUIRED,
                 ack_timeout=Producer.DEFAULT_ACK_TIMEOUT):
        self.topic = topic
        client._load_metadata_for_topics(topic)

        if not partitioner:
            partitioner = HashedPartitioner

        self.partitioner = partitioner(client.topic_partitions[topic])

        super(KeyedProducer, self).__init__(client, async,
                                            req_acks, ack_timeout)

    def send(self, key, msg):
        partitions = self.client.topic_partitions[self.topic]
        partition = self.partitioner.partition(key, partitions)

        req = ProduceRequest(self.topic, partition,
                             messages=[create_message(msg)])

        return self.send_request(req)
