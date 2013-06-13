from itertools import cycle
from multiprocessing import Queue, Process
import logging

from kafka.common import ProduceRequest
from kafka.protocol import create_message

log = logging.getLogger("kafka")


class Producer(object):
    """
    Base class to be used by producers

    Params:
    client - The Kafka client instance to use
    topic - The topic for sending messages to
    async - If set to true, the messages are sent asynchronously via another
            thread (process). We will not wait for a response to these
    """
    def __init__(self, client, async=False):
        self.client = client
        self.async = async

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
            self.client.send_produce_request([req])[0]

    def send_request(self, req):
        """
        Helper method to send produce requests
        """
        if self.async:
            self.queue.put(req)
        else:
            resp = self.client.send_produce_request([req])[0]
            assert resp.error == 0

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
    """
    def __init__(self, client, topic, async=False):
        self.topic = topic
        client._load_metadata_for_topics(topic)
        self.next_partition = cycle(client.topic_partitions[topic])
        super(SimpleProducer, self).__init__(client, async)

    def send_messages(self, *msg):
        req = ProduceRequest(self.topic, self.next_partition.next(),
                             messages=[create_message(m) for m in msg])
        self.send_request(req)
