from itertools import cycle
import logging

from kafka.common import ProduceRequest
from kafka.protocol import create_message

log = logging.getLogger("kafka")


class SimpleProducer(object):
    """
    A simple, round-robbin producer. Each message goes to exactly one partition
    """
    def __init__(self, client, topic):
        self.client = client
        self.topic = topic
        self.client._load_metadata_for_topics(topic)
        self.next_partition = cycle(self.client.topic_partitions[topic])

    def send_messages(self, *msg):
        req = ProduceRequest(self.topic, self.next_partition.next(),
                             messages=[create_message(m) for m in msg])

        resp = self.client.send_produce_request([req])[0]
        assert resp.error == 0
