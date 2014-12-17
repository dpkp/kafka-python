from __future__ import absolute_import

import logging

from kafka.partitioner import HashedPartitioner
from .base import (
    Producer, BATCH_SEND_DEFAULT_INTERVAL,
    BATCH_SEND_MSG_COUNT
)

log = logging.getLogger("kafka")


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
            if not self.client.has_metadata_for_topic(topic):
                self.client.load_metadata_for_topics(topic)

            self.partitioners[topic] = self.partitioner_class(self.client.get_partition_ids_for_topic(topic))

        partitioner = self.partitioners[topic]
        return partitioner.partition(key, self.client.get_partition_ids_for_topic(topic))

    def send_messages(self,topic,key,*msg):
        partition = self._next_partition(topic, key)
        return self._send_messages(topic, partition, *msg,key=key)

    def send(self, topic, key, msg):
        partition = self._next_partition(topic, key)
        return self._send_messages(topic, partition, msg, key=key)

    def __repr__(self):
        return '<KeyedProducer batch=%s>' % self.async
