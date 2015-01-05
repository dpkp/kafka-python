from __future__ import absolute_import

import logging

from kafka.partitioner import HashedPartitioner
from .base import (
    Producer, BATCH_SEND_DEFAULT_INTERVAL,
    BATCH_SEND_MSG_COUNT, BATCH_SEND_QUEUE_BUFFERING_MAX_MESSAGES,
    BATCH_SEND_QUEUE_MAX_WAIT, BATCH_SEND_MAX_RETRY, BATCH_SEND_RETRY_BACKOFF_MS
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
    batch_send_queue_buffering_max_messages - If set, maximum number of messages
                                              allowed on the async queue
    batch_send_queue_max_wait - If set, wait to put messages in the async queue
                                until free space or this timeout
    batch_send_max_retry - Number of retry for async send, default: 3
    batch_send_retry_backoff_ms - sleep between retry, default: 300ms
    """

    def __init__(self, client, partitioner=None, async=False,
                 req_acks=Producer.ACK_AFTER_LOCAL_WRITE,
                 ack_timeout=Producer.DEFAULT_ACK_TIMEOUT,
                 codec=None,
                 batch_send=False,
                 batch_send_every_n=BATCH_SEND_MSG_COUNT,
                 batch_send_every_t=BATCH_SEND_DEFAULT_INTERVAL,
                 batch_send_queue_buffering_max_messages=BATCH_SEND_QUEUE_BUFFERING_MAX_MESSAGES,
                 batch_send_queue_max_wait=BATCH_SEND_QUEUE_MAX_WAIT,
                 batch_send_max_retry=BATCH_SEND_MAX_RETRY,
                 batch_send_retry_backoff_ms=BATCH_SEND_RETRY_BACKOFF_MS):
        if not partitioner:
            partitioner = HashedPartitioner
        self.partitioner_class = partitioner
        self.partitioners = {}

        super(KeyedProducer, self).__init__(client, async, req_acks,
                                            ack_timeout, codec, batch_send,
                                            batch_send_every_n,
                                            batch_send_every_t,
                                            batch_send_queue_buffering_max_messages,
                                            batch_send_queue_max_wait,
                                            batch_send_max_retry,
                                            batch_send_retry_backoff_ms)

    def _next_partition(self, topic, key):
        if topic not in self.partitioners:
            if not self.client.has_metadata_for_topic(topic):
                self.client.load_metadata_for_topics(topic)

            self.partitioners[topic] = self.partitioner_class(self.client.get_partition_ids_for_topic(topic))

        partitioner = self.partitioners[topic]
        return partitioner.partition(key, self.client.get_partition_ids_for_topic(topic))

    def send_messages(self, topic, key, *msg):
        partition = self._next_partition(topic, key)
        return self._send_messages(topic, partition, *msg, key=key)

    def send(self, topic, key, msg):
        partition = self._next_partition(topic, key)
        return self._send_messages(topic, partition, msg, key=key)

    def __repr__(self):
        return '<KeyedProducer batch=%s>' % self.async
