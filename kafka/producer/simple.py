from __future__ import absolute_import

import logging
import random

from itertools import cycle

from six.moves import xrange

from .base import (
    Producer, BATCH_SEND_DEFAULT_INTERVAL,
    BATCH_SEND_MSG_COUNT, BATCH_SEND_QUEUE_BUFFERING_MAX_MESSAGES,
    BATCH_SEND_QUEUE_MAX_WAIT, BATCH_SEND_MAX_RETRY, BATCH_SEND_RETRY_BACKOFF_MS
)

log = logging.getLogger("kafka")


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
    batch_send_queue_buffering_max_messages - If set, maximum number of messages
                                              allowed on the async queue
    batch_send_queue_max_wait - If set, wait to put messages in the async queue
                                until free space or this timeout
    batch_send_max_retry - Number of retry for async send, default: 3
    batch_send_retry_backoff_ms - sleep between retry, default: 300ms
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
                 batch_send_queue_buffering_max_messages=BATCH_SEND_QUEUE_BUFFERING_MAX_MESSAGES,
                 batch_send_queue_max_wait=BATCH_SEND_QUEUE_MAX_WAIT,
                 batch_send_max_retry=BATCH_SEND_MAX_RETRY,
                 batch_send_retry_backoff_ms=BATCH_SEND_RETRY_BACKOFF_MS,
                 random_start=False):
        self.partition_cycles = {}
        self.random_start = random_start
        super(SimpleProducer, self).__init__(client, async, req_acks,
                                             ack_timeout, codec, batch_send,
                                             batch_send_every_n,
                                             batch_send_every_t,
                                             batch_send_queue_buffering_max_messages,
                                             batch_send_queue_max_wait,
                                             batch_send_max_retry,
                                             batch_send_retry_backoff_ms)

    def _next_partition(self, topic):
        if topic not in self.partition_cycles:
            if not self.client.has_metadata_for_topic(topic):
                self.client.load_metadata_for_topics(topic)

            self.partition_cycles[topic] = cycle(self.client.get_partition_ids_for_topic(topic))

            # Randomize the initial partition that is returned
            if self.random_start:
                num_partitions = len(self.client.get_partition_ids_for_topic(topic))
                for _ in xrange(random.randint(0, num_partitions - 1)):
                    next(self.partition_cycles[topic])

        return next(self.partition_cycles[topic])

    def send_messages(self, topic, *msg):
        partition = self._next_partition(topic)
        return super(SimpleProducer, self).send_messages(topic, partition, *msg)

    def __repr__(self):
        return '<SimpleProducer batch=%s>' % self.async
